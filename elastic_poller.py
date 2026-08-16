"""Poll Elasticsearch for new events and forward them to Edwin (Dexda).

Uses a millisecond-precision bookmark file to resume after restarts. Each poll
cycle opens a point-in-time, queries ES with an exclusive lower bound (gt), and
paginates via search_after until the backlog for that cycle is drained. The PIT
makes the snapshot stable across pages and is what permits _shard_doc as the
sort tie-breaker.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

import common_event
import dexda_request

load_dotenv()

# Exit codes
OK = 0
ERROR_CODE_UNKNOWN = 1
ERROR_CODE_VALIDATION_FAILED = 2
ERROR_CODE_EVENT_MAPPING_FAILED = 3
ERROR_CODE_EVENT_DELIVERY_FAILED = 4
ERROR_CODE_HTTP = 5
ERROR_CODE_UNEXPECTED = 6

# Elasticsearch configuration
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASS = os.getenv("ELASTIC_PASS")
ELASTIC_TOKEN = os.getenv("ELASTIC_TOKEN")
ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_BATCH_SIZE = int(os.getenv("ELASTIC_BATCH_SIZE", 500))
ELASTIC_INDEX = os.getenv("ELASTIC_INDEXS")
ELASTIC_QUERY = os.getenv("ELASTIC_QUERY", "*")
ELASTIC_VERIFY_SSL = os.getenv("ELASTIC_VERIFY_SSL", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
# Point-in-time keep-alive. Extended on every page, so this only needs to cover
# the gap between two consecutive pages, not the duration of the whole cycle.
ELASTIC_PIT_KEEP_ALIVE = os.getenv("ELASTIC_PIT_KEEP_ALIVE", "5m")

# Edwin (Dexda) configuration
PAUSE_INTERVAL = os.getenv("POLLER_INTERVAL", 240)
DEXDA_ORG = os.getenv("DEXDA_ORG")
DEXDA_ID = os.getenv("DEXDA_ID")
DEXDA_TOKEN = os.getenv("DEXDA_TOKEN")

# Bookmark persistence (BOOKMARK_PATH defaults to CWD; Docker sets /data/)
BOOKMARK_PATH = os.getenv("BOOKMARK_PATH")
DEBUG = os.getenv("DEBUG", False)
LOG = os.getenv("LOG", True)
logging.disabled = LOG

bookmark_dir = BOOKMARK_PATH.rstrip("/") if BOOKMARK_PATH else "."
bookmark_file = os.path.join(bookmark_dir, f"{DEXDA_ORG}.elastic.bookmark")

print("Using LM portal: " + str(DEXDA_ORG))
print("Using ES portal: " + str(ELASTIC_URL))
print("Using bookmarkfile: " + str(bookmark_file))


class ElasticsearchQueryError(Exception):
    """Raised when an Elasticsearch request fails.

    Carries the HTTP status and response body so callers can tell an expired
    point-in-time apart from a genuine cluster failure.
    """

    def __init__(self, message, status_code=None, body=""):
        super().__init__(message)
        self.status_code = status_code
        self.body = body or ""

    @property
    def is_missing_context(self) -> bool:
        """True when Elasticsearch says the point-in-time no longer exists.

        Once a PIT keep_alive lapses, ES answers with 404 and a
        search_context_missing_exception root cause.
        """
        if self.status_code == 404:
            return True
        return "search_context_missing" in self.body or "No search context" in self.body


def log(msg, *args):
    if LOG:
        sys.stderr.write(msg + " ".join([str(a) for a in args]) + "\n")


def debug(message, *args):
    if DEBUG:
        current_time = datetime.now().strftime("%d.%b %Y %H:%M:%S")
        print(current_time + " - " + message + " ".join([str(a) for a in args]))


def build_logs_query(
    text: str,
    bookmark_ms: int,
    end: str = "now",
    size: int = 10000,
    timestamp_field: str = "@timestamp",
    search_after: Optional[List[Any]] = None,
    pit_id: Optional[str] = None,
    keep_alive: Optional[str] = None,
) -> Dict[str, Any]:
    """Build an Elasticsearch _search body.

    Uses an exclusive millisecond lower bound (gt) so the last processed event
    is never re-fetched.

    When pit_id is supplied the body carries a `pit` block and _shard_doc is
    appended as the sort tie-breaker. _shard_doc is only a legal sort field
    inside a point-in-time -- ES rejects it otherwise -- and it is the only
    stable cross-shard tie-breaker available, since sorting on _id would require
    enabling fielddata on ES 8+. Emitting the two together here keeps that
    invariant in one place.

    Passing keep_alive on every page extends the PIT lease.

    The `lte: end` upper bound is defense in depth, not the consistency
    mechanism: under a PIT the snapshot is already frozen at open time, and
    since "now" only grows across pages it can never shrink the result set.
    """
    query: Dict[str, Any] = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {"query_string": {"query": text}}
                ],
                "filter": [
                    {
                        "range": {
                            timestamp_field: {
                                "gt": bookmark_ms,
                                "lte": end,
                                "format": "epoch_millis",
                            }
                        }
                    }
                ],
            }
        },
        "sort": [
            {timestamp_field: {"order": "asc"}},
        ],
    }
    if pit_id:
        query["pit"] = {
            "id": pit_id,
            "keep_alive": keep_alive or ELASTIC_PIT_KEEP_ALIVE,
        }
        query["sort"].append({"_shard_doc": "asc"})
    if search_after is not None:
        query["search_after"] = search_after
    return query


def _es_request(
    method: str,
    path: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    base_url: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
    verify_ssl: bool = True,
    timeout: int = 30,
) -> Dict[str, Any]:
    """Single entry point for every Elasticsearch HTTP call.

    Centralises URL joining, auth headers, TLS verification and error wrapping
    for _search, _pit open and _pit close.
    """
    base = (base_url if base_url is not None else ELASTIC_URL) or ""

    if api_key and (username or password):
        raise ValueError("Use either api_key or username/password, not both.")

    if len(base) < 5:
        raise ValueError("You have not supplied a correct url for ElasticSearch.")

    url = f"{base.rstrip('/')}/{path.lstrip('/')}"

    headers = {"Accept": "application/json"}
    auth = None
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    elif username and password:
        auth = (username, password)
    if body is not None:
        # Opening a PIT is a bodyless POST; only set Content-Type when sending one.
        headers["Content-Type"] = "application/json"

    try:
        response = requests.request(
            method,
            url,
            json=body,
            params=params,
            headers=headers,
            auth=auth,
            verify=verify_ssl,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json() if response.content else {}

    except requests.RequestException as exc:
        status = None
        detail = ""
        response = getattr(exc, "response", None)
        if response is not None:
            status = getattr(response, "status_code", None)
            try:
                detail = response.text
            except Exception:
                detail = ""
        message = f"Elasticsearch {method} /{path.lstrip('/')} failed: {exc}."
        if detail:
            message += f" Response body: {detail}"
        raise ElasticsearchQueryError(
            message, status_code=status, body=detail
        ) from exc


def _es_conn_kwargs() -> Dict[str, Any]:
    """Connection kwargs read from module globals at call time.

    Read lazily rather than captured at import so tests can rebind the globals
    with patch.object.
    """
    return {
        "base_url": ELASTIC_URL,
        "username": ELASTIC_USER,
        "password": ELASTIC_PASS,
        "api_key": ELASTIC_TOKEN,
        "verify_ssl": ELASTIC_VERIFY_SSL,
    }


def query_elasticsearch(
    index: Optional[str],
    query: Dict[str, Any],
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
    verify_ssl: bool = True,
    timeout: int = 30,
) -> Dict[str, Any]:
    """POST a query DSL body to Elasticsearch and return the JSON response.

    When index is falsy the PIT-scoped /_search endpoint is used: a request
    carrying a `pit` block must not name an index in the path, because the PIT
    itself already pins the index set.
    """
    path = f"{index}/_search" if index else "_search"
    return _es_request(
        "POST",
        path,
        body=query,
        username=username,
        password=password,
        api_key=api_key,
        verify_ssl=verify_ssl,
        timeout=timeout,
    )


def open_point_in_time(
    index: str,
    keep_alive: Optional[str] = None,
    timeout: int = 30,
    **conn: Any,
) -> str:
    """Open a point-in-time over `index` and return its id.

    ignore_unavailable is deliberately not set: with it, a mistyped index would
    silently yield a PIT over zero shards and the poller would report "0 hits"
    forever. A loud 404 beats silent nothing.
    """
    result = _es_request(
        "POST",
        f"{index}/_pit",
        params={"keep_alive": keep_alive or ELASTIC_PIT_KEEP_ALIVE},
        timeout=timeout,
        **conn,
    )
    pit_id = result.get("id")
    if not pit_id:
        raise ElasticsearchQueryError(
            f"Elasticsearch did not return a point-in-time id: {result}"
        )
    return pit_id


def close_point_in_time(
    pit_id: Optional[str],
    timeout: int = 30,
    **conn: Any,
) -> bool:
    """Release a point-in-time.

    Never raises: this runs in a finally block and must not mask the exception
    that got us there.
    """
    if not pit_id:
        return False

    try:
        result = _es_request(
            "DELETE", "_pit", body={"id": pit_id}, timeout=timeout, **conn
        )
    except ElasticsearchQueryError as exc:
        if exc.status_code == 404:
            return False  # already expired or freed; nothing to release
        log("WARN - failed to close point-in-time: ", str(exc))
        return False
    except Exception as exc:  # pragma: no cover - defensive
        log("WARN - unexpected error closing point-in-time: ", str(exc))
        return False

    return bool(result.get("succeeded"))


def fetch_elasticsearch_hits(
    bookmark_ms: int,
    search_after: Optional[List[Any]] = None,
    pit_id: Optional[str] = None,
    keep_alive: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
    """Fetch one page of hits from Elasticsearch.

    Returns (hits, took, pit_id). Elasticsearch may rotate the point-in-time id
    on any response, so callers must thread the returned id into the next call
    and close that one rather than the id they started with.
    """
    query_body = build_logs_query(
        text=ELASTIC_QUERY,
        bookmark_ms=bookmark_ms,
        size=ELASTIC_BATCH_SIZE,
        search_after=search_after,
        pit_id=pit_id,
        keep_alive=keep_alive or ELASTIC_PIT_KEEP_ALIVE,
    )
    result = query_elasticsearch(
        # A PIT already pins the index set; naming it in the path as well is an error.
        index=None if pit_id else ELASTIC_INDEX,
        query=query_body,
        username=ELASTIC_USER,
        password=ELASTIC_PASS,
        api_key=ELASTIC_TOKEN,
        verify_ssl=ELASTIC_VERIFY_SSL,
        timeout=60,
    )
    print("took: " + str(result.get("took")))
    hits = result.get("hits", {}).get("hits", [])
    print("Found " + str(len(hits)))
    return hits, result.get("took", 0), result.get("pit_id", pit_id)


def hit_timestamp_ms(hit: Dict[str, Any]) -> int:
    """Return the @timestamp of an ES hit as epoch milliseconds."""
    timestamp_str = hit["_source"]["@timestamp"]
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def epoch_ms_to_zulu(ts_ms: int) -> str:
    """Format epoch milliseconds as an ISO-8601 Zulu string for logging."""
    return (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        + "Z"
    )


def createEvent(payload):
    """Map a raw Elasticsearch hit to a CommonEvent using elastic_event_mappings.yaml."""
    try:
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name="elastic_event_mappings.yaml",
            mapping_file_path=".",
            original_record=payload,
        )
        log("INFO - successfully mapped event payload to cef")
    except Exception as error:
        log("ERROR - exception in createEvent: %s", str(error))
        sys.exit(ERROR_CODE_EVENT_MAPPING_FAILED)
    return event


def send_event(event_list):
    """Deliver a batch of CEF events to Edwin. Returns False if any batch fails."""
    auth_dict = {
        "dexda_org": DEXDA_ORG,
        "client_id": DEXDA_ID,
        "client_secret": DEXDA_TOKEN,
    }
    try:
        dxd_request = dexda_request.DexdaRequest.new_from_param(auth_dict=auth_dict)
        access_token = dxd_request.access_token
        success = dxd_request.send(access_token=access_token, data=event_list)
        if not success:
            log("ERROR - send_event: one or more batches failed to deliver")
            return False
        log("INFO - successfully sent Dexda event")
    except Exception as error:
        log("ERROR - exception in send_event: ", str(error))
        sys.exit(ERROR_CODE_VALIDATION_FAILED)
    return True


def getBookmark():
    """Read the bookmark file. Creates the file with 0 if it does not exist."""
    os.makedirs(bookmark_dir, exist_ok=True)
    if not os.path.exists(bookmark_file):
        print("INFO - Bookmark file not found, creating it")
        with open(bookmark_file, "w") as fh:
            fh.write("0")

    with open(bookmark_file, "r") as fh:
        return int(float(fh.read()))


def setBookmark(bookmark):
    """Persist the bookmark as epoch milliseconds (last successfully sent event)."""
    os.makedirs(bookmark_dir, exist_ok=True)
    with open(bookmark_file, "w") as fh:
        fh.write(str(int(bookmark)))


def process_hits(
    hits: List[Dict[str, Any]],
    query_bookmark: int,
    watermark: int,
    bookmark_loaded: bool,
) -> Tuple[List[Dict[str, Any]], int]:
    """Map ES hits to CEF events.

    Returns the event list and the @timestamp (ms) of the last hit processed.
    """
    event_list = []
    last_timestamp_ms = query_bookmark

    for hit in hits:
        event = createEvent(hit)
        event.set_enrichment_value("lm_bookmark", query_bookmark)
        event.set_enrichment_value("lm_watermark", watermark)
        event.set_enrichment_value("lm_loaded", bookmark_loaded)
        event.set_enrichment_value("lm_elastic_index", ELASTIC_INDEX)

        try:
            space_ids = hit["_source"].get("kibana", {}).get("space_ids", [])
            if space_ids:
                event.set_enrichment_value("lm_service_id", ",".join(space_ids))
        except (TypeError, AttributeError):
            pass

        cef = event.get_cef()
        # Ensure event_source_id is always the stable ES document _id
        cef["cef"]["event_source_id"] = cef["cef"]["source_record"]["_id"]

        if "," in cef["cef"]["event_ci"]:
            ci = cef["cef"]["event_ci"]
            cef["cef"]["event_ci"] = ci.split(",")[0]
            try:
                # Alert recovery events include event.end; mark severity as clear
                if cef["cef"]["source_record"]["_source"]["event"].get("end"):
                    cef["cef"]["event_severity"] = 0
            except (TypeError, NameError) as exc:
                print(exc)

        event_list.append(cef)
        last_timestamp_ms = hit_timestamp_ms(hit)
        print(epoch_ms_to_zulu(last_timestamp_ms))

    return event_list, last_timestamp_ms


def poll_cycle(bookmark: int, watermark: int, bookmark_loaded: bool) -> int:
    """Run one poll cycle inside a point-in-time, paginating until the backlog
    is drained or delivery fails.

    cycle_bookmark is held constant for the gt filter across pages; search_after
    advances the cursor within a cycle when more than ELASTIC_BATCH_SIZE events
    share the same time window.

    The PIT gives a frozen snapshot, so _shard_doc is both a legal and a stable
    sort tie-breaker and pagination cannot skip or duplicate documents when many
    events share the same millisecond. It is always released, including on the
    delivery-failure early return and on exceptions.

    Returns the updated bookmark (unchanged if delivery fails or no events found).
    """
    cycle_bookmark = bookmark
    search_after = None
    updated_bookmark = bookmark

    try:
        pit_id = open_point_in_time(
            ELASTIC_INDEX, keep_alive=ELASTIC_PIT_KEEP_ALIVE, **_es_conn_kwargs()
        )
    except (ElasticsearchQueryError, ValueError) as exc:
        print(f"ERROR - could not open point-in-time on {ELASTIC_INDEX}: {exc}")
        return updated_bookmark  # bookmark unchanged; retry next interval

    try:
        while True:
            print(
                f"Query bookmark (exclusive gt): {cycle_bookmark} "
                f"({epoch_ms_to_zulu(cycle_bookmark)})"
            )
            # pit_id is rebound on every page so the finally below always closes
            # the latest id, which is what ES requires when it rotates one.
            hits, _took, pit_id = fetch_elasticsearch_hits(
                cycle_bookmark, search_after=search_after, pit_id=pit_id
            )

            if not hits:
                break

            event_list, last_timestamp_ms = process_hits(
                hits, cycle_bookmark, watermark, bookmark_loaded
            )

            print("BK: " + str(cycle_bookmark))
            print("TS: " + str(last_timestamp_ms))
            print(f"Created {len(event_list)} edwin events.")

            if not send_event(event_list):
                print("ERROR - delivery failed; bookmark not advanced")
                return updated_bookmark  # finally still releases the PIT

            updated_bookmark = last_timestamp_ms
            setBookmark(updated_bookmark)
            bookmark_loaded = True

            if len(hits) < ELASTIC_BATCH_SIZE:
                break

            # More results may exist; continue from the last hit in this page.
            # Inside a PIT every hit carries sort values, so a missing one means
            # something is wrong upstream rather than a case worth guessing at.
            search_after = hits[-1].get("sort")
            if not search_after:
                print("ERROR - ES returned hits without sort values; ending cycle")
                break

    except ElasticsearchQueryError as exc:
        if exc.is_missing_context:
            print("WARN - point-in-time expired mid-cycle; retrying next interval")
            pit_id = None  # already gone; nothing left to release
        else:
            print(f"ERROR - Elasticsearch query failed mid-cycle: {exc}")
        return updated_bookmark
    finally:
        close_point_in_time(pit_id, **_es_conn_kwargs())

    return updated_bookmark


def readElasticRecords():
    """Load fixture data for local testing (output_500.json)."""
    print("Loading records....")
    with open("output_500.json", "r") as fh:
        return fh.read()


if __name__ == "__main__":
    # Default to now-2h when no bookmark file exists
    two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
    default_bookmark = int(two_hours_ago.timestamp() * 1000)
    bookmark = default_bookmark
    bookmark_loaded = False

    stored_bookmark = getBookmark()
    if stored_bookmark > 0:
        bookmark_loaded = True
        bookmark = stored_bookmark

    # watermark captures the bookmark at process start for enrichment metadata
    watermark = bookmark
    print(f"Using the bookmark of {bookmark}")

    while True:
        bookmark = poll_cycle(bookmark, watermark, bookmark_loaded)
        print(f"Sleeping {PAUSE_INTERVAL} seconds")
        time.sleep(int(PAUSE_INTERVAL))

    sys.exit(OK)
