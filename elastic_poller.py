"""Poll Elasticsearch for new events and forward them to Edwin (Dexda).

Uses a millisecond-precision bookmark file to resume after restarts. Each poll
cycle queries ES with an exclusive lower bound (gt) and paginates via
search_after until the backlog for that cycle is drained.
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
    """Raised when the Elasticsearch query fails."""


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
) -> Dict[str, Any]:
    """Build an Elasticsearch _search body.

    Uses an exclusive millisecond lower bound (gt) so the last processed event
    is never re-fetched. The _id sort tie-breaker keeps pagination stable when
    multiple documents share the same @timestamp.
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
            {"_id": {"order": "asc"}},
        ],
    }
    if search_after is not None:
        query["search_after"] = search_after
    return query


def query_elasticsearch(
    index: str,
    query: Dict[str, Any],
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
    verify_ssl: bool = True,
    timeout: int = 30,
) -> Dict[str, Any]:
    """POST a query DSL body to Elasticsearch and return the JSON response."""
    base_url = ELASTIC_URL

    if api_key and (username or password):
        raise ValueError("Use either api_key or username/password, not both.")

    if len(base_url) < 5:
        raise ValueError("You have not supplied a correct url for ElasticSearch.")

    url = f"{base_url.rstrip('/')}/{index}/_search"

    headers = {"Content-Type": "application/json"}
    auth = None
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"
    elif username and password:
        auth = (username, password)

    try:
        response = requests.post(
            url,
            json=query,
            headers=headers,
            auth=auth,
            verify=verify_ssl,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    except requests.RequestException as exc:
        detail = ""
        if getattr(exc, "response", None) is not None:
            try:
                detail = f" Response body: {exc.response.text}"
            except Exception:
                pass
        raise ElasticsearchQueryError(
            f"Elasticsearch query failed: {exc}.{detail}"
        ) from exc


def fetch_elasticsearch_hits(
    bookmark_ms: int,
    search_after: Optional[List[Any]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Fetch one page of hits from Elasticsearch."""
    query_body = build_logs_query(
        text="*",
        bookmark_ms=bookmark_ms,
        size=ELASTIC_BATCH_SIZE,
        search_after=search_after,
    )
    result = query_elasticsearch(
        index=ELASTIC_INDEX,
        query=query_body,
        username=ELASTIC_USER,
        password=ELASTIC_PASS,
        api_key=ELASTIC_TOKEN,
        verify_ssl=False,
        timeout=60,
    )
    print("took: " + str(result["took"]))
    hits = result.get("hits", {}).get("hits", [])
    print("Found " + str(len(hits)))
    return hits, result["took"]


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
    """Run one poll cycle, paginating until the backlog is drained or delivery fails.

    cycle_bookmark is held constant for the gt filter across pages; search_after
    advances the cursor within a cycle when more than ELASTIC_BATCH_SIZE events
    share the same time window.

    Returns the updated bookmark (unchanged if delivery fails or no events found).
    """
    cycle_bookmark = bookmark
    search_after = None
    updated_bookmark = bookmark

    while True:
        print(
            f"Query bookmark (exclusive gt): {cycle_bookmark} "
            f"({epoch_ms_to_zulu(cycle_bookmark)})"
        )
        hits, _ = fetch_elasticsearch_hits(cycle_bookmark, search_after=search_after)

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
            return updated_bookmark

        updated_bookmark = last_timestamp_ms
        setBookmark(updated_bookmark)
        bookmark_loaded = True

        if len(hits) < ELASTIC_BATCH_SIZE:
            break

        # More results may exist; continue from the last hit in this page
        last_hit = hits[-1]
        search_after = last_hit.get("sort")
        if search_after is None:
            search_after = [hit_timestamp_ms(last_hit), last_hit["_id"]]

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
