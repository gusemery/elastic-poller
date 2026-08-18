"""Poll cycle orchestration."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Tuple

import lm_logs
from elastic_poller import bookmark, config, delivery, elasticsearch


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
        event = delivery.create_event(hit)
        event.set_enrichment_value("lm_bookmark", query_bookmark)
        event.set_enrichment_value("lm_watermark", watermark)
        event.set_enrichment_value("lm_loaded", bookmark_loaded)
        event.set_enrichment_value("lm_elastic_index", config.ELASTIC_INDEX)

        try:
            space_ids = hit["_source"].get("kibana", {}).get("space_ids", [])
            if space_ids:
                event.set_enrichment_value("lm_service_id", ",".join(space_ids))
        except (TypeError, AttributeError):
            pass

        cef = event.get_cef()
        cef["cef"]["event_source_id"] = cef["cef"]["source_record"]["_id"]

        if "," in cef["cef"]["event_ci"]:
            ci = cef["cef"]["event_ci"]
            cef["cef"]["event_ci"] = ci.split(",")[0]
            try:
                if cef["cef"]["source_record"]["_source"]["event"].get("end"):
                    cef["cef"]["event_severity"] = 0
            except (TypeError, NameError) as exc:
                config.logger.debug("Could not evaluate event.end for severity: %s", exc)

        event_list.append(cef)
        last_timestamp_ms = elasticsearch.hit_timestamp_ms(hit)
        config.logger.debug(
            "Processed hit timestamp %s",
            elasticsearch.epoch_ms_to_zulu(last_timestamp_ms),
        )

    return event_list, last_timestamp_ms


def _log_poll_summary(
    *,
    status: str,
    initial_bookmark_ms: int,
    final_bookmark_ms: int,
    pages_fetched: int,
    events_delivered: int,
    issues: List[str],
    duration_ms: int,
) -> None:
    """Emit one operational summary per poll cycle for stderr and LM Logs."""
    bookmark_advanced = final_bookmark_ms != initial_bookmark_ms
    errors_encountered = bool(issues)
    issue_text = ",".join(issues) if issues else "none"

    message = (
        f"Poll cycle finished: status={status}, events_delivered={events_delivered}, "
        f"pages_fetched={pages_fetched}, bookmark_advanced={str(bookmark_advanced).lower()}, "
        f"errors={str(errors_encountered).lower()}, issues={issue_text}, "
        f"bookmark={elasticsearch.epoch_ms_to_zulu(final_bookmark_ms)}, "
        f"duration_ms={duration_ms}"
    )

    lm_logs.log_with_context(
        config.logger,
        lm_logs.operational_log_level(),
        message,
        event_type="poll_summary",
        status=status,
        bookmark_advanced=bookmark_advanced,
        initial_bookmark_ms=initial_bookmark_ms,
        initial_bookmark_zulu=elasticsearch.epoch_ms_to_zulu(initial_bookmark_ms),
        final_bookmark_ms=final_bookmark_ms,
        final_bookmark_zulu=elasticsearch.epoch_ms_to_zulu(final_bookmark_ms),
        pages_fetched=pages_fetched,
        events_delivered=events_delivered,
        errors_encountered=errors_encountered,
        issues=",".join(issues) if issues else "",
        duration_ms=duration_ms,
    )


def poll_cycle(bookmark_ms: int, watermark: int, bookmark_loaded: bool) -> int:
    """Run one poll cycle inside a point-in-time, paginating until drained or delivery fails."""
    cycle_started = time.monotonic()
    initial_bookmark_ms = bookmark_ms
    cycle_bookmark = bookmark_ms
    search_after = None
    updated_bookmark = bookmark_ms
    pages_fetched = 0
    events_delivered = 0
    status = "complete"
    issues: List[str] = []
    pit_id = None

    lm_logs.log_with_context(
        config.logger,
        logging.DEBUG,
        "Poll cycle started",
        event_type="poll_started",
        bookmark_ms=cycle_bookmark,
        bookmark_zulu=elasticsearch.epoch_ms_to_zulu(cycle_bookmark),
    )

    try:
        pit_id = elasticsearch.open_point_in_time(
            config.ELASTIC_INDEX,
            keep_alive=config.ELASTIC_PIT_KEEP_ALIVE,
            **elasticsearch._es_conn_kwargs(),
        )
    except (elasticsearch.ElasticsearchQueryError, ValueError) as exc:
        status = "pit_open_failed"
        issues.append(f"pit_open_failed: {exc}")
        config.logger.error(
            "Could not open point-in-time on %s: %s", config.ELASTIC_INDEX, exc
        )
    else:
        try:
            while True:
                lm_logs.log_with_context(
                    config.logger,
                    logging.DEBUG,
                    "Querying Elasticsearch",
                    bookmark_ms=cycle_bookmark,
                    bookmark_zulu=elasticsearch.epoch_ms_to_zulu(cycle_bookmark),
                )
                hits, _took, pit_id = elasticsearch.fetch_elasticsearch_hits(
                    cycle_bookmark, search_after=search_after, pit_id=pit_id
                )
                pages_fetched += 1

                if not hits:
                    break

                event_list, last_timestamp_ms = process_hits(
                    hits, cycle_bookmark, watermark, bookmark_loaded
                )

                lm_logs.log_with_context(
                    config.logger,
                    logging.DEBUG,
                    "Events mapped for delivery",
                    cycle_bookmark_ms=cycle_bookmark,
                    last_timestamp_ms=last_timestamp_ms,
                    last_timestamp_zulu=elasticsearch.epoch_ms_to_zulu(
                        last_timestamp_ms
                    ),
                    event_count=len(event_list),
                )

                if not delivery.send_event(event_list):
                    status = "delivery_failed"
                    issues.append("edwin_delivery_failed")
                    config.logger.warning(
                        "Edwin delivery failed; bookmark not advanced "
                        "(last successful bookmark %s)",
                        updated_bookmark,
                    )
                    break

                previous_bookmark = updated_bookmark
                updated_bookmark = last_timestamp_ms
                bookmark.set_bookmark(updated_bookmark)
                bookmark_loaded = True
                events_delivered += len(event_list)

                lm_logs.log_with_context(
                    config.logger,
                    logging.DEBUG,
                    "Bookmark advanced",
                    previous_bookmark_ms=previous_bookmark,
                    new_bookmark_ms=updated_bookmark,
                    new_bookmark_zulu=elasticsearch.epoch_ms_to_zulu(updated_bookmark),
                )

                if len(hits) < config.ELASTIC_BATCH_SIZE:
                    break

                search_after = hits[-1].get("sort")
                if not search_after:
                    status = "incomplete"
                    issues.append("hits_missing_sort_values")
                    config.logger.error(
                        "Elasticsearch returned hits without sort values; ending cycle"
                    )
                    break

        except elasticsearch.ElasticsearchQueryError as exc:
            if exc.is_missing_context:
                status = "pit_expired"
                issues.append("pit_expired_mid_cycle")
                config.logger.warning(
                    "Point-in-time expired mid-cycle; retrying next interval"
                )
                pit_id = None
            else:
                status = "es_error"
                issues.append(f"elasticsearch_query_failed: {exc}")
                config.logger.error("Elasticsearch query failed mid-cycle: %s", exc)
        finally:
            elasticsearch.close_point_in_time(pit_id, **elasticsearch._es_conn_kwargs())

    duration_ms = int((time.monotonic() - cycle_started) * 1000)
    _log_poll_summary(
        status=status,
        initial_bookmark_ms=initial_bookmark_ms,
        final_bookmark_ms=updated_bookmark,
        pages_fetched=pages_fetched,
        events_delivered=events_delivered,
        issues=issues,
        duration_ms=duration_ms,
    )
    return updated_bookmark


def read_elastic_records() -> str:
    """Load fixture data for local testing (output_500.json)."""
    config.logger.info("Loading records from output_500.json")
    with open("output_500.json", "r") as fh:
        return fh.read()


def log_startup() -> None:
    """Emit a sanitized configuration snapshot at startup."""
    context = lm_logs.build_startup_context(
        edwin_org=config.EDWIN_ORG,
        elastic_url=config.ELASTIC_URL,
        elastic_index=config.ELASTIC_INDEX,
        elastic_query=config.ELASTIC_QUERY,
        elastic_batch_size=config.ELASTIC_BATCH_SIZE,
        elastic_verify_ssl=config.ELASTIC_VERIFY_SSL,
        elastic_pit_keep_alive=config.ELASTIC_PIT_KEEP_ALIVE,
        poller_interval=str(config.PAUSE_INTERVAL),
        bookmark_path=bookmark.bookmark_file,
        lm_logs_enabled=config.LM_LOGS_ENABLED,
    )
    metadata = {key: value for key, value in context.items() if key != "msg"}
    lm_logs.log_with_context(
        config.logger, lm_logs.operational_log_level(), context["msg"], **metadata
    )


readElasticRecords = read_elastic_records
