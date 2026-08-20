"""Create and remove a disposable Kibana alerting test environment."""

from __future__ import annotations

import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request
import uuid


ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200").rstrip("/")
KIBANA_URL = os.getenv("KIBANA_URL", "http://localhost:5601").rstrip("/")
TEST_INDEX = os.getenv("KIBANA_TEST_INDEX", "elastic-poller-kibana-test")
RULE_NAME = os.getenv("KIBANA_TEST_RULE_NAME", "elastic-poller-kibana-test-rule")

rule_id: str | None = None


def request(
    method: str,
    url: str,
    body: object | None = None,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[int, object]:
    payload = None
    request_headers = {"Accept": "application/json", **(headers or {})}
    if body is not None:
        payload = json.dumps(body).encode()
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url,
        data=payload,
        headers=request_headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            content = response.read()
            return response.status, json.loads(content) if content else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        raise RuntimeError(f"{method} {url} returned HTTP {error.code}: {detail}") from error


def wait_for(url: str) -> None:
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            request("GET", url)
            return
        except (OSError, RuntimeError):
            time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {url}")


def create_test_event() -> None:
    event = {
        "@timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "message": "elastic-poller Kibana integration test event",
        "event": {
            "provider": "alerting",
            "action": "test-event",
            "kind": "alert",
            "category": ["test"],
        },
        "rule": {
            "id": "elastic-poller-test-rule",
            "name": RULE_NAME,
            "category": "test",
            "license": "basic",
        },
        "kibana": {
            "alert": {
                "rule": {
                    "rule_type_id": ".es-query",
                }
            },
            "space_ids": ["default"],
        },
    }
    request(
        "PUT",
        f"{ELASTICSEARCH_URL}/{TEST_INDEX}/_doc/{uuid.uuid4()}?refresh=wait_for",
        event,
    )


def create_rule() -> str:
    requested_rule_id = str(uuid.uuid4())
    status, response = request(
        "POST",
        f"{KIBANA_URL}/api/alerting/rule/{requested_rule_id}",
        {
            "name": RULE_NAME,
            "consumer": "stackAlerts",
            "rule_type_id": ".es-query",
            "schedule": {"interval": "10s"},
            "params": {
                "aggType": "count",
                "groupBy": "all",
                "searchType": "esQuery",
                "size": 100,
                "termSize": 5,
                "timeWindowSize": 5,
                "timeWindowUnit": "m",
                "thresholdComparator": ">",
                "threshold": [0],
                "index": [TEST_INDEX],
                "timeField": "@timestamp",
                "esQuery": '{"query":{"match_all":{}}}',
            },
            "actions": [],
            "tags": ["elastic-poller-test"],
        },
        headers={"kbn-xsrf": "true"},
    )
    if status != 200 or not isinstance(response, dict):
        raise RuntimeError(f"Unexpected rule response: {response}")
    return str(response.get("id", requested_rule_id))


def delete_rule() -> None:
    if not rule_id:
        return
    try:
        request(
            "DELETE",
            f"{KIBANA_URL}/api/alerting/rule/{rule_id}",
            headers={"kbn-xsrf": "true"},
        )
        print(f"Deleted Kibana rule {rule_id}", flush=True)
    except (OSError, RuntimeError) as error:
        print(f"Warning: could not delete Kibana rule {rule_id}: {error}", file=sys.stderr)


def stop_handler(_signum: int, _frame: object) -> None:
    delete_rule()
    raise SystemExit(0)


def main() -> None:
    global rule_id
    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    wait_for(f"{ELASTICSEARCH_URL}/_cluster/health")
    wait_for(f"{KIBANA_URL}/api/status")
    create_test_event()
    rule_id = create_rule()
    print(f"Created Kibana rule {rule_id}", flush=True)
    print(f"Test index: {TEST_INDEX}", flush=True)
    print("Kibana test environment is ready", flush=True)

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
