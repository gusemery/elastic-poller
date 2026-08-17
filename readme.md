# Edwin Event Integration (Elasticsearch → Edwin)

Polls Elasticsearch for Kibana alerting event-log documents and forwards them to Edwin (Dexda) as CEF events.

## Supported versions

| Component | Supported | Notes |
|-----------|-----------|-------|
| **Elasticsearch** | **8.x / 9.x** (recommended), **7.12+** | Each poll cycle opens a point-in-time (`_pit`, ES 7.10+) and paginates with `search_after`, using `_shard_doc` as the sort tie-breaker. `_shard_doc` was added in ES 7.12 and is only valid inside a PIT, so ES 7.11 and older are not supported. |
| **Kibana** | **7.x / 8.x / 9.x** | Poller reads the Kibana alerting **event log** (e.g. `.kibana-event-log-ds`), not raw log/filebeat indices |

CI runs the integration suite against **8.11.4, 8.19.20 and 9.5.1**. 8.11.4 is
pinned deliberately: Elastic relaxed the `_shard_doc` validation during the 8.x
line, so 8.11.4 rejects `_shard_doc` outside a point-in-time with HTTP 400 while
8.19.20 and 9.5.1 accept it. Testing only the latest releases would miss that
class of regression.

Elasticsearch **7.x reached end of support in January 2026**. New deployments should use **Elasticsearch 8.x** with Kibana 8 alerting.

## Files

| File | Purpose |
|------|---------|
| `elastic_poller.py` | Main poller: bookmark, ES query, pagination, Edwin delivery |
| `dexda_request.py` | HTTP client for Edwin OAuth and event ingestion |
| `common_event.py` | Maps raw records to Common Event Format (CEF) |
| `elastic_event_mappings.yaml` | JSONPath mappings from ES documents to CEF fields |
| `test_elastic_poller.py` | Unit tests for bookmark, query, pagination, and mapping |
| `test_integration_elasticsearch.py` | Integration tests against a real Elasticsearch (skipped unless `ES_TEST_URL` is set) |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

## Configuration

Create a `.env` file (see `.env.example`):

| Variable | Description |
|----------|-------------|
| `DEXDA_ORG` | Edwin portal prefix |
| `DEXDA_ID` | Edwin API client ID |
| `DEXDA_TOKEN` | Edwin API client secret |
| `ELASTIC_URL` | Elasticsearch base URL (e.g. `https://es-host:9200`) |
| `ELASTIC_INDEXS` | Index, comma-separated list, or wildcard pattern to search (production: `.kibana-event-log-ds`). The trailing `S` is a legacy variable name, kept for compatibility with deployed `.env` files. |
| `ELASTIC_BATCH_SIZE` | Hits per ES page (default `500`) |
| `ELASTIC_QUERY` | Optional Lucene `query_string` filter (default `*`) |
| `ELASTIC_VERIFY_SSL` | Verify TLS certs for ES (default `false`) |
| `ELASTIC_PIT_KEEP_ALIVE` | Point-in-time lease per poll cycle (default `5m`). Extended on every page, so it only needs to cover the gap between two pages. |
| `ELASTIC_USER` / `ELASTIC_PASS` | Basic auth (optional) |
| `ELASTIC_TOKEN` | API key auth (optional; use instead of user/pass) |
| `POLLER_INTERVAL` | Seconds between poll cycles (default `240`) |
| `BOOKMARK_PATH` | Directory for bookmark file (default: current directory) |
| `DEBUG` | Print debug output (`true`/`false`) |
| `LOG` | Enable logging (`true`/`false`) |

### Selecting indices

`ELASTIC_INDEXS` is passed straight through to Elasticsearch, so it accepts any
index expression the ES path syntax supports — the plural name is not a
misnomer:

```bash
ELASTIC_INDEXS=.kibana-event-log-ds              # one index or data stream
ELASTIC_INDEXS=.ds-file*                         # wildcard pattern
ELASTIC_INDEXS=logs-app-a,logs-app-b             # comma-separated list
```

A single point-in-time spans every matched index, so pagination stays correct
across all of them within a cycle.

### Elasticsearch query filter

`ELASTIC_QUERY` is applied at query time (not after fetch), so the bookmark only advances over documents that match.

Examples:

```env
# All documents (default)
ELASTIC_QUERY=*

# Exclude rule scheduler noise (recommended for environment correlation)
ELASTIC_QUERY=NOT event.action:execute-start

# Only alert state changes
ELASTIC_QUERY=event.action:(active-instance OR new-instance OR recovered-instance)
```

### Bookmark behavior

- Bookmark is stored as **epoch milliseconds** in `{BOOKMARK_PATH}/{DEXDA_ORG}.elastic.bookmark`
- Queries use an **exclusive** lower bound (`gt` + `epoch_millis`) so the last sent event is never re-fetched
- **`search_after`** pagination drains backlogs larger than `ELASTIC_BATCH_SIZE` within a single cycle
- Each cycle paginates inside a **point-in-time snapshot**, so pages cannot skip or duplicate documents when many events share the same millisecond, or when the index refreshes mid-cycle. The PIT is always released, including when delivery fails.
- If no bookmark file exists, polling starts at **now − 2 hours**
- Bookmark is **not** advanced if Edwin delivery fails
- An Elasticsearch error mid-cycle aborts that cycle, keeps the bookmark already committed, and retries on the next interval rather than exiting the process

> **Known limitation.** The bookmark has millisecond granularity, and it advances
> to the last delivered hit's `@timestamp` after every page. If a page boundary
> falls inside a group of documents sharing one millisecond and the cycle then
> aborts (delivery failure, PIT expiry, restart), the next cycle's `gt` bound
> skips the remainder of that group. The point-in-time makes pagination correct
> *within* a cycle; it does not close this gap *between* cycles.

### Enrichments sent to Edwin

Each event includes optional enrichments:

- `lm_bookmark` — query bookmark at cycle start
- `lm_watermark` — bookmark at process startup
- `lm_elastic_index` — index being polled
- `lm_service_id` — Kibana space IDs (`kibana.space_ids`), for environment correlation

## Run

```bash
python3 elastic_poller.py
```

## Docker

```bash
docker build -t lm/elastic-poller .
docker run -d --name elastic-poller -v elastic-poller-data:/data lm/elastic-poller
```

Requires a `.env` file in the build context (copied into the image at build time). `BOOKMARK_PATH` defaults to `/data/` in the container image.

## Local Elasticsearch testing

Start a single-node instance (swap the tag for `9.5.1` or `8.11.4` to match a CI leg):

```bash
docker run -d --name elastic-poller-es \
  -p 9200:9200 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  -e xpack.security.http.ssl.enabled=false \
  -e cluster.routing.allocation.disk.threshold_enabled=false \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
  docker.elastic.co/elasticsearch/elasticsearch:8.19.20
```

Point `.env` at `http://localhost:9200`, seed a test index, and run the poller.

## Tests

Run both suites from the **repository root** — `common_event` resolves the
mapping file relative to the working directory.

```bash
# Unit tests only. The integration module skips itself when ES_TEST_URL is unset.
python -m unittest discover -s . -p "test_*.py" -v

# Integration tests against a running Elasticsearch (see above).
ES_TEST_URL=http://localhost:9200 ES_REQUIRE_INTEGRATION=1 \
  python -m unittest test_integration_elasticsearch.py -v
```

`ES_REQUIRE_INTEGRATION=1` turns a missing `ES_TEST_URL` into a hard failure, so
a misconfigured run cannot pass by skipping every test.

CI (see `.github/workflows/test.yml`) runs the unit job on every push and pull
request, plus an integration job matrixed over Elasticsearch 8.11.4, 8.19.20 and
9.5.1.

## Notes

- Use `.kibana-event-log-ds` (or your Kibana event log data stream) for alerting events — not filebeat log indices
- `metrics.alert.threshold` and similar rule types carry better correlation data than `logs.alert.document.count` execute-start events
- Set `ELASTIC_VERIFY_SSL=true` in production when Elasticsearch uses valid TLS certificates
