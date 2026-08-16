# Edwin Event Integration (Elasticsearch → Edwin)

Polls Elasticsearch for Kibana alerting event-log documents and forwards them to Edwin (Dexda) as CEF events.

## Supported versions

| Component | Supported | Notes |
|-----------|-----------|-------|
| **Elasticsearch** | **8.x** (recommended), **7.12+** | Requires `search_after` pagination with `_shard_doc` sort tie-breaker (added in ES 7.12). ES 7.11 and older are not supported. |
| **Kibana** | **7.x / 8.x** | Poller reads the Kibana alerting **event log** (e.g. `.kibana-event-log-ds`), not raw log/filebeat indices |

Elasticsearch **7.x reached end of support in January 2026**. New deployments should use **Elasticsearch 8.x** with Kibana 8 alerting.

## Files

| File | Purpose |
|------|---------|
| `elastic_poller.py` | Main poller: bookmark, ES query, pagination, Edwin delivery |
| `dexda_request.py` | HTTP client for Edwin OAuth and event ingestion |
| `common_event.py` | Maps raw records to Common Event Format (CEF) |
| `elastic_event_mappings.yaml` | JSONPath mappings from ES documents to CEF fields |
| `test_elastic_poller.py` | Unit tests for bookmark, query, pagination, and mapping |

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
| `ELASTIC_INDEXS` | Index or pattern to search (production: `.kibana-event-log-ds`) |
| `ELASTIC_BATCH_SIZE` | Hits per ES page (default `500`) |
| `ELASTIC_QUERY` | Optional Lucene `query_string` filter (default `*`) |
| `ELASTIC_VERIFY_SSL` | Verify TLS certs for ES (default `false`) |
| `ELASTIC_USER` / `ELASTIC_PASS` | Basic auth (optional) |
| `ELASTIC_TOKEN` | API key auth (optional; use instead of user/pass) |
| `POLLER_INTERVAL` | Seconds between poll cycles (default `240`) |
| `BOOKMARK_PATH` | Directory for bookmark file (default: current directory) |
| `DEBUG` | Print debug output (`true`/`false`) |
| `LOG` | Enable logging (`true`/`false`) |

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
- If no bookmark file exists, polling starts at **now − 2 hours**
- Bookmark is **not** advanced if Edwin delivery fails

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

Start a single-node ES 8 instance:

```bash
docker run -d --name elastic-poller-es \
  -p 9200:9200 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  -e "ES_JAVA_OPTS=-Xms256m -Xmx256m" \
  docker.elastic.co/elasticsearch/elasticsearch:8.19.0
```

Point `.env` at `http://localhost:9200`, seed a test index, and run the poller.

## Tests

```bash
python -m unittest test_elastic_poller.py -v
```

CI runs the same command on push and pull requests (see `.github/workflows/test.yml`).

## Notes

- Use `.kibana-event-log-ds` (or your Kibana event log data stream) for alerting events — not filebeat log indices
- `metrics.alert.threshold` and similar rule types carry better correlation data than `logs.alert.document.count` execute-start events
- Set `ELASTIC_VERIFY_SSL=true` in production when Elasticsearch uses valid TLS certificates
