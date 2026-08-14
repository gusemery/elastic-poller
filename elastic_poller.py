import sys
import json
import os
import logging
from typing import Any, Dict, Optional
import requests
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import common_event
import dexda_request
from dotenv import load_dotenv
from dateutil import parser
#from prometheus_client import (
#    Counter,
#    Histogram,
#    Gauge,
#)

load_dotenv()

BASE_LABELS = ["destination"]

#hits_sent = Gauge(
#    "hits_sent_current",
#    "Total number of sent records:",
#    BASE_LABELS,
#)

#send_attempts = Counter(
#    "send_attempts_total",
#    "Total number of send attempts.",
#    BASE_LABELS,
#)

#send_outcomes = Counter(
#    "send_outcomes_total",
#    "Send outcomes by status (success/failure) and reason.",
#    BASE_LABELS + ["status", "reason"],  # status=success|failure, reason=ok|timeout|5xx|validation|unknown
#)

#retries = Counter(
#    "send_retries_total",
#    "Total number of send retries performed.",
#    BASE_LABELS,  # reason=timeout|5xx|throttle|...
#)

OK = 0
ERROR_CODE_UNKNOWN = 1
ERROR_CODE_VALIDATION_FAILED = 2
ERROR_CODE_EVENT_MAPPING_FAILED = 3
ERROR_CODE_EVENT_DELIVERY_FAILED = 4
ERROR_CODE_HTTP= 5
ERROR_CODE_UNEXPECTED = 6

ELASTIC_USER = os.getenv('ELASTIC_USER')
ELASTIC_PASS = os.getenv('ELASTIC_PASS')
ELASTIC_TOKEN = os.getenv('ELASTIC_TOKEN')
ELASTIC_URL = os.getenv('ELASTIC_URL')
ELASTIC_BATCH_SIZE = os.getenv('ELASTIC_BATCH_SIZE', 500)
ELASTIC_INDEX = os.getenv('ELASTIC_INDEXS')

PAUSE_INTERVAL = os.getenv('POLLER_INTERVAL', 240)
DEXDA_ORG = os.getenv('DEXDA_ORG')
DEXDA_ID = os.getenv('DEXDA_ID')
DEXDA_TOKEN = os.getenv('DEXDA_TOKEN')

BOOKMARK_PATH = os.getenv('BOOKMARK_PATH')
DEBUG = os.getenv('DEBUG', False)
LOG = os.getenv('LOG', True)
logging.disabled = LOG

#bookmark_file = BOOKMARK_PATH + '/' + DEXDA_ORG + '.elastic.bookmark' 
bookmark_file = './' + DEXDA_ORG + '.elastic.bookmark'



print("Using LM portal: " + str(DEXDA_ORG))
print("Using ES portal: " + str(ELASTIC_URL))
print("Using bookmarkfile: " + str(bookmark_file))



class ElasticsearchQueryError(Exception):
    """Raised when the Elasticsearch query fails."""

def log(msg, *args):
    if LOG == True:
       sys.stderr.write(msg + " ".join([str(a) for a in args]) + "\n")

def debug(message, *args):
    if DEBUG == True:
      currentTime = datetime.now().strftime("%d.%b %Y %H:%M:%S")
      print(currentTime + ' - ' +  message + " ".join([str(a) for a in args]))

def build_logs_query(
    text: str,
    start: str = "now-15m",
    end: str = "now",
    size: int = 10000,
    timestamp_field: str = "@timestamp"):

    return {
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
                                "gte": start,
                                "lte": end
                            }
                        }
                    }
                ]
            }
        },
        "sort": [{timestamp_field: {"order": "asc"}}]
    }
def query_elasticsearch(
    index: str,
    query: Dict[str, Any],
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
    verify_ssl: bool = True,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Query an Elasticsearch index and return the JSON response.

    Args:
        base_url: Elasticsearch base URL, e.g. "https://your-es-host:9200"
        index: Index name or pattern, e.g. "logs-*"
        query: Elasticsearch query DSL body
        username: Optional basic auth username
        password: Optional basic auth password
        api_key: Optional API key. If provided, overrides basic auth.
                 Pass the raw base64 API key value expected by Elasticsearch.
        verify_ssl: Whether to verify SSL certificates
        timeout: Request timeout in seconds

    Returns:
        Parsed JSON response from Elasticsearch

    Raises:
        ValueError: If auth parameters are invalid
        ElasticsearchQueryError: If the request fails
    """
    base_url = ELASTIC_URL

    if api_key and (username or password):
        raise ValueError("Use either api_key or username/password, not both.")

    if len(base_url) < 5:
        raise ValueError('You have not supplied a correct url for ElasticSearch.')
    
    url = f"{base_url.rstrip('/')}/{index}/_search"

    headers = {
        "Content-Type": "application/json",
    }

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
        raise ElasticsearchQueryError(f"Elasticsearch query failed: {exc}.{detail}") from exc


def epoch_ms_to_zulu(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def createEvent(payload):
    event_list = []
    try:
        event = common_event.CommonEvent.new_from_file(
            mapping_file_name = "elastic_event_mappings.yaml",
            mapping_file_path = ".",
            original_record = payload
        )
        log("INFO - successfully mapped event payload to cef")
    except Exception as error:
        log("ERROR - exception in createEvent: %s", str(error))
        sys.exit(ERROR_CODE_EVENT_MAPPING_FAILED)
    return(event)

def send_event(event_list):
    auth_dict = {
        "dexda_org": DEXDA_ORG,
        "client_id": DEXDA_ID,
        "client_secret": DEXDA_TOKEN
    }
    try:
        dxd_request = dexda_request.DexdaRequest.new_from_param(auth_dict=auth_dict)
        access_token = dxd_request.access_token
        dxd_request.send(access_token=access_token, data=event_list)
        log("INFO - successfully sent Dexda event")
    except Exception as error:
        log("ERROR - exception in send_event: ", str(error))
        sys.exit(ERROR_CODE_VALIDATION_FAILED)
    return True

def getBookmark():
    # Check if the file exists
    if not os.path.exists(bookmark_file):
        # If file doesn't exist, create it and write 0
        print("INFO - Bookmark file not found, creating it")
        with open(bookmark_file, 'w') as fh:
            fh.write('0')

    # Open the file and read its contents
    with open(bookmark_file, 'r') as fh:
        f = fh.read()
    return int(f)


def readCEF():
   with open('cef.json','r') as fh:
      result = fh.read()
   return json.loads(result)

def writeCEF(buffer):
   events = json.dumps(buffer, indent=4)
   with open('cef.json', 'w') as fh:
     fh.write(events)
   fh.close

def setBookmark(bookmark):
    with open(bookmark_file, 'w') as fh:
        fh.write(str(bookmark))
    fh.close()

def readElasticRecords():
   print('Loading records....')
   with open('output_500.json','r') as fh:
      result = fh.read()
   fh.close
   return result

#
#   Main section.  This runs the main routine, and all of the processing is passed to functions
#
if __name__ == '__main__':
    done = 0

    two_hours_ago = datetime.now() - timedelta(hours=2)
    timestamp = int(two_hours_ago.timestamp() * 1000)
    bookmark = timestamp
    bookmark_loaded = False
    if (getBookmark() > 0):
        bookmark_loaded = True
        bookmark = getBookmark()
    watermark = bookmark

    print(f"Using the bookmark of {bookmark}")


    while True:

        print(f"Time would be {epoch_ms_to_zulu(bookmark)}")

        queryBody = build_logs_query(text="*",size=ELASTIC_BATCH_SIZE, start=epoch_ms_to_zulu(bookmark))

        # result = json.loads(readElasticRecords())
        result = query_elasticsearch(index=ELASTIC_INDEX, query=queryBody, verify_ssl=False, timeout=60)

        event_payload = result

        print('took: ' + str(event_payload['took']))
        hits = event_payload.get('hits')
        hits = hits.get('hits')
        print('Found ' + str(len(hits)))

        #sys.exit(OK)

        df = pd.DataFrame(columns=['eventTimestamp', 'event'])

        for e in hits:
            r = {'eventTimestamp': e['_source']['@timestamp'], 'event': json.dumps(e)}        
            df = df._append(r, ignore_index=True)

        count=0
        event_list = []
        for i in df.index:
            count = count + 1
            e = json.loads(str(df['event'][i]))
            event = createEvent(e)

            # Set enrichments here
#            namespace = ",".join(e['_source']['kibana']['space_ids'])
#            event.set_enrichment_value('lm_service_id', namespace)
            event.set_enrichment_value("lm_bookmark", bookmark)
            event.set_enrichment_value("lm_watermark", watermark)
            event.set_enrichment_value("lm_loaded", bookmark_loaded)
            event.set_enrichment_value("lm_elastic_index", ELASTIC_INDEX)
            # event.set_enrichment_value('lm_service_id', e['_source']['kibana']['space_ids'])

            cef = event.get_cef()

            cef["cef"]["event_source_id"] = cef["cef"]["source_record"]["_id"]
            
            if "," in cef["cef"]["event_ci"]:
                ci = cef["cef"]["event_ci"]
                cef["cef"]["event_ci"] = ci.split(',')[0]
                try:
                 if cef['cef']['source_record']['_source']['event'].get('end'):
                     # print("End exisis")
                     cef['cef']['event_severity'] = 0
                except (TypeError, NameError) as e:
                  print(e.name)
            currentTime = df['eventTimestamp'][i]      
            event_list.append(cef)
            dt = datetime.fromisoformat(currentTime.replace("Z", "+00:00"))
            timestamp = (dt.timestamp() *1000)

            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            print(dt.isoformat().replace("+00:00", "Z"))
        print('BK: ' + str(bookmark))
        print('TS: ' + str(timestamp))
        if len(event_list) > 0:
           print(f"Created {str(len(event_list))} edwin events.")
           send_event(event_list)
           setBookmark(int(timestamp))
           bookmark = timestamp
 
        print(f"Sleeping {str(PAUSE_INTERVAL)} seconds")
        time.sleep(int(PAUSE_INTERVAL))


    sys.exit(OK)
