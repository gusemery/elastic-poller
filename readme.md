# Edwin Event Integration (ElasticSearch -> Edwin)

This project contains scripts and configurations for integrating events from ElasticSearch with Edwin.

## Files

- `dexda_event_poller.py`: Main script that polls for events and sends them to Edwin.
- `dexda_request.py`: Module for handling HTTP requests to Edwin.
- `common_event.py`: Class for representing and mapping events to Edwin Common Event Format (CEF).
- `elastic_event_mappings.yaml`: Configuration file for mapping ElasticSearch events to Edwin CEF.
- `requirements.txt`: List of Python dependencies for the project.

## Setup

1. Install the required dependencies:

   ```
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt

   ```

2. Set up your Edwin/ElasticSearch authentication:
   - Create a `.env` file with your Edwin organization and API key or supply the values directly in the `elastic_poller.py` file:
     ```
      DEXDA_ORG="portal_prefix"
      DEXDA_ID="" - Edwin ID
      DEXDA_TOKEN="" - Edwin Key
      ELASTIC_URL="http://[servername]:9200/" - Url to ElasticSearch w/Port
      ELASTIC_INDEXS = ".ds-file*" - Index you wish to search
      ELASTIC_BATCH_SIZE = 500 - Number of records to ingest at a time
      DEBUG = False - Debug to console
      LOG = False - Log to console
      POLLER_INTERVAL = 240
      ELASTIC_USER="username"
      ELASTIC_PASS="Password"
      ELASTIC_TOKEN="token"
      DEBUG = False
      LOG = False
     ```

   - The system uses EITHER a bearer token or username/password to access ElasticSearch.

3. Configure the event mappings:
   - Review and adjust the `elastic_event_mappings.yaml` file to ensure correct mapping of ElasticSearch events to Edwin CEF.

4. Execute the poller
   - Run the following command in the directory.

   ```
   
   python3 elastic_poller.py
   
   ```

## Usage

Run the main script using Python 3 to start polling for events and sending them to Edwin:

### Edwin Event Poller

The script will continuously poll for new events, process them, and send them to Edwin. It will sleep for 30 seconds between each polling cycle.

## Customization

- Adjust the polling interval by modifying the `time.sleep()` value in `elastic_poller.py`.
- Modify the event processing logic in `elastic_poller.py` if needed.
- Update the `elastic_event_mappings.yaml` file to change how ElasticSearch events are mapped to Edwin CEF.

## Notes

- Ensure that your Edwin API key has the necessary permissions to send events.
- The script uses a bookmark system to keep track of the last processed event. This helps in resuming from where it left off in case of interruptions.
- Error handling and retries are implemented for robustness.
- There will be a file created called '<edwinorg>.elastic.bookmark' that holds the prior 'timestamp' to the microsecond to query from next.  Allowing only passing NEW records to Edwin. If this file doesn't exist, the poller starts at Now - 2 hours.


## Dependencies

See `requirements.txt` for the list of Python packages required.

## Aditional topics

There is a Dockerfile that exists to create a custom 'local' docker container.  

```
docker build -t lm/elastic-poller .

```