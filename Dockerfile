FROM python:3.12

WORKDIR /app

COPY .env ./.env
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY elastic_poller/ edwin_request.py common_event.py lm_logs.py ./
COPY elastic_event_mappings.yaml ./

ENV BOOKMARK_PATH=/data/

CMD ["python3", "-u", "-m", "elastic_poller"]
