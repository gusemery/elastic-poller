FROM python:3.12

WORKDIR /app

COPY .env ./.env
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY *.py ./
COPY *.yaml ./

ENV BOOKMARK_PATH=/data/

CMD ["python3", "-u", "/app/elastic_poller.py"]
