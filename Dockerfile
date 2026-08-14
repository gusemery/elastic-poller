   FROM python:3.12

   RUN mkdir /app
   WORKDIR /app

   COPY .env ./.env
   COPY *.py ./
   COPY *.yaml ./
   COPY requirements.txt requirements.txt
   RUN pip3 install --no-cache-dir -r /app/requirements.txt
   ENV BOOKMARK_PATH=/data/

   CMD [ "python3", "-u", "/app/elastic_poller.py" ]
