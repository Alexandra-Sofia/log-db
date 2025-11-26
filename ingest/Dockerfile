FROM python:3.12-slim

WORKDIR /ingest

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x wait_for_postgres.sh execute.py

