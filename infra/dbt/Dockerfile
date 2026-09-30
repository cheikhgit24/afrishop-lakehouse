FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "dbt-core~=1.8.0" "dbt-postgres~=1.8.0"

WORKDIR /usr/app/dbt
ENTRYPOINT ["dbt"]