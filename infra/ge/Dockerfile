FROM python:3.11-slim

RUN pip install --no-cache-dir "great-expectations~=1.3" "sqlalchemy>=2.0,<2.1" \
    psycopg2-binary pandas pytest

WORKDIR /opt/project
ENV PYTHONPATH=/opt/project/src
