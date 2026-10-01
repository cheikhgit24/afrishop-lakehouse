FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY infra/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip as c; b=SparkSession.builder.master('local[1]').appName('warm').config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'); s=c(b).getOrCreate(); s.range(1).write.format('delta').save('/tmp/warm'); s.stop()"
# --- Phase 7 : environnements isoles pour dbt et Great Expectations ---
# (venv separes pour eviter tout conflit de dependances avec Airflow/pyspark)
RUN python -m venv /opt/airflow/venvs/dbt \
 && /opt/airflow/venvs/dbt/bin/pip install --no-cache-dir "dbt-core~=1.8.0" "dbt-postgres~=1.8.0" \
 && python -m venv /opt/airflow/venvs/ge \
 && /opt/airflow/venvs/ge/bin/pip install --no-cache-dir "great-expectations~=1.3" \
      "sqlalchemy>=2.0,<2.1" psycopg2-binary pandas
