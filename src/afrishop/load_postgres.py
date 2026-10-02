"""Charge les tables Delta de la zone curated dans le schema PostgreSQL `curated`.

Rechargement complet (truncate + insert) : relancer ne cree aucun doublon.

Usage :
    python -m afrishop.load_postgres
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from afrishop.logging_utils import get_logger, log_event
from afrishop.spark_session import get_spark

TABLES = ["orders", "order_lines", "customers", "products", "payments", "deliveries", "_quarantine"]
PG_PACKAGE = "org.postgresql:postgresql:42.7.4"
log = get_logger("afrishop.load_postgres")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="/opt/airflow/data", type=Path)
    args = parser.parse_args()

    host = os.environ.get("POSTGRES_HOST", "postgres")
    url = f"jdbc:postgresql://{host}:5432/{os.environ['POSTGRES_DB']}?reWriteBatchedInserts=true"
    props = {
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "driver": "org.postgresql.Driver",
        "batchsize": "5000",
    }

    spark = get_spark("load_postgres", extra_packages=[PG_PACKAGE])
    try:
        for table in TABLES:
            started = time.time()
            df = spark.read.format("delta").load(str(args.data_dir / "curated" / table))
            target = f"curated.{table.lstrip('_')}"
            df.write.mode("overwrite").option("truncate", "true").jdbc(url, target, properties=props)
            rows = spark.read.jdbc(url, target, properties=props).count()
            log_event(
                log, "table loaded", table=target, rows=rows, duration_s=round(time.time() - started, 1)
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
