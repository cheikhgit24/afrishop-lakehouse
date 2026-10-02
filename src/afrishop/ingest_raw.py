"""Ingestion batch des CSV sources vers la zone raw (Parquet).

- Aucune transformation : toutes les colonnes restent en texte (donnees brutes).
- Colonnes de tracabilite ajoutees : _source_file, _ingested_at, _run_date.
- Idempotent : relancer une meme run_date REMPLACE sa partition, sans doublon.
- Le CSV d'origine est archive pour audit.

Usage :
    python -m afrishop.ingest_raw --run-date 2026-09-30
    python -m afrishop.ingest_raw --run-date 2026-09-30 --sources orders payments
"""

from __future__ import annotations

import argparse
import shutil
import time
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from afrishop.logging_utils import get_logger, log_event
from afrishop.spark_session import get_spark

SOURCES = ["orders", "order_lines", "customers", "products", "payments", "deliveries"]
log = get_logger("afrishop.ingest_raw")


def ingest_source(spark: SparkSession, source: str, run_date: str, data_dir: Path) -> dict:
    started = time.time()
    src_file = data_dir / "source" / f"{source}.csv"
    if not src_file.exists():
        raise FileNotFoundError(f"Fichier source introuvable : {src_file}")

    df = spark.read.csv(str(src_file), header=True, inferSchema=False, encoding="UTF-8")
    df = (
        df.withColumn("_source_file", F.lit(src_file.name))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_date", F.lit(run_date))
    )

    target = data_dir / "raw" / source / f"ingestion_date={run_date}"
    df.write.mode("overwrite").parquet(str(target))

    archive_dir = data_dir / "raw" / "_archive" / source / run_date
    archive_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_file, archive_dir / src_file.name)

    rows_in = spark.read.csv(str(src_file), header=True).count()
    rows_out = spark.read.parquet(str(target)).count()
    status = "success" if rows_in == rows_out else "row_count_mismatch"
    metrics = {
        "source": source,
        "run_date": run_date,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "duration_s": round(time.time() - started, 2),
        "status": status,
    }
    log_event(log, "source ingested", **metrics)
    if status != "success":
        raise RuntimeError(f"Ecart de lignes pour {source} : {rows_in} != {rows_out}")
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-date", default=date.today().isoformat())
    parser.add_argument("--sources", nargs="+", default=SOURCES, choices=SOURCES)
    parser.add_argument("--data-dir", default="/opt/airflow/data", type=Path)
    args = parser.parse_args()

    spark = get_spark("ingest_raw")
    try:
        for source in args.sources:
            ingest_source(spark, source, args.run_date, args.data_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
