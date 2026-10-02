"""Tests de l'ingestion raw : parquet partitionne, archive, idempotence, erreurs."""

import pytest

from afrishop.ingest_raw import SOURCES, ingest_source

pytestmark = pytest.mark.spark


def _write_source(tmp_path, name="orders", rows=3):
    (tmp_path / "source").mkdir(parents=True, exist_ok=True)
    lines = ["order_id,total_amount"] + [f"O{i},{i * 10}" for i in range(rows)]
    (tmp_path / "source" / f"{name}.csv").write_text("\n".join(lines), encoding="utf-8")


def test_ingest_source_writes_partitioned_parquet_and_archive(spark, tmp_path):
    _write_source(tmp_path)
    metrics = ingest_source(spark, "orders", "2026-01-15", tmp_path)
    assert metrics["status"] == "success" and metrics["rows_in"] == metrics["rows_out"] == 3
    target = tmp_path / "raw" / "orders" / "ingestion_date=2026-01-15"
    df = spark.read.parquet(str(target))
    assert {"_source_file", "_ingested_at", "_run_date"} <= set(df.columns)
    assert (tmp_path / "raw" / "_archive" / "orders" / "2026-01-15" / "orders.csv").exists()


def test_ingest_source_is_idempotent_for_the_same_run_date(spark, tmp_path):
    _write_source(tmp_path)
    ingest_source(spark, "orders", "2026-01-15", tmp_path)
    again = ingest_source(spark, "orders", "2026-01-15", tmp_path)
    target = tmp_path / "raw" / "orders" / "ingestion_date=2026-01-15"
    assert spark.read.parquet(str(target)).count() == again["rows_out"] == 3


def test_ingest_source_missing_file_raises(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        ingest_source(spark, "orders", "2026-01-15", tmp_path)


def test_sources_list_matches_the_six_csv_files():
    assert sorted(SOURCES) == sorted(
        ["orders", "order_lines", "customers", "products", "payments", "deliveries"]
    )
