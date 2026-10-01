"""Zone curated : nettoyage, deduplication, quarantaine, anonymisation, ecriture Delta.

Lit la partition raw d'une run_date et alimente des tables Delta par MERGE
(upsert sur cle metier) : relancer une meme journee ne cree aucun doublon.

Regles (voir docs/profiling_report.md) :
- orders       : dedoublonnage sur order_id (updated_at le plus recent) ;
                 quarantaine si aucune ligne valide (NO_LINES) ou total incoherent (BAD_TOTAL).
- order_lines  : quarantaine si product_id inexistant (ORPHAN_PRODUCT) ou commande non retenue.
- customers    : e-mail et telephone re-haches en SHA-256 + sel, noms supprimes (minimisation) ;
                 customer_id ambigu -> quarantaine ; master_customer_id regroupe les doublons d'e-mail.
- products     : produits a prix nul ou cout > prix signales (dq_flag) et copies en quarantaine.
- payments     : un seul paiement CAPTURED retenu par commande (is_selected).
- deliveries   : delai, retard (>= 7 jours) et conflit de statut calcules.

Usage :
    python -m afrishop.curated --run-date 2026-09-30
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from afrishop.logging_utils import get_logger, log_event
from afrishop.spark_session import get_spark

TOL = 0.05
LATE_DAYS = 7
log = get_logger("afrishop.curated")


# ----------------------------------------------------------------------------- utilitaires
def read_raw(spark: SparkSession, data_dir: Path, source: str, run_date: str) -> DataFrame:
    return spark.read.parquet(str(data_dir / "raw" / source / f"ingestion_date={run_date}"))


def latest_by(df: DataFrame, key: str, order_col: str) -> DataFrame:
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc(), F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def to_quarantine(df: DataFrame, source: str, key: str, reason: Column, run_date: str) -> DataFrame:
    cols = [c for c in df.columns if not c.startswith("_") and c not in ("dq_flag", "dq_reason")]
    return df.select(
        F.lit(source).alias("source"),
        F.col(key).cast("string").alias("record_key"),
        reason.alias("reason"),
        F.lit(run_date).alias("run_date"),
        F.to_json(F.struct(*cols)).alias("payload"),
    )


def upsert(
    spark: SparkSession, df: DataFrame, path: Path, keys: list[str], partition_by: list[str] | None = None
) -> None:
    """MERGE sur cle metier (cree la table Delta au premier passage)."""
    p = str(path)
    if DeltaTable.isDeltaTable(spark, p):
        cond = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
        (
            DeltaTable.forPath(spark, p)
            .alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        writer = df.write.format("delta")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(p)


# ----------------------------------------------------------------------------- transformations
def build_products(raw: DataFrame) -> DataFrame:
    df = latest_by(raw, "product_id", "updated_at")
    flag = (
        F.when(F.col("unit_price").cast("double") == 0, F.lit("ZERO_PRICE"))
        .when(F.col("unit_cost").cast("double") > F.col("unit_price").cast("double"), F.lit("COST_GT_PRICE"))
    )
    return df.select(
        "product_id", "sku", "product_name", "category_id", "category_name", "subcategory_name",
        "brand", "seller_id",
        F.col("unit_cost").cast("decimal(10,2)").alias("unit_cost"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("weight_grams").cast("int").alias("weight_grams"),
        F.col("is_perishable").cast("boolean").alias("is_perishable"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        "product_status",
        flag.alias("dq_flag"),
        "_ingested_at",
    )


def ambiguous_customer_ids(raw_customers: DataFrame) -> DataFrame:
    return raw_customers.groupBy("customer_id").count().filter("count > 1").select("customer_id")


def build_customers(raw: DataFrame, salt: str) -> tuple[DataFrame, DataFrame]:
    w_id = Window.partitionBy("customer_id")
    df = raw.withColumn("_n", F.count("*").over(w_id))
    quarantine = df.filter("_n > 1")
    ok = df.filter("_n = 1").drop("_n")

    w_mail = Window.partitionBy("email_hash")
    ok = ok.withColumn("master_customer_id", F.min("customer_id").over(w_mail))

    def salted(col: str) -> Column:
        return F.sha2(F.concat_ws("|", F.lit(salt), F.col(col)), 256)

    clean = ok.select(
        "customer_id",
        "master_customer_id",
        (F.col("customer_id") != F.col("master_customer_id")).alias("is_duplicate_account"),
        salted("email_hash").alias("email_hash"),
        salted("phone_hash").alias("phone_hash"),
        F.col("birth_year").cast("int").alias("birth_year"),
        "gender",
        F.col("registration_date").cast("date").alias("registration_date"),
        "country_code", "city", "customer_segment", "preferred_payment_method",
        "loyalty_tier", "account_status",
        "_ingested_at",
    )
    return clean, quarantine


def selected_payments(raw_payments: DataFrame) -> DataFrame:
    """Un seul paiement CAPTURED par commande : le plus recent."""
    cap = raw_payments.filter("payment_status = 'CAPTURED'").withColumn(
        "_pd", F.col("payment_date").cast("timestamp")
    )
    w = Window.partitionBy("order_id").orderBy(F.col("_pd").desc(), F.col("payment_id"))
    return cap.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").select(
        "payment_id", "order_id", F.col("payment_amount").cast("decimal(12,2)").alias("captured_amount")
    )


def build_orders(
    raw_orders: DataFrame,
    raw_lines: DataFrame,
    raw_products: DataFrame,
    ambiguous_ids: DataFrame,
    selected: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    o = latest_by(raw_orders, "order_id", "updated_at").select(
        "order_id", "order_number", "customer_id",
        F.col("order_date").cast("timestamp").alias("order_date"),
        "order_status", "channel", "country_code", "delivery_address_city",
        "delivery_address_region", "delivery_address_zone", "promo_code", "currency",
        F.col("subtotal_amount").cast("decimal(12,2)").alias("subtotal_amount"),
        F.col("shipping_amount").cast("decimal(10,2)").alias("shipping_amount"),
        F.col("discount_amount").cast("decimal(10,2)").alias("discount_amount"),
        F.col("total_amount").cast("decimal(12,2)").alias("total_amount"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
        "_ingested_at",
    )

    valid_lines = raw_lines.join(raw_products.select("product_id"), "product_id", "left_semi")
    lines_sum = valid_lines.groupBy("order_id").agg(
        F.sum(F.col("line_total").cast("decimal(14,2)")).alias("lines_sum")
    )
    o = o.join(lines_sum, "order_id", "left")

    expected = F.col("subtotal_amount") + F.col("shipping_amount") - F.col("discount_amount")
    reason = (
        F.when(F.col("lines_sum").isNull(), F.lit("NO_LINES"))
        .when(F.abs(F.col("total_amount") - expected) > TOL, F.lit("BAD_TOTAL"))
    )
    o = o.withColumn("dq_reason", reason)

    quarantine = o.filter("dq_reason IS NOT NULL")
    ok = o.filter("dq_reason IS NULL").drop("dq_reason", "lines_sum")

    ambiguous = ambiguous_ids.withColumn("_amb", F.lit(True))
    ok = (
        ok.join(ambiguous, "customer_id", "left")
        .join(selected.select("order_id", "captured_amount"), "order_id", "left")
        .withColumn("customer_dq_flag", F.when(F.col("_amb"), F.lit("AMBIGUOUS_CUSTOMER_ID")))
        .withColumn("has_payment", F.col("captured_amount").isNotNull())
        .withColumn(
            "is_partial_payment",
            F.col("captured_amount").isNotNull()
            & (F.col("captured_amount") < F.col("total_amount") - F.lit(TOL)),
        )
        .withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
        .drop("_amb")
    )
    return ok, quarantine


def build_order_lines(
    raw_lines: DataFrame, raw_products: DataFrame, orders_clean: DataFrame
) -> tuple[DataFrame, DataFrame]:
    typed = raw_lines.select(
        "order_line_id", "order_id", "product_id", "product_sku",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("line_discount").cast("decimal(10,2)").alias("line_discount"),
        F.col("line_total").cast("decimal(12,2)").alias("line_total"),
        "seller_id", "_ingested_at",
    )
    known = raw_products.select("product_id").withColumn("_known", F.lit(True))
    kept = orders_clean.select("order_id", "order_year_month").withColumn("_order_ok", F.lit(True))
    j = typed.join(known, "product_id", "left").join(kept, "order_id", "left")
    reason = (
        F.when(F.col("_known").isNull(), F.lit("ORPHAN_PRODUCT"))
        .when(F.col("_order_ok").isNull(), F.lit("ORDER_NOT_IN_CLEAN"))
    )
    j = j.withColumn("dq_reason", reason)
    quarantine = j.filter("dq_reason IS NOT NULL")
    clean = j.filter("dq_reason IS NULL").drop("dq_reason", "_known", "_order_ok")
    return clean, quarantine


def build_payments(raw: DataFrame, selected: DataFrame) -> DataFrame:
    df = latest_by(raw, "payment_id", "payment_date")
    sel = selected.select("payment_id").withColumn("is_selected", F.lit(True))
    return (
        df.join(sel, "payment_id", "left")
        .select(
            "payment_id", "order_id", "payment_method", "payment_provider",
            F.col("payment_amount").cast("decimal(12,2)").alias("payment_amount"),
            "currency", "payment_status",
            F.col("payment_date").cast("timestamp").alias("payment_date"),
            "transaction_reference",
            F.coalesce(F.col("is_selected"), F.lit(False)).alias("is_selected"),
            "_ingested_at",
        )
    )


def build_deliveries(raw: DataFrame, orders_clean: DataFrame) -> tuple[DataFrame, DataFrame]:
    d = latest_by(raw, "delivery_id", "shipped_at").select(
        "delivery_id", "order_id", "carrier", "tracking_number", "delivery_status",
        F.col("shipped_at").cast("timestamp").alias("shipped_at"),
        F.col("delivered_at").cast("timestamp").alias("delivered_at"),
        F.col("delivery_attempts").cast("int").alias("delivery_attempts"),
        F.col("delivery_cost").cast("decimal(10,2)").alias("delivery_cost"),
        "_ingested_at",
    )
    o = orders_clean.select("order_id", "order_date", "order_status")
    j = d.join(o, "order_id", "left")
    quarantine = j.filter(F.col("order_date").isNull()).withColumn(
        "dq_reason", F.lit("ORDER_NOT_IN_CLEAN")
    )
    ok = (
        j.filter(F.col("order_date").isNotNull())
        .withColumn("delivery_delay_days", F.datediff("delivered_at", "order_date"))
        .withColumn("is_late", F.col("delivery_delay_days") >= F.lit(LATE_DAYS))
        .withColumn(
            "status_conflict",
            (F.col("delivery_status") == "DELIVERED")
            & F.col("order_status").isin("CONFIRMED", "PROCESSING", "SHIPPED"),
        )
        .drop("order_date", "order_status")
    )
    return ok, quarantine


# ----------------------------------------------------------------------------- orchestration
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--data-dir", default="/opt/airflow/data", type=Path)
    args = parser.parse_args()

    salt = os.environ.get("HASH_SALT")
    if not salt:
        raise SystemExit("Variable d'environnement HASH_SALT absente.")

    spark = get_spark("curated")
    cur = args.data_dir / "curated"
    run_date = args.run_date
    t0 = time.time()

    def raw(name: str) -> DataFrame:
        return read_raw(spark, args.data_dir, name, run_date)

    def write(name: str, clean: DataFrame, keys: list[str], quarantine: DataFrame | None,
              qsource: str | None = None, qkey: str | None = None, partition_by: list[str] | None = None) -> None:
        clean = clean.cache()
        n_clean = clean.count()
        upsert(spark, clean, cur / name, keys, partition_by)
        n_q = 0
        if quarantine is not None:
            q = to_quarantine(quarantine, qsource or name, qkey or keys[0],
                              F.coalesce(*[F.col(c) for c in ("dq_reason", "dq_flag") if c in quarantine.columns]),
                              run_date).cache()
            n_q = q.count()
            qkeys = ["source", "record_key", "reason", "payload"]
            upsert(spark, q.dropDuplicates(qkeys), cur / "_quarantine", qkeys, ["source"])
            q.unpersist()
        clean.unpersist()
        log_event(log, "table curated", table=name, rows_clean=n_clean, rows_quarantine=n_q, run_date=run_date)

    try:
        products = build_products(raw("products"))
        write("products", products, ["product_id"], products.filter("dq_flag IS NOT NULL"),
              qsource="products", qkey="product_id")

        raw_customers = raw("customers")
        customers, cust_q = build_customers(raw_customers, salt)
        write("customers", customers, ["customer_id"], cust_q.withColumn("dq_reason", F.lit("AMBIGUOUS_CUSTOMER_ID")),
              qsource="customers", qkey="customer_id")

        raw_payments = raw("payments")
        selected = selected_payments(raw_payments)

        orders, orders_q = build_orders(
            raw("orders"), raw("order_lines"), raw("products"),
            ambiguous_customer_ids(raw_customers), selected,
        )
        orders = orders.cache()
        write("orders", orders, ["order_id"], orders_q, qsource="orders", qkey="order_id",
              partition_by=["order_year_month"])

        lines, lines_q = build_order_lines(raw("order_lines"), raw("products"), orders)
        write("order_lines", lines, ["order_line_id"], lines_q, qsource="order_lines",
              qkey="order_line_id", partition_by=["order_year_month"])

        write("payments", build_payments(raw_payments, selected), ["payment_id"], None)

        deliveries, deliveries_q = build_deliveries(raw("deliveries"), orders)
        write("deliveries", deliveries, ["delivery_id"], deliveries_q, qsource="deliveries",
              qkey="delivery_id")

        log_event(log, "curated done", run_date=run_date, duration_s=round(time.time() - t0, 1))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()