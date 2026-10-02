"""Tests des transformations de la zone curated (Spark local, donnees minuscules)."""

import pytest
from pyspark.sql import functions as F

from afrishop import curated

pytestmark = pytest.mark.spark
TS = "2026-01-01 00:00:00"


def df(spark, rows):
    """Construit un DataFrame 100 % texte (comme la zone raw) avec _ingested_at."""
    rows = [{**r, "_ingested_at": TS} for r in rows]
    return spark.createDataFrame(rows)


def test_latest_by_keeps_most_recent_version(spark):
    raw = df(
        spark,
        [
            {"k": "1", "v": "old", "updated_at": "2026-01-01"},
            {"k": "1", "v": "new", "updated_at": "2026-02-01"},
            {"k": "2", "v": "only", "updated_at": "2026-01-01"},
        ],
    )
    out = {r["k"]: r["v"] for r in curated.latest_by(raw, "k", "updated_at").collect()}
    assert out == {"1": "new", "2": "only"}


def test_build_products_flags_zero_price_and_cost_above_price(spark):
    base = {
        "sku": "s",
        "product_name": "n",
        "category_id": "c",
        "category_name": "cat",
        "subcategory_name": "sub",
        "brand": "b",
        "seller_id": "s1",
        "weight_grams": "10",
        "is_perishable": "false",
        "created_at": TS,
        "updated_at": TS,
        "product_status": "ACTIVE",
    }
    raw = df(
        spark,
        [
            {**base, "product_id": "P1", "unit_cost": "5", "unit_price": "10"},
            {**base, "product_id": "P2", "unit_cost": "0", "unit_price": "0"},
            {**base, "product_id": "P3", "unit_cost": "20", "unit_price": "10"},
        ],
    )
    flags = {r["product_id"]: r["dq_flag"] for r in curated.build_products(raw).collect()}
    assert flags == {"P1": None, "P2": "ZERO_PRICE", "P3": "COST_GT_PRICE"}


def test_build_customers_hashes_pii_groups_duplicates_and_quarantines_ambiguous_ids(spark):
    base = {
        "birth_year": "1990",
        "gender": "F",
        "registration_date": "2025-01-01",
        "country_code": "SN",
        "city": "Dakar",
        "customer_segment": "NEW",
        "preferred_payment_method": "WAVE",
        "loyalty_tier": "BRONZE",
        "account_status": "ACTIVE",
    }
    raw = df(
        spark,
        [
            {**base, "customer_id": "C1", "email_hash": "e1", "phone_hash": "p1"},
            {**base, "customer_id": "C2", "email_hash": "e1", "phone_hash": "p2"},  # meme e-mail que C1
            {**base, "customer_id": "C3", "email_hash": "e3", "phone_hash": "p3"},
            {**base, "customer_id": "C9", "email_hash": "e8", "phone_hash": "p8"},  # id ambigu
            {**base, "customer_id": "C9", "email_hash": "e9", "phone_hash": "p9"},
        ],
    )
    clean, quarantine = curated.build_customers(raw, salt="pepper")
    rows = {r["customer_id"]: r for r in clean.collect()}
    assert set(rows) == {"C1", "C2", "C3"}
    assert rows["C1"]["master_customer_id"] == rows["C2"]["master_customer_id"] == "C1"
    assert rows["C2"]["is_duplicate_account"] is True and rows["C1"]["is_duplicate_account"] is False
    assert len(rows["C1"]["email_hash"]) == 64 and rows["C1"]["email_hash"] != "e1"
    assert quarantine.count() == 2


def test_salt_changes_the_hash(spark):
    base = {
        "birth_year": "1990",
        "gender": "F",
        "registration_date": "2025-01-01",
        "country_code": "SN",
        "city": "x",
        "customer_segment": "NEW",
        "preferred_payment_method": "WAVE",
        "loyalty_tier": "B",
        "account_status": "A",
        "customer_id": "C1",
        "email_hash": "e",
        "phone_hash": "p",
    }
    a = curated.build_customers(df(spark, [base]), "s1")[0].first()["email_hash"]
    b = curated.build_customers(df(spark, [base]), "s2")[0].first()["email_hash"]
    assert a != b


def test_selected_payments_keeps_one_captured_payment_per_order(spark):
    raw = df(
        spark,
        [
            {
                "payment_id": "A",
                "order_id": "O1",
                "payment_status": "CAPTURED",
                "payment_date": "2026-01-01 10:00:00",
                "payment_amount": "50",
            },
            {
                "payment_id": "B",
                "order_id": "O1",
                "payment_status": "CAPTURED",
                "payment_date": "2026-01-02 10:00:00",
                "payment_amount": "60",
            },
            {
                "payment_id": "C",
                "order_id": "O1",
                "payment_status": "FAILED",
                "payment_date": "2026-01-03 10:00:00",
                "payment_amount": "60",
            },
        ],
    )
    rows = curated.selected_payments(raw).collect()
    assert len(rows) == 1 and rows[0]["payment_id"] == "B"


def _order(oid, subtotal, shipping, discount, total):
    return {
        "order_id": oid,
        "order_number": oid,
        "customer_id": "C1",
        "order_date": "2026-01-10 08:00:00",
        "order_status": "DELIVERED",
        "channel": "WEB",
        "country_code": "SN",
        "delivery_address_city": "Dakar",
        "delivery_address_region": "r",
        "delivery_address_zone": "URBAN",
        "promo_code": "",
        "currency": "XOF",
        "subtotal_amount": subtotal,
        "shipping_amount": shipping,
        "discount_amount": discount,
        "total_amount": total,
        "created_at": TS,
        "updated_at": TS,
    }


def test_build_orders_quarantines_bad_totals_and_orders_without_lines(spark):
    orders = df(
        spark,
        [
            _order("OK", "100", "10", "5", "105"),
            _order("BAD", "100", "10", "5", "999"),
            _order("NOLINE", "100", "10", "5", "105"),
            _order("PARTIAL", "100", "0", "0", "100"),
        ],
    )
    lines = df(
        spark, [{"order_id": o, "product_id": "P1", "line_total": "100"} for o in ("OK", "BAD", "PARTIAL")]
    )
    products = df(spark, [{"product_id": "P1"}])
    payments = df(
        spark,
        [
            {
                "payment_id": "A",
                "order_id": "PARTIAL",
                "payment_status": "CAPTURED",
                "payment_date": TS,
                "payment_amount": "40",
            },
            {
                "payment_id": "B",
                "order_id": "OK",
                "payment_status": "CAPTURED",
                "payment_date": TS,
                "payment_amount": "105",
            },
        ],
    )
    selected = curated.selected_payments(payments)
    ambiguous = spark.createDataFrame([("C1",)], ["customer_id"])
    clean, quarantine = curated.build_orders(orders, lines, products, ambiguous, selected)

    reasons = {r["order_id"]: r["dq_reason"] for r in quarantine.collect()}
    assert reasons == {"BAD": "BAD_TOTAL", "NOLINE": "NO_LINES"}
    rows = {r["order_id"]: r for r in clean.collect()}
    assert set(rows) == {"OK", "PARTIAL"}
    assert rows["PARTIAL"]["is_partial_payment"] is True and rows["OK"]["is_partial_payment"] is False
    assert rows["OK"]["has_payment"] is True
    assert rows["OK"]["customer_dq_flag"] == "AMBIGUOUS_CUSTOMER_ID"
    assert rows["OK"]["order_year_month"] == "2026-01"


def test_build_order_lines_quarantines_orphan_products_and_unretained_orders(spark):
    lines = df(
        spark,
        [
            {
                "order_line_id": "L1",
                "order_id": "O1",
                "product_id": "P1",
                "product_sku": "s",
                "quantity": "1",
                "unit_price": "10",
                "line_discount": "0",
                "line_total": "10",
                "seller_id": "S",
            },
            {
                "order_line_id": "L2",
                "order_id": "O1",
                "product_id": "PX",
                "product_sku": "s",
                "quantity": "1",
                "unit_price": "10",
                "line_discount": "0",
                "line_total": "10",
                "seller_id": "S",
            },
            {
                "order_line_id": "L3",
                "order_id": "O9",
                "product_id": "P1",
                "product_sku": "s",
                "quantity": "1",
                "unit_price": "10",
                "line_discount": "0",
                "line_total": "10",
                "seller_id": "S",
            },
        ],
    )
    products = df(spark, [{"product_id": "P1"}])
    orders_clean = spark.createDataFrame([("O1", "2026-01")], ["order_id", "order_year_month"])
    clean, quarantine = curated.build_order_lines(lines, products, orders_clean)
    assert [r["order_line_id"] for r in clean.collect()] == ["L1"]
    reasons = {r["order_line_id"]: r["dq_reason"] for r in quarantine.collect()}
    assert reasons == {"L2": "ORPHAN_PRODUCT", "L3": "ORDER_NOT_IN_CLEAN"}


def test_build_deliveries_computes_delay_late_flag_and_status_conflict(spark):
    raw = df(
        spark,
        [
            {
                "delivery_id": "D1",
                "order_id": "O1",
                "carrier": "c",
                "tracking_number": "t",
                "delivery_status": "DELIVERED",
                "shipped_at": "2026-01-11 00:00:00",
                "delivered_at": "2026-01-20 00:00:00",
                "delivery_attempts": "1",
                "delivery_cost": "5",
            },
            {
                "delivery_id": "D2",
                "order_id": "O2",
                "carrier": "c",
                "tracking_number": "t",
                "delivery_status": "DELIVERED",
                "shipped_at": "2026-01-11 00:00:00",
                "delivered_at": "2026-01-12 00:00:00",
                "delivery_attempts": "1",
                "delivery_cost": "5",
            },
            {
                "delivery_id": "D3",
                "order_id": "ZZ",
                "carrier": "c",
                "tracking_number": "t",
                "delivery_status": "DELIVERED",
                "shipped_at": "2026-01-11 00:00:00",
                "delivered_at": "2026-01-12 00:00:00",
                "delivery_attempts": "1",
                "delivery_cost": "5",
            },
        ],
    )
    orders = spark.createDataFrame(
        [("O1", "2026-01-10 00:00:00", "DELIVERED"), ("O2", "2026-01-10 00:00:00", "SHIPPED")],
        ["order_id", "order_date", "order_status"],
    ).withColumn("order_date", F.to_timestamp("order_date"))
    ok, quarantine = curated.build_deliveries(raw, orders)
    rows = {r["delivery_id"]: r for r in ok.collect()}
    assert rows["D1"]["delivery_delay_days"] == 10 and rows["D1"]["is_late"] is True
    assert rows["D2"]["is_late"] is False and rows["D2"]["status_conflict"] is True
    assert [r["delivery_id"] for r in quarantine.collect()] == ["D3"]


def test_upsert_is_idempotent_and_updates_by_key(spark, tmp_path):
    path = tmp_path / "t"
    first = spark.createDataFrame([("1", "a"), ("2", "b")], ["id", "v"])
    curated.upsert(spark, first, path, ["id"])
    curated.upsert(spark, first, path, ["id"])  # rejeu
    assert spark.read.format("delta").load(str(path)).count() == 2
    curated.upsert(spark, spark.createDataFrame([("2", "B"), ("3", "c")], ["id", "v"]), path, ["id"])
    out = {r["id"]: r["v"] for r in spark.read.format("delta").load(str(path)).collect()}
    assert out == {"1": "a", "2": "B", "3": "c"}
