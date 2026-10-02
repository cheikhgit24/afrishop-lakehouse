import pytest

pytest.importorskip("great_expectations")

from afrishop.quality.expectations import SUITES  # noqa: E402


def test_each_suite_defines_expectations_on_its_key(name):
    expectations = SUITES[name]()
    assert len(expectations) >= 5
    columns = {e.column for e in expectations}
    assert any(c.endswith("_id") for c in columns)


def test_orders_suite_checks_business_critical_columns():
    columns = {e.column for e in SUITES["orders"]()}
    assert {"order_id", "order_status", "currency", "total_amount"} <= columns


def test_customers_suite_checks_hash_format_for_privacy():
    patterns = [getattr(e, "regex", None) for e in SUITES["customers"]()]
    assert r"^[0-9a-f]{64}$" in patterns or any(p and "64" in p for p in patterns)
