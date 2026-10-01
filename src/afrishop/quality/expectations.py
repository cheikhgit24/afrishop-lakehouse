"""Suites d'attentes Great Expectations pour la zone curated.

Chaque suite est decrite par une liste de tuples (type d'attente, parametres).
`mostly` tolere une petite part d'ecarts quand la regle metier le permet.
"""

from __future__ import annotations

import great_expectations.expectations as gxe

ORDER_STATUSES = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
CHANNELS = ["WEB", "MOBILE_APP", "WHATSAPP_BOT", "CALL_CENTER", "AGENT_BIZ"]
PAYMENT_STATUSES = ["PENDING", "AUTHORIZED", "CAPTURED", "FAILED", "REFUNDED"]


def orders_suite() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToBeUnique(column="order_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
        gxe.ExpectColumnValuesToBeInSet(column="order_status", value_set=ORDER_STATUSES),
        gxe.ExpectColumnValuesToBeInSet(column="currency", value_set=["XOF", "GHS", "NGN"]),
        gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0),
        # Anomalie connue : ~0,04 % de sous-totaux negatifs (18 commandes), a signaler a la source.
        gxe.ExpectColumnValuesToBeBetween(column="subtotal_amount", min_value=0, mostly=0.999),
        gxe.ExpectColumnValuesToNotBeNull(column="order_date"),
        gxe.ExpectColumnValuesToBeInSet(column="channel", value_set=CHANNELS),
    ]


def customers_suite() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="customer_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="master_customer_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="email_hash", mostly=0.95),
        gxe.ExpectColumnValuesToMatchRegex(column="email_hash", regex=r"^[0-9a-f]{64}$"),
        gxe.ExpectColumnValuesToBeBetween(column="birth_year", min_value=1930, max_value=2012, mostly=0.99),
        gxe.ExpectColumnValuesToBeInSet(column="customer_segment", value_set=["NEW", "REGULAR", "VIP", "DORMANT"]),
    ]


def products_suite() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="product_id"),
        gxe.ExpectColumnValuesToBeUnique(column="product_id"),
        # Anomalie connue : 119 produits a prix 0 (flag ZERO_PRICE), conserves mais signales.
        gxe.ExpectColumnValuesToBeBetween(column="unit_price", min_value=0, strict_min=True, mostly=0.98),
        gxe.ExpectColumnValuesToBeBetween(column="unit_cost", min_value=0),
        gxe.ExpectColumnValuesToNotBeNull(column="category_name"),
    ]


def payments_suite() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="payment_id"),
        gxe.ExpectColumnValuesToBeUnique(column="payment_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="order_id"),
        gxe.ExpectColumnValuesToBeInSet(column="payment_status", value_set=PAYMENT_STATUSES),
        gxe.ExpectColumnValuesToBeBetween(column="payment_amount", min_value=0),
    ]


SUITES = {
    "orders": orders_suite,
    "customers": customers_suite,
    "products": products_suite,
    "payments": payments_suite,
}
