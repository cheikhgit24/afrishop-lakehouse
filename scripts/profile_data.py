"""Profilage des 6 CSV AfriShop (phase 2).

Usage (depuis la racine du projet) :
    python scripts/profile_data.py
    python scripts/profile_data.py --source data/source --output docs/profiling_report.md

Produit un rapport Markdown reproductible : volumes, valeurs manquantes et
catalogue des anomalies avec leur traitement prevu.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

TOL = 0.05  # tolerance (devise) pour les comparaisons de montants
FILES = ["orders", "order_lines", "customers", "products", "payments", "deliveries"]


def load(source: Path) -> dict[str, pd.DataFrame]:
    missing = [f for f in FILES if not (source / f"{f}.csv").exists()]
    if missing:
        raise SystemExit(f"Fichiers introuvables dans {source} : {missing}")
    return {f: pd.read_csv(source / f"{f}.csv", low_memory=False) for f in FILES}


def pct(n: int, total: int) -> str:
    return f"{n / total * 100:.2f} %" if total else "n/a"


def to_md(df: pd.DataFrame) -> str:
    lines = ["| " + " | ".join(map(str, df.columns)) + " |",
             "| " + " | ".join("---" for _ in df.columns) + " |"]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join(str(v) for v in row) + " |")
    return "\n".join(lines)


def overview(d: dict[str, pd.DataFrame]) -> str:
    rows = []
    for name, df in d.items():
        nulls = df.isna().sum()
        nulls = ", ".join(f"{c}={n}" for c, n in nulls[nulls > 0].items()) or "aucune"
        rows.append({"fichier": name, "lignes": len(df), "colonnes": df.shape[1],
                     "valeurs manquantes": nulls})
    return to_md(pd.DataFrame(rows))


def anomalies(d: dict[str, pd.DataFrame]) -> list[dict]:
    o, ol, c = d["orders"], d["order_lines"], d["customers"]
    p, pay, dl = d["products"], d["payments"], d["deliveries"]
    out: list[dict] = []

    def add(name: str, count: int, base: int, rule: str) -> None:
        out.append({"anomalie": name, "nombre": count, "part": pct(count, base),
                    "traitement prevu": rule})

    # --- orders
    od = o.drop_duplicates("order_id").copy()
    add("orders : order_id en doublon", int(o.order_id.duplicated().sum()), len(o),
        "Garder la version la plus recente (updated_at)")

    lines_sum = ol.groupby("order_id").line_total.sum().rename("lines_sum")
    od = od.merge(lines_sum, on="order_id", how="left")
    od["calc"] = od.subtotal_amount + od.shipping_amount - od.discount_amount
    add("orders : total_amount != sous-total + livraison - remise",
        int((abs(od.total_amount - od.calc) > TOL).sum()), len(od),
        "Quarantaine")
    add("orders : aucune ligne dans order_lines", int(od.lines_sum.isna().sum()), len(od),
        "Quarantaine (recalcul impossible)")
    has_lines = od.dropna(subset=["lines_sum"])
    add("orders : sous-total != somme des lignes",
        int((abs(has_lines.subtotal_amount - has_lines.lines_sum) > TOL).sum()),
        len(has_lines), "Signaler, recalculer depuis les lignes")
    add("orders : client inconnu", int((~od.customer_id.isin(c.customer_id)).sum()), len(od),
        "Quarantaine")

    # --- order_lines
    add("order_lines : product_id inexistant", int((~ol.product_id.isin(p.product_id)).sum()),
        len(ol), "Quarantaine")
    calc_line = ol.quantity * ol.unit_price - ol.line_discount
    add("order_lines : line_total != quantite x prix - remise",
        int((abs(calc_line - ol.line_total) > TOL).sum()), len(ol), "Recalculer / signaler")
    add("order_lines : order_id inexistant", int((~ol.order_id.isin(o.order_id)).sum()),
        len(ol), "Quarantaine")

    # --- customers
    add("customers : email_hash en doublon", int(c.email_hash.duplicated().sum()), len(c),
        "Client maitre (MDM) : regrouper les comptes")
    add("customers : phone_hash en doublon", int(c.phone_hash.duplicated().sum()), len(c),
        "Client maitre (MDM)")
    add("customers : customer_id en doublon (personnes differentes)",
        int(c.customer_id.duplicated().sum()), len(c),
        "Cle de substitution + quarantaine a arbitrer")

    # --- products
    add("products : unit_price = 0", int((p.unit_price == 0).sum()), len(p), "Quarantaine")
    add("products : unit_cost > unit_price (prix > 0)",
        int(((p.unit_cost > p.unit_price) & (p.unit_price > 0)).sum()), len(p), "Quarantaine")

    # --- payments
    n_pay = pay.groupby("order_id").size()
    add("payments : commandes avec plusieurs paiements", int((n_pay > 1).sum()), len(od),
        "Retenir le paiement CAPTURED, ignorer les FAILED")
    cap = pay[pay.payment_status == "CAPTURED"]
    n_cap = cap.groupby("order_id").size()
    add("payments : commandes avec plusieurs CAPTURED", int((n_cap > 1).sum()), len(n_cap),
        "Dedoublonner : un seul paiement capture par commande")
    cap_sum = cap.groupby("order_id").payment_amount.sum().rename("cap")
    m = od.merge(cap_sum, on="order_id")
    add("payments : paiement partiel (capture < total)",
        int((m.cap < m.total_amount - TOL).sum()), len(m),
        "Flag paiement_partiel, conserver")
    add("payments : transaction_reference en doublon",
        int(pay.transaction_reference.duplicated().sum()), len(pay), "Signaler")
    no_pay = od[~od.order_id.isin(pay.order_id)]
    add("orders : DELIVERED sans aucun paiement",
        int((no_pay.order_status == "DELIVERED").sum()), len(od), "Signaler")

    # --- deliveries
    dd = dl.merge(od[["order_id", "order_date", "order_status"]], on="order_id")
    order_ts = pd.to_datetime(dd.order_date, utc=True).dt.tz_localize(None)
    lag = (pd.to_datetime(dd.delivered_at) - order_ts).dt.days
    add("deliveries : livraison >= 7 jours apres la commande", int((lag >= 7).sum()),
        int(lag.notna().sum()), "Arrivee tardive : reconcilier par order_id")
    bad = dd[(dd.delivery_status == "DELIVERED") &
             dd.order_status.isin(["CONFIRMED", "PROCESSING", "SHIPPED"])]
    add("deliveries : DELIVERED alors que la commande n'est pas DELIVERED",
        len(bad), len(dd), "Statut de livraison fait foi, signaler")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default="data/source", type=Path)
    ap.add_argument("--output", default="docs/profiling_report.md", type=Path)
    args = ap.parse_args()

    d = load(args.source)
    table = to_md(pd.DataFrame(anomalies(d)))
    o = d["orders"]
    report = "\n".join([
        "# Rapport de profilage (phase 2)",
        "",
        "## Vue d'ensemble",
        "",
        overview(d),
        "",
        "## Perimetre",
        "",
        f"Commandes du {o.order_date.min()[:10]} au {o.order_date.max()[:10]}, "
        f"pays : {', '.join(sorted(o.country_code.unique()))}.",
        "",
        "## Catalogue des anomalies",
        "",
        table,
        "",
    ])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nRapport ecrit dans {args.output}")


if __name__ == "__main__":
    main()
