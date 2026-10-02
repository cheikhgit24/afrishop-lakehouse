"""Demonstration : arrivee tardive d'une livraison + voyage dans le temps Delta.

Scenario : une livraison deja connue passe a DELIVERED avec plusieurs jours de retard.
On l'applique par MERGE (comme le pipeline), puis on montre :
  1. l'historique Delta de la table (versions, operations) ;
  2. l'etat AVANT la correction (VERSION AS OF) et APRES ;
  3. un rejeu de la meme correction : aucune nouvelle ligne (idempotence).

Usage (dans le conteneur Airflow) :
    python /opt/airflow/scripts/demo_late_arrival.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, "/opt/airflow/src")

from delta.tables import DeltaTable  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from afrishop.spark_session import get_spark  # noqa: E402

PATH = str(Path("/opt/airflow/data/curated/deliveries"))
COLS = ["delivery_id", "order_id", "delivery_status", "delivered_at", "delivery_delay_days", "is_late"]


def show(title: str, df) -> None:
    print(f"\n=== {title} ===")
    df.select(*COLS).show(truncate=False)


def main() -> None:
    spark = get_spark("demo_late_arrival")
    spark.sparkContext.setLogLevel("ERROR")
    table = DeltaTable.forPath(spark, PATH)

    # Une livraison pas encore livree, choisie de facon deterministe.
    target = (
        spark.read.format("delta").load(PATH)
        .filter(F.col("delivery_status").isin("IN_TRANSIT", "OUT_FOR_DELIVERY"))
        .orderBy("delivery_id").limit(1)
    )
    if target.count() == 0:
        print("Aucune livraison en cours : relance d'abord le pipeline sur des donnees fraiches.")
        return
    row = target.first()
    did = row["delivery_id"]
    start_version = table.history(1).first()["version"]
    show("AVANT : livraison en cours", spark.read.format("delta").load(PATH).filter(F.col("delivery_id") == did))

    # Mise a jour tardive : livree 9 jours apres l'expedition.
    late = (
        spark.read.format("delta").load(PATH).filter(F.col("delivery_id") == did)
        .withColumn("delivery_status", F.lit("DELIVERED"))
        .withColumn("delivered_at", F.expr("coalesce(shipped_at, current_timestamp()) + interval 9 days"))
        .withColumn("delivery_delay_days", F.lit(9.0))
        .withColumn("is_late", F.lit(True))
    )
    for attempt in (1, 2):
        (table.alias("t").merge(late.alias("s"), "t.delivery_id <=> s.delivery_id")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        print(f"MERGE tardif applique (passage {attempt}) : lignes totales = "
              f"{spark.read.format('delta').load(PATH).count()}")

    show("APRES : livraison arrivee en retard", spark.read.format("delta").load(PATH).filter(F.col("delivery_id") == did))

    print("\n=== Historique Delta (versions) ===")
    table.history().select("version", "timestamp", "operation").show(truncate=False)

    old = spark.read.format("delta").option("versionAsOf", start_version).load(PATH)
    show(f"VOYAGE DANS LE TEMPS : version {start_version} (avant la correction)", old.filter(F.col("delivery_id") == did))


if __name__ == "__main__":
    main()
