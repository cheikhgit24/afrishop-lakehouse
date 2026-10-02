"""Remet la table curated.deliveries a son etat d'origine apres demo_late_arrival.py.

Methode : on parcourt l'historique Delta pour retrouver la version creee par la demo
(une seule ligne modifiee, passee en DELIVERED / 9 jours / en retard), puis on re-injecte
UNIQUEMENT l'ancienne version de cette ligne par MERGE. Les autres lignes ne sont pas touchees.

Usage (dans le conteneur Airflow) :
    python /opt/airflow/scripts/reset_late_arrival.py
"""

from __future__ import annotations

import sys

sys.path.insert(0, "/opt/airflow/src")

from delta.tables import DeltaTable  # noqa: E402

from afrishop.spark_session import get_spark  # noqa: E402

PATH = "/opt/airflow/data/curated/deliveries"
MAX_VERSIONS_SCANNED = 15


def main() -> None:
    spark = get_spark("reset_late_arrival")
    spark.sparkContext.setLogLevel("ERROR")
    table = DeltaTable.forPath(spark, PATH)
    versions = sorted((r["version"] for r in table.history().select("version").collect()), reverse=True)

    def at(v: int):
        return spark.read.format("delta").option("versionAsOf", v).load(PATH)

    for v in versions[:MAX_VERSIONS_SCANNED]:
        if v == 0:
            break
        changed = at(v).exceptAll(at(v - 1))
        rows = changed.collect()
        if len(rows) != 1:
            continue
        r = rows[0]
        if r["delivery_status"] == "DELIVERED" and r["delivery_delay_days"] == 9.0 and r["is_late"]:
            did = r["delivery_id"]
            original = at(v - 1).filter(f"delivery_id = '{did}'")
            (table.alias("t").merge(original.alias("s"), "t.delivery_id <=> s.delivery_id")
             .whenMatchedUpdateAll().execute())
            back = spark.read.format("delta").load(PATH).filter(f"delivery_id = '{did}'").first()
            print(f"Livraison {did} restauree depuis la version {v - 1} : "
                  f"statut={back['delivery_status']}, retard={back['is_late']}")
            return
    print("Aucune modification issue de la demo trouvee : rien a restaurer.")


if __name__ == "__main__":
    main()
