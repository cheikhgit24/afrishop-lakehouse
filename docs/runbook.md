# Runbook d'exploitation

## Démarrer et arrêter

```powershell
docker compose up -d      # démarrer
docker compose ps         # état des conteneurs
docker compose down       # arrêter (les données PostgreSQL sont conservées)
```

`docker compose down -v` supprime aussi le volume PostgreSQL : à n'utiliser que pour repartir de zéro.

## Exécuter le pipeline

Interface Airflow (http://localhost:8081) : activer le DAG `afrishop_pipeline`, puis « Trigger DAG ».
En ligne de commande : `docker compose exec airflow-scheduler airflow dags trigger afrishop_pipeline`.

Rejouer une période (idempotent) :

```powershell
docker compose exec airflow-scheduler airflow dags backfill afrishop_pipeline --start-date 2026-09-27 --end-date 2026-09-28
```

Contrôle des volumes après exécution :

```powershell
docker compose exec airflow-scheduler python /opt/airflow/scripts/check_curated.py
```

## Suivre les exécutions

Table `ops.pipeline_runs` (statut, durée, tâches en échec, volumes). Journaux JSON dans l'interface
Airflow : onglet Logs de chaque tâche.

## Incidents fréquents

| Symptôme | Cause probable | Action |
|---|---|---|
| `check_sources` en échec | fichier CSV absent de `data/source` | déposer les 6 fichiers, relancer la tâche |
| `build_curated` : `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` | doublons dans la source du MERGE | corrigé en 1.0.0 ; sinon dédupliquer avant le MERGE |
| `ge_validate` : `PermissionError` sur `reports/ge` | dossier créé par un autre utilisateur | `docker compose exec -u root airflow-scheduler chmod -R a+rwX /opt/airflow/reports` |
| `ge_validate` en échec (attentes non respectées) | données hors règles | consulter les Data Docs (`reports/ge/gx/uncommitted/data_docs/local_site/index.html`), traiter la source |
| `dbt_test` en échec | règle métier violée | `docker compose run --rm dbt test` pour le détail ; corriger la donnée ou la règle |
| Airflow inaccessible sur le port | port déjà utilisé (ex. autre conteneur) | changer le port publié dans `docker-compose.yml` |
| Délai de téléchargement pip pendant un build | réseau instable | relancer `docker compose build`, ou ajouter `--default-timeout=300` |

## Reprise après échec

Dans l'interface, sur la tâche en échec : « Clear task » (avec « Downstream ») relance la tâche et
celles qui suivent. Les étapes sont idempotentes : aucune purge manuelle n'est nécessaire.

## Qualité du code et du SQL

```powershell
docker compose run --rm tests ruff check src tests dags
docker compose run --rm tests black --check src tests dags
docker compose run --rm tests mypy
docker compose run --rm --entrypoint sh dbt -c "pip install -q sqlfluff && sqlfluff lint models tests macros"
```

Les snapshots (balises Jinja `{% snapshot %}`) sont exclus de sqlfluff.

## Démonstration d'arrivée tardive

```powershell
docker compose exec airflow-scheduler python /opt/airflow/scripts/demo_late_arrival.py
docker compose exec airflow-scheduler python /opt/airflow/scripts/reset_late_arrival.py   # remise à l'état d'origine
```

## Sauvegarde

Les données vivent dans le dossier `data/` (raw, curated) et le volume Docker `pgdata`.
Sauvegarde PostgreSQL : `docker compose exec postgres pg_dump -U <utilisateur> <base> > sauvegarde.sql`.
