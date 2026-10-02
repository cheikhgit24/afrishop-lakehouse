# Journal des changements

Format inspiré de Keep a Changelog ; versionnement sémantique.

## [1.0.0] - 2026-10

### Ajouté
- Infrastructure Docker Compose : PostgreSQL 15, Airflow 2.9.3 (LocalExecutor), conteneurs dbt,
  Great Expectations et tests.
- Profilage des 6 sources et catalogue des anomalies (`docs/profiling_report.md`).
- Zone raw : ingestion Parquet partitionnée, archivage, contrôle des volumes.
- Zone curated : Delta Lake, MERGE idempotent, quarantaine motivée, hachage salé, client maître.
- Chargement du curated dans PostgreSQL (schéma `curated`).
- dbt : 8 modèles de staging, 4 modèles intermédiaires, schéma en étoile (5 dimensions, 1 table
  de faits), 3 marts métier (RFM, rotation produits, taux de retour incrémental), 2 snapshots SCD2,
  65 tests.
- Great Expectations : 4 suites, rapports HTML (Data Docs).
- DAG Airflow `afrishop_pipeline` : mapping dynamique, reprises, SLA, callbacks, métriques.
- Tests pytest (couverture supérieure à 60 %), black, ruff, mypy, pre-commit, sqlfluff.
- Démonstration d'arrivée tardive et de voyage dans le temps Delta.

### Corrigé
- Quarantaine : doublons de clé lors du MERGE (clé étendue au contenu et déduplication).
- Great Expectations : droits d'écriture sur `reports/` pour l'utilisateur Airflow.
