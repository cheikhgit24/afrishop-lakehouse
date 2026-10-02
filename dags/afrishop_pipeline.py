"""DAG principal AfriShop : raw -> curated -> PostgreSQL -> qualite -> dbt.

Ordre des taches
    check_sources
      -> ingest_raw (6 taches dynamiques, une par source, 2 en parallele maximum)
      -> build_curated -> load_postgres -> ge_validate
      -> dbt_seed -> dbt_snapshot -> dbt_run -> dbt_test
      -> record_metrics (toujours execute, ecrit ops.pipeline_runs)

Parametres de resilience : 2 reprises avec delai exponentiel, SLA par tache,
callbacks d'echec en JSON, une seule execution a la fois.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/src")

from afrishop.logging_utils import get_logger, log_event  # noqa: E402

logger = get_logger("afrishop.dag")

SOURCES = ["orders", "order_lines", "customers", "products", "payments", "deliveries"]
DATA_DIR = Path("/opt/airflow/data")
SRC = "cd /opt/airflow && export PYTHONPATH=/opt/airflow/src"
DBT = (
    "cd /opt/airflow/dbt && export DBT_PROFILES_DIR=/opt/airflow/dbt && "
    "/opt/airflow/venvs/dbt/bin/dbt {cmd} --target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
)


def on_failure(context) -> None:
    """Alerte d'echec : journal JSON de niveau ERROR (branchable sur Slack/mail)."""
    ti = context["task_instance"]
    logger.error(
        "task_failed",
        extra={
            "extra_fields": {
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": context["run_id"],
                "try_number": ti.try_number,
                "error": str(context.get("exception")),
            }
        },
    )


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    logger.warning("sla_missed", extra={"extra_fields": {"dag_id": dag.dag_id, "tasks": str(task_list)}})


default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure,
}


@dag(
    dag_id="afrishop_pipeline",
    description="Pipeline e-commerce AfriShop : raw, curated, qualite, analytics.",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=on_sla_miss,
    tags=["afrishop", "lakehouse"],
)
def afrishop_pipeline():
    @task(task_id="check_sources")
    def check_sources() -> list[str]:
        missing = [s for s in SOURCES if not (DATA_DIR / "source" / f"{s}.csv").exists()]
        if missing:
            raise FileNotFoundError(f"Fichiers source manquants : {missing}")
        return SOURCES

    ingest_raw = BashOperator.partial(
        task_id="ingest_raw",
        max_active_tis_per_dag=2,  # limite la memoire : 1 JVM Spark par source
    ).expand(
        bash_command=[
            f"{SRC} && python -m afrishop.ingest_raw --run-date {{{{ ds }}}} --sources {s}" for s in SOURCES
        ]
    )

    build_curated = BashOperator(
        task_id="build_curated",
        bash_command=f"{SRC} && python -m afrishop.curated --run-date {{{{ ds }}}}",
        sla=timedelta(minutes=30),
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"{SRC} && python -m afrishop.load_postgres",
        sla=timedelta(minutes=30),
    )

    ge_validate = BashOperator(
        task_id="ge_validate",
        bash_command=(
            f"{SRC} && /opt/airflow/venvs/ge/bin/python -m afrishop.quality.validate "
            "--output-dir /opt/airflow/reports/ge --fail-on-error"
        ),
    )

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=DBT.format(cmd="seed"))
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=DBT.format(cmd="snapshot"))
    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT.format(cmd="run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT.format(cmd="test"))

    @task(task_id="record_metrics", trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def record_metrics(**context) -> dict:
        from afrishop import pipeline_metrics

        dag_run = context["dag_run"]
        others = [ti for ti in dag_run.get_task_instances() if ti.task_id != "record_metrics"]
        row = pipeline_metrics.summarize_run(dag_run, others)
        row = pipeline_metrics.record(row)
        log_event(
            logger,
            "pipeline_run_recorded",
            run_id=row["run_id"],
            state=row["state"],
            duration_s=row["duration_s"],
            orders=row["orders_count"],
            quarantine=row["quarantine_count"],
        )
        if row["state"] == "failed":
            raise RuntimeError(f"Pipeline en echec : {row['failed_tasks'].adapted}")
        return {"state": row["state"]}

    (
        check_sources()
        >> ingest_raw
        >> build_curated
        >> load_postgres
        >> ge_validate
        >> dbt_seed
        >> dbt_snapshot
        >> dbt_run
        >> dbt_test
        >> record_metrics()
    )


afrishop_pipeline()
