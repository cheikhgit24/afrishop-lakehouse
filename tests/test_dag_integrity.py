"""Verifie que le DAG se charge sans erreur et respecte les regles de resilience."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_pipeline_structure(dagbag):
    dag = dagbag.get_dag("afrishop_pipeline")
    assert dag is not None
    expected = {
        "check_sources",
        "ingest_raw",
        "build_curated",
        "load_postgres",
        "ge_validate",
        "dbt_seed",
        "dbt_snapshot",
        "dbt_run",
        "dbt_test",
        "record_metrics",
    }
    assert set(dag.task_ids) == expected
    assert dag.max_active_runs == 1 and dag.catchup is False


def test_retries_and_failure_callback_configured(dagbag):
    dag = dagbag.get_dag("afrishop_pipeline")
    for task in dag.tasks:
        if task.task_id != "record_metrics":
            assert task.retries == 2
        assert task.on_failure_callback is not None


def test_record_metrics_runs_even_if_upstream_fails(dagbag):
    dag = dagbag.get_dag("afrishop_pipeline")
    assert dag.get_task("record_metrics").trigger_rule == "all_done"
    assert dag.get_task("dbt_test").downstream_task_ids == {"record_metrics"}
