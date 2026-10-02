from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from afrishop import pipeline_metrics


def _run(started):
    return SimpleNamespace(run_id="r1", dag_id="d", logical_date=started, start_date=started)


def _ti(task_id, state):
    return SimpleNamespace(task_id=task_id, state=state)


def test_summarize_run_success():
    start = datetime.now(UTC) - timedelta(minutes=5)
    row = pipeline_metrics.summarize_run(_run(start), [_ti("a", "success"), _ti("b", "success")])
    assert row["state"] == "success"
    assert row["failed_tasks"].adapted == []
    assert row["duration_s"] >= 290


def test_summarize_run_failed_lists_failed_and_upstream_failed_tasks():
    start = datetime.now(UTC)
    tis = [_ti("a", "success"), _ti("b", "failed"), _ti("c", "upstream_failed"), _ti("b", "failed")]
    row = pipeline_metrics.summarize_run(_run(start), tis)
    assert row["state"] == "failed"
    assert row["failed_tasks"].adapted == ["b", "c"]


def test_record_executes_ddl_then_upsert_with_counts():
    calls = []

    class Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params=None):
            calls.append((sql.strip().split()[0].lower(), params))
            self._last = sql

        def fetchone(self):
            return (42,)

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return Cur()

        def close(self):
            pass

    row = pipeline_metrics.record({"run_id": "r1", "dag_id": "d", "state": "success"}, conn=Conn())
    assert row["orders_count"] == 42 and row["quarantine_count"] == 42
    verbs = [c[0] for c in calls]
    assert verbs[0] == "create" and "insert" in verbs
