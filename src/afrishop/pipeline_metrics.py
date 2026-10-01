"""Metriques d'execution du pipeline, stockees dans PostgreSQL (schema ops).

Une ligne par execution de DAG : statut, duree, taches en echec, volumes curated.
Sert de base au suivi (Power BI) et a l'alerting.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import Json

DDL = """
create schema if not exists ops;
create table if not exists ops.pipeline_runs (
    run_id          text primary key,
    dag_id          text not null,
    logical_date    timestamptz,
    state           text not null,
    started_at      timestamptz,
    ended_at        timestamptz,
    duration_s      double precision,
    failed_tasks    jsonb,
    orders_count    bigint,
    quarantine_count bigint,
    recorded_at     timestamptz not null default now()
);
"""

UPSERT = """
insert into ops.pipeline_runs
    (run_id, dag_id, logical_date, state, started_at, ended_at, duration_s,
     failed_tasks, orders_count, quarantine_count)
values (%(run_id)s, %(dag_id)s, %(logical_date)s, %(state)s, %(started_at)s, %(ended_at)s,
        %(duration_s)s, %(failed_tasks)s, %(orders_count)s, %(quarantine_count)s)
on conflict (run_id) do update set
    state = excluded.state, ended_at = excluded.ended_at, duration_s = excluded.duration_s,
    failed_tasks = excluded.failed_tasks, orders_count = excluded.orders_count,
    quarantine_count = excluded.quarantine_count, recorded_at = now()
"""


def connect(**overrides: Any):
    params = dict(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )
    params.update(overrides)
    return psycopg2.connect(**params)


def curated_counts(cur) -> tuple[int | None, int | None]:
    try:
        cur.execute("select count(*) from curated.orders")
        orders = cur.fetchone()[0]
        cur.execute("select count(*) from curated.quarantine")
        quarantine = cur.fetchone()[0]
        return orders, quarantine
    except psycopg2.Error:
        return None, None


def summarize_run(dag_run, task_instances) -> dict:
    """Construit la ligne de metriques a partir d'un DagRun et de ses TaskInstances."""
    failed = sorted(
        {ti.task_id for ti in task_instances if ti.state in ("failed", "upstream_failed")}
    )
    started = dag_run.start_date
    ended = datetime.now(started.tzinfo) if started else None
    return {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "logical_date": dag_run.logical_date,
        "state": "failed" if failed else "success",
        "started_at": started,
        "ended_at": ended,
        "duration_s": (ended - started).total_seconds() if started and ended else None,
        "failed_tasks": Json(failed),
    }


def record(row: dict, conn=None) -> dict:
    own = conn is None
    conn = conn or connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(DDL)
            orders, quarantine = curated_counts(cur)
            row = {**row, "orders_count": orders, "quarantine_count": quarantine}
            cur.execute(UPSERT, row)
        return row
    finally:
        if own:
            conn.close()
