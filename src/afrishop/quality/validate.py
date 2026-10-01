"""Validation de la qualite des donnees curated avec Great Expectations.

Usage (dans le conteneur ge) :
    python -m afrishop.quality.validate
    python -m afrishop.quality.validate --tables orders customers --fail-on-error

Produit :
  - des Data Docs HTML dans reports/ge/data_docs
  - un resume JSON dans reports/ge/summary.json
Code de sortie 1 si --fail-on-error et au moins une attente echoue.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd
from great_expectations.checkpoint import UpdateDataDocsAction
from sqlalchemy import create_engine

from afrishop.logging_utils import get_logger, log_event
from afrishop.quality.expectations import SUITES

logger = get_logger("afrishop.quality")


def engine_from_env():
    url = "postgresql+psycopg2://{u}:{p}@{h}:5432/{d}".format(
        u=os.environ["POSTGRES_USER"],
        p=os.environ["POSTGRES_PASSWORD"],
        h=os.environ.get("POSTGRES_HOST", "postgres"),
        d=os.environ["POSTGRES_DB"],
    )
    return create_engine(url)


def validate_table(context, name: str, df: pd.DataFrame) -> dict:
    ds = context.data_sources.add_or_update_pandas(f"src_{name}")
    asset = ds.add_dataframe_asset(name=name)
    batch_def = asset.add_batch_definition_whole_dataframe(f"{name}_all")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=f"{name}_suite"))
    for exp in SUITES[name]():
        suite.add_expectation(exp)
    suite.save()

    vdef = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"{name}_validation", data=batch_def, suite=suite)
    )
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=f"{name}_checkpoint",
            validation_definitions=[vdef],
            actions=[UpdateDataDocsAction(name="update_docs")],
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": df})
    vr = next(iter(result.run_results.values()))
    failed = [
        {
            "expectation": r.expectation_config.type,
            "column": r.expectation_config.kwargs.get("column"),
            "unexpected_count": r.result.get("unexpected_count"),
            "unexpected_percent": r.result.get("unexpected_percent"),
        }
        for r in vr.results
        if not r.success
    ]
    return {
        "table": name,
        "rows": len(df),
        "expectations": len(vr.results),
        "failed": len(failed),
        "success": vr.success,
        "failures": failed,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tables", nargs="+", default=list(SUITES), choices=list(SUITES))
    ap.add_argument("--output-dir", default="reports/ge", type=Path)
    ap.add_argument("--fail-on-error", action="store_true")
    args = ap.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    context = gx.get_context(mode="file", project_root_dir=str(args.output_dir))
    engine = engine_from_env()

    summary = []
    for name in args.tables:
        df = pd.read_sql(f"select * from curated.{name}", engine)
        res = validate_table(context, name, df)
        summary.append(res)
        log_event(logger, "ge_validation", **{k: v for k, v in res.items() if k != "failures"})

    context.build_data_docs()
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, default=str), encoding="utf-8"
    )
    ok = all(s["success"] for s in summary)
    for s in summary:
        print(f"{s['table']:<10} {'OK  ' if s['success'] else 'FAIL'} "
              f"{s['expectations'] - s['failed']}/{s['expectations']} attentes OK")
        for f in s["failures"]:
            print(f"    - {f['expectation']} ({f['column']}): {f['unexpected_count']} ecarts")
    return 0 if (ok or not args.fail_on_error) else 1


if __name__ == "__main__":
    sys.exit(main())
