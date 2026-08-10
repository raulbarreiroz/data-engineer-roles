"""Airflow DAG - orchestrate the server-log lakehouse job.

Drop this file into AIRFLOW_HOME/dags (or add this repo's dags/ to DAGS_FOLDER).
On EMR/Databricks you'd replace the PythonOperator body with a submit operator.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    # Airflow 2.x classic style - still the most common in existing data teams
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - allows reading the file without Airflow installed
    DAG = None
    PythonOperator = None


REPO = Path(__file__).resolve().parents[1]
JOB = REPO / "jobs" / "process_server_logs.py"


def _run_spark_job(**_context):
    env = os.environ.copy()
    env.setdefault("OUTPUT_ROOT", str(REPO / "output"))
    proc = subprocess.run(
        [sys.executable, str(JOB)],
        cwd=str(REPO),
        env=env,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"spark job exited {proc.returncode}")


def _sanity_check(**_context):
    out = REPO / "output" / "silver" / "server_sessions"
    if not out.exists():
        raise FileNotFoundError(f"missing silver output: {out}")
    files = list(out.rglob("*.parquet"))
    if not files:
        raise RuntimeError("no parquet files under silver output")
    print(f"ok - found {len(files)} parquet file(s)")


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

if DAG is not None:
    with DAG(
        dag_id="lakehouse_server_logs",
        default_args=default_args,
        description="Bronze to silver server logs with window joins",
        schedule="0 7 * * *",
        start_date=datetime(2025, 10, 1),
        catchup=False,
        tags=["lakehouse", "spark", "logs"],
    ) as dag:
        run_job = PythonOperator(
            task_id="process_server_logs",
            python_callable=_run_spark_job,
        )
        check = PythonOperator(
            task_id="validate_silver_output",
            python_callable=_sanity_check,
        )
        run_job >> check
else:
    dag = None  # type: ignore
