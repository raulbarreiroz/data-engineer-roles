# Setup - Server Log Lakehouse (PySpark + Airflow)

## Layout

```
jobs/                 Spark batch jobs
  process_server_logs.py
dags/                 Airflow DAGs (drop into AIRFLOW_HOME/dags)
  lakehouse_logs_dag.py
sample_data/          Tiny log + dim files for local runs
conf/                 spark-defaults style notes
output/               Parquet bronze/silver writes land here locally
```

This is a sketch of how a data team structures a lakehouse job: bronze ingest, silver windowed joins, then Delta-like parquet output. Sample data is tiny on purpose so you can run without a cluster.

## Local run (no cluster)

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install pyspark pandas pyarrow

python jobs/process_server_logs.py
```

Expect parquet under `output/silver/server_sessions/` partitioned by `event_date`.

Spark will download a Hadoop binary the first time - that can take a minute.

## Airflow

The DAG file is self-contained. Point Airflow at this repo's `dags/` folder or copy the file:

```bash
export AIRFLOW_HOME=~/airflow
# symlink or copy dags/lakehouse_logs_dag.py into $AIRFLOW_HOME/dags
airflow dags list | grep lakehouse
```

Locally the PythonOperator just shells out to `process_server_logs.py`. On a real platform you'd swap that for an EMR/Databricks submit operator.

## Databricks / EMR (optional)

| Platform | Notes |
|---|---|
| **Databricks** | Upload `jobs/` as a repo or wheel; replace local paths with `dbfs:/` or Unity Catalog volumes; enable Delta (`format("delta")`) instead of plain parquet. |
| **EMR / EMR Serverless** | Package deps, submit with `spark-submit --deploy-mode cluster`; land output on `s3://bucket/lake/silver/...`. |
| **Delta** | Code comments mark where to switch `.format("delta")` - parquet keeps the demo dependency-light. |

## Tuning knobs

- `LOG_INPUT` / `DIM_INPUT` / `OUTPUT_ROOT` env vars override default paths.
- Window is 30 minutes looking back on `user_id` - adjust in the job if you want sliding.
