# Setup — Daily Sales ETL

## What this does

Pulls sales rows from a local CSV (or optional public API), cleans nulls/dupes with Pandas, then loads into PostgreSQL. If Postgres isn't available it falls back to SQLite so you can run everything on a laptop.

## Quick start (local)

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt

python scripts/run_etl.py
```

After a successful run you should see row counts in the terminal and a `data/local_warehouse.db` SQLite file (default path).

## Environment

| Variable | Default | Notes |
|---|---|---|
| `DB_URL` | `sqlite:///data/local_warehouse.db` | Use `postgresql+psycopg2://user:pass@localhost:5432/sales` for Postgres |
| `SALES_CSV` | `data/sample_sales.csv` | Path to the input file |
| `USE_API` | `0` | Set to `1` to also fetch a small public JSON sample |
| `TABLE_NAME` | `daily_sales` | Destination table |

## Postgres (optional)

```bash
createdb sales
export DB_URL="postgresql+psycopg2://postgres:postgres@localhost:5432/sales"
python scripts/run_etl.py
```

## Cron sketch

```cron
# run every day at 06:15
15 6 * * * cd /opt/sales-etl && .venv/bin/python scripts/run_etl.py >> /var/log/sales-etl.log 2>&1
```

## Notes

- The sample CSV has intentional nulls and duplicate order ids so the clean step is visible.
- `replace` is used for the local demo; in prod you'd usually `append` with an idempotency key.
