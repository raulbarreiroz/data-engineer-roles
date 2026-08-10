"""Runtime knobs for the daily sales ETL.

Kept separate so ops can tweak paths/URLs without digging through the pipeline.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# default to sqlite so the demo works without Docker/Postgres
DB_URL = os.getenv("DB_URL", f"sqlite:///{REPO_ROOT / 'data' / 'local_warehouse.db'}")
SALES_CSV = Path(os.getenv("SALES_CSV", REPO_ROOT / "data" / "sample_sales.csv"))
USE_API = os.getenv("USE_API", "0").strip() in {"1", "true", "True", "yes"}
TABLE_NAME = os.getenv("TABLE_NAME", "daily_sales")

# tiny public endpoint — used only when USE_API=1; we map a couple fields into our schema
API_URL = os.getenv(
    "API_URL",
    "https://jsonplaceholder.typicode.com/posts?_limit=5",
)
