from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

from scripts.config import API_URL

log = logging.getLogger(__name__)


def read_sales_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")

    df = pd.read_csv(path, parse_dates=["order_date"], dayfirst=False)
    log.info("loaded %s rows from %s", len(df), path.name)
    return df


def fetch_api_extras(url: str = API_URL) -> pd.DataFrame:
    """Optional enrichment source.

    jsonplaceholder doesn't return sales data — we just show the pattern of
    merging an API payload into the same frame shape.
    """
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("API pull failed (%s) — continuing with CSV only", exc)
        return pd.DataFrame()

    rows = []
    for i, item in enumerate(resp.json()):
        rows.append(
            {
                "order_id": 9000 + i,
                "order_date": pd.Timestamp("2025-11-07") + pd.Timedelta(days=i),
                "customer_id": f"API-{item.get('userId', 0)}",
                "sku": f"SKU-API{item.get('id', i)}",
                "qty": 1,
                "unit_price": 9.99,
                "region": "API",
                "status": "completed",
            }
        )

    out = pd.DataFrame(rows)
    log.info("fetched %s synthetic rows from API", len(out))
    return out
