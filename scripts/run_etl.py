#!/usr/bin/env python3
"""Daily sales ETL entrypoint.

Usage:
    python scripts/run_etl.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# allow `python scripts/run_etl.py` from repo root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.config import DB_URL, SALES_CSV, TABLE_NAME, USE_API
from scripts.extract import fetch_api_extras, read_sales_csv
from scripts.load import load_frame
from scripts.transform import clean_sales

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger("etl")


def main() -> int:
    frames = [read_sales_csv(SALES_CSV)]

    if USE_API:
        api_df = fetch_api_extras()
        if not api_df.empty:
            frames.append(api_df)

    raw = pd.concat(frames, ignore_index=True)
    clean = clean_sales(raw)

    if clean.empty:
        log.error("nothing left after cleaning — aborting load")
        return 1

    loaded = load_frame(clean, DB_URL, TABLE_NAME)
    log.info("done. loaded=%s preview:\\n%s", loaded, clean.head(3).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
