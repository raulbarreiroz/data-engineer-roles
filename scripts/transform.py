from __future__ import annotations

import logging

import pandas as pd

log = logging.getLogger(__name__)

REQUIRED = ["order_id", "order_date", "customer_id", "sku", "qty", "unit_price"]


def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # drop exact dupes first (sample file has a repeated 1002)
    work = df.drop_duplicates(subset=["order_id"], keep="first").copy()

    # blank strings -> NaN so dropna catches them
    for col in ["customer_id", "sku", "status", "region"]:
        if col in work.columns:
            work[col] = work[col].replace(r"^\s*$", pd.NA, regex=True)

    work = work.dropna(subset=REQUIRED)

    work["qty"] = pd.to_numeric(work["qty"], errors="coerce")
    work["unit_price"] = pd.to_numeric(work["unit_price"], errors="coerce")
    work = work.dropna(subset=["qty", "unit_price"])

    # skip cancelled / pending for the warehouse fact table
    if "status" in work.columns:
        work = work[work["status"].str.lower() == "completed"]

    work["line_total"] = (work["qty"] * work["unit_price"]).round(2)
    work["order_date"] = pd.to_datetime(work["order_date"], errors="coerce")
    work = work.dropna(subset=["order_date"])

    work = work.sort_values(["order_date", "order_id"]).reset_index(drop=True)

    log.info("cleaned %s -> %s rows", before, len(work))
    return work
