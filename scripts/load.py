from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

log = logging.getLogger(__name__)


def get_engine(db_url: str):
    # sqlite needs the parent folder to exist
    if db_url.startswith("sqlite///") or db_url.startswith("sqlite:///"):
        raw = db_url.replace("sqlite:///", "", 1)
        if raw and raw != ":memory:":
            Path(raw).parent.mkdir(parents=True, exist_ok=True)

    return create_engine(db_url)


def load_frame(df: pd.DataFrame, db_url: str, table: str) -> int:
    engine = get_engine(db_url)

    # replace keeps the local demo deterministic; swap to append in prod
    df.to_sql(table, con=engine, if_exists="replace", index=False)

    with engine.connect() as conn:
        n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()

    log.info("wrote %s rows to %s (%s)", n, table, _dialect_hint(db_url))
    return int(n or 0)


def _dialect_hint(db_url: str) -> str:
    if db_url.startswith("postgresql"):
        return "postgres"
    if db_url.startswith("sqlite"):
        return "sqlite"
    return "other"
