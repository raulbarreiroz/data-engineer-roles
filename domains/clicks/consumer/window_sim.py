"""Tumbling-window simulator (Flink-ish, pure Python).

Groups click events by user_id into fixed windows of --window-sec using
event time (ts_ms). Prints aggregates that a Flink SQL job would emit.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from pathlib import Path

OUTBOX = Path(__file__).resolve().parents[1] / "outbox" / "clicks.jsonl"


def load_file_events(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"no outbox yet: {path} (run the producer first)")
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def load_kafka_events(max_messages: int = 500, timeout_sec: float = 8.0):
    from kafka import KafkaConsumer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("CLICKS_TOPIC", "mesh.clicks.raw.v1")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=int(timeout_sec * 1000),
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    n = 0
    try:
        for msg in consumer:
            yield msg.value
            n += 1
            if n >= max_messages:
                break
    finally:
        consumer.close()


def tumble(events, window_sec: int):
    """Bucket by floor(ts_ms / window) and user_id."""
    win_ms = window_sec * 1000
    buckets: dict[tuple, list] = defaultdict(list)

    for ev in events:
        ts = int(ev["ts_ms"])
        start = ts - (ts % win_ms)
        key = (ev["user_id"], start)
        buckets[key].append(ev)

    rows = []
    for (user_id, start_ms), items in sorted(buckets.items(), key=lambda x: (x[0][1], x[0][0])):
        pages = {e["page"] for e in items}
        rows.append(
            {
                "user_id": user_id,
                "window_start_ms": start_ms,
                "window_end_ms": start_ms + win_ms,
                "clicks": len(items),
                "distinct_pages": len(pages),
                "pages": sorted(pages),
            }
        )
    return rows


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("file", "kafka"), default="file")
    p.add_argument("--window-sec", type=int, default=10)
    p.add_argument("--max-messages", type=int, default=500)
    args = p.parse_args()

    t0 = time.time()
    if args.mode == "kafka":
        events = list(load_kafka_events(args.max_messages))
    else:
        events = list(load_file_events(OUTBOX))

    aggs = tumble(events, args.window_sec)
    print(f"events={len(events)} windows={len(aggs)} elapsed={time.time()-t0:.2f}s")
    for row in aggs[:25]:
        print(json.dumps(row))
    if len(aggs) > 25:
        print(f"... {len(aggs) - 25} more windows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
