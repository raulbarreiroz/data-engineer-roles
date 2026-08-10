"""Synthetic click events for the clicks domain.

Modes:
  file  - append JSON lines to domains/clicks/outbox/clicks.jsonl (default)
  kafka - publish to KAFKA_BOOTSTRAP / CLICKS_TOPIC when a broker is up
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

OUTBOX = Path(__file__).resolve().parents[1] / "outbox" / "clicks.jsonl"

PAGES = ["/", "/pricing", "/docs", "/blog", "/signup"]
UTM = ["organic", "paid", "email", "referral", None]


def make_event(i: int) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"u-{random.randint(1, 80):04d}",
        "session_id": f"s-{random.randint(1, 40):04d}",
        "page": random.choice(PAGES),
        "referrer": random.choice(UTM),
        "ts": now.isoformat(),
        "ts_ms": int(now.timestamp() * 1000),
        "seq": i,
    }


def emit_file(n: int, sleep_ms: int) -> None:
    OUTBOX.parent.mkdir(parents=True, exist_ok=True)
    with OUTBOX.open("a", encoding="utf-8") as fh:
        for i in range(n):
            ev = make_event(i)
            fh.write(json.dumps(ev) + "\n")
            if sleep_ms:
                time.sleep(sleep_ms / 1000.0)
    print(f"wrote {n} events -> {OUTBOX}")


def emit_kafka(n: int, sleep_ms: int) -> None:
    from kafka import KafkaProducer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("CLICKS_TOPIC", "mesh.clicks.raw.v1")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks=1,
    )

    try:
        for i in range(n):
            ev = make_event(i)
            producer.send(topic, key=ev["user_id"], value=ev)
            if sleep_ms:
                time.sleep(sleep_ms / 1000.0)
        producer.flush()
        print(f"sent {n} events to {topic}@{bootstrap}")
    finally:
        producer.close()


def main() -> int:
    p = argparse.ArgumentParser(description="Emit click events for the clicks domain")
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--mode", choices=("file", "kafka"), default="file")
    p.add_argument("--sleep-ms", type=int, default=0, help="optional pacing between events")
    args = p.parse_args()

    if args.mode == "kafka":
        emit_kafka(args.count, args.sleep_ms)
    else:
        emit_file(args.count, args.sleep_ms)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
