# Setup - Clickstream mesh (Kafka + streaming consumer)

## What you get

A Data Mesh-ish layout with two domains:

- `domains/clicks/` - producers + a Python consumer that simulates Flink-style tumbling windows
- `domains/sessions/` - contract notes for a downstream domain that would consume click aggregates

Kafka is optional. Without a broker the producer writes JSONL to `domains/clicks/outbox/` and the consumer can read that file - same windowing math either way.

## Quick start (no Kafka)

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python -m domains.clicks.producer.emit_clicks --count 200 --mode file
python -m domains.clicks.consumer.window_sim --mode file --window-sec 10
```

## With Kafka

```bash
export KAFKA_BOOTSTRAP=localhost:9092
export CLICKS_TOPIC=mesh.clicks.raw.v1

python -m domains.clicks.producer.emit_clicks --count 500 --mode kafka
python -m domains.clicks.consumer.window_sim --mode kafka --window-sec 10
```

Topic naming follows `<mesh>.<domain>.<dataset>.vN`.

## Flink notes (conceptual)

See `domains/clicks/flink/click_windows.sql` for the equivalent Flink SQL. On a real cluster you'd register the Kafka source with event-time + watermark, run a tumbling window aggregate, then sink to Iceberg or another Kafka topic owned by the sessions domain.

This repo keeps a Python window simulator so reviewers can run the pipeline without JVM/Flink setup.

## Data Mesh contracts

`domains/clicks/contracts/clicks_raw.yaml` and `domains/sessions/contracts/session_features.yaml` describe ownership, SLAs, and schema.
