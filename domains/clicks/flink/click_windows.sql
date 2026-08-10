-- Flink SQL sketch for the clicks domain
-- Equivalent logic lives in domains/clicks/consumer/window_sim.py

CREATE TABLE clicks_raw (
  event_id STRING,
  user_id STRING,
  session_id STRING,
  page STRING,
  referrer STRING,
  ts_ms BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(ts_ms, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'mesh.clicks.raw.v1',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE click_windows (
  user_id STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  clicks BIGINT,
  distinct_pages BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'mesh.clicks.windows.v1',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);

INSERT INTO click_windows
SELECT
  user_id,
  window_start,
  window_end,
  COUNT(*) AS clicks,
  COUNT(DISTINCT page) AS distinct_pages
FROM TABLE(
  TUMBLE(TABLE clicks_raw, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
GROUP BY user_id, window_start, window_end;
