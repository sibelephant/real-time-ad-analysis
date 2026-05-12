-- Flink SQL job: consume raw ad-events, filter, and sink to enriched-ad-events
-- This runs via the Flink SQL Client: flink run -f /flink/job.sql

CREATE TABLE source_events (
    event_id STRING,
    campaign_id STRING,
    ad_id STRING,
    user_id STRING,
    event_type STRING,
    event_time BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad-events',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-consumer-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

CREATE TABLE sink_events (
    event_id STRING,
    campaign_id STRING,
    ad_id STRING,
    user_id STRING,
    event_type STRING,
    event_time BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'enriched-ad-events',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'format' = 'json'
);

INSERT INTO sink_events
SELECT event_id, campaign_id, ad_id, user_id, event_type, event_time
FROM source_events
WHERE event_id IS NOT NULL
  AND campaign_id IS NOT NULL
  AND event_type IN ('impression', 'click');
