import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def run_job():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    source_ddl = """
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
        )
    """

    sink_ddl = """
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
        )
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    print("Starting Flink Job...")
    t_env.execute_sql("""
        INSERT INTO sink_events
        SELECT event_id, campaign_id, ad_id, user_id, event_type, event_time
        FROM source_events
        WHERE event_id IS NOT NULL
    """)
    print("Flink Job Submitted.")

if __name__ == '__main__':
    run_job()
