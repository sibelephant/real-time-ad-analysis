# Real-Time Ad Analysis System Implementation Plan

This document outlines the proposed architecture and implementation steps for a real-time ad analysis system using FastAPI, Redpanda, Flink, and Apache Pinot.

## Problem Description
The goal is to build a demonstration of a modern real-time streaming analytics stack. The system will simulate ad impressions and clicks, stream them through a message broker, process them, and make them available for fast analytical queries via a REST API.

## Architecture Overview

1.  **Redpanda**: The core message broker. We will have two topics: `ad-events` (raw data) and `enriched-ad-events` (processed data).
2.  **FastAPI (Data Generator & API)**:
    *   **Generator**: A background process or endpoint that simulates and publishes ad events (impressions and clicks) to the `ad-events` Redpanda topic.
    *   **Analytics API**: Exposes REST endpoints to query the processed data from Pinot and return analytics (e.g., Click-Through Rate per campaign).
3.  **Apache Flink (PyFlink)**: A stream processing job that reads raw events from `ad-events`, performs validation/enrichment (e.g., adding processing timestamps or filtering out malformed data), and writes the output to `enriched-ad-events`.
4.  **Apache Pinot**: The real-time OLAP database. It will be configured to ingest data directly from the `enriched-ad-events` Redpanda topic. The FastAPI analytics endpoints will query Pinot.

## User Review Required

> [!IMPORTANT]
> The Flink job will be written in **Python (PyFlink)** for simplicity and better integration with the Python ecosystem used in the FastAPI application. Please confirm if you prefer a Java/Scala Flink job instead.
> 
> The system will be containerized using `docker-compose`. Ensure you have Docker and Docker Compose installed and running on your system.

## Proposed Changes

### Docker Infrastructure
We will create a `docker-compose.yml` to orchestrate all necessary services.

#### [NEW] docker-compose.yml
*   Redpanda (1 node)
*   Pinot Zookeeper
*   Pinot Controller
*   Pinot Broker
*   Pinot Server
*   Flink JobManager
*   Flink TaskManager
*   FastAPI Application Container (optional, or run locally)

### Apache Pinot Configuration
Definitions for the Pinot table and schema to ingest from Redpanda.

#### [NEW] pinot/schema.json
Defines the schema for ad events (e.g., `timestamp`, `campaign_id`, `ad_id`, `event_type`, `user_id`).

#### [NEW] pinot/table.json
Defines the real-time table configuration, connecting it to the `enriched-ad-events` Redpanda topic and mapping it to the schema.

### Flink Stream Processing (PyFlink)
The Flink job to process the data stream.

#### [NEW] flink/processor.py
A PyFlink script using the Table API/SQL to consume from Redpanda, apply a simple transformation, and sink back to Redpanda.
*Requires downloading the `flink-sql-connector-kafka` JAR.*

### FastAPI Application
The core Python application for generation and serving.

#### [NEW] app/requirements.txt
Dependencies: `fastapi`, `uvicorn`, `kafka-python`, `pinotdb`, `pydantic`.

#### [NEW] app/main.py
*   **Producer**: Uses `kafka-python` to send simulated JSON events to Redpanda.
*   **API**: Uses `pinotdb` to query Pinot for analytics (e.g., `/analytics/ctr?campaign_id=123`).

#### [NEW] scripts/setup.sh
A bash script to initialize the Redpanda topics, create the Pinot schema/table, and submit the Flink job.

## Verification Plan

### Automated/Scripted Tests
1.  Run `docker-compose up -d` to start all infrastructure components.
2.  Run `scripts/setup.sh` to initialize topics, Pinot tables, and submit the Flink job.
3.  Start the FastAPI application.
4.  Trigger the data generator via a FastAPI endpoint or background task.
5.  Call the FastAPI analytics endpoints (e.g., `/analytics/summary`) to verify that data is flowing through Redpanda -> Flink -> Pinot and is queryable.

### Manual Verification
*   Check the Flink Web UI (usually `localhost:8081`) to verify the job is running.
*   Check the Pinot Data Explorer (usually `localhost:9000`) to execute raw SQL queries against the ingested data.
*   Check Redpanda Console (if added to docker-compose) or CLI to inspect topic messages.
