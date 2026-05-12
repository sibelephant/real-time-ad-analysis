# Real-Time Ad Analysis System

A real-time advertising analytics platform built on a modern streaming data stack:

| Component | Role |
|---|---|
| **Redpanda** | Message broker (Kafka-compatible) |
| **Apache Flink (PyFlink)** | Stream processor |
| **Apache Pinot** | Real-time OLAP database |
| **FastAPI** | Event generator + Analytics REST API |

## Architecture

```
FastAPI (Generator)
       │
       ▼
  Redpanda ("ad-events" topic)
       │
       ▼
 Apache Flink (processor.py)
       │
       ▼
  Redpanda ("enriched-ad-events" topic)
       │
       ▼
  Apache Pinot (real-time table ingestion)
       │
       ▼
FastAPI (Analytics API)  ◄──  You / Dashboard
```

## Prerequisites

- Docker & Docker Compose v2 (`docker compose` command)

## Quick Start

### 1. Start all infrastructure services

```bash
docker compose up -d
```

This pulls and starts: Redpanda, Zookeeper, Pinot (Controller, Broker, Server), Flink (JobManager, TaskManager).
The FastAPI app is also built and exposed at `http://localhost:8000`.

### 2. Run the setup script

Wait ~60 seconds for all services to be healthy, then run:

```bash
./scripts/setup.sh
```

This will:
- Create the `ad-events` and `enriched-ad-events` Redpanda topics
- Upload the Pinot schema and real-time table config (connecting Pinot to the `enriched-ad-events` topic)
- Submit the Flink streaming job

### 3. Start generating ad events

```bash
curl -X POST http://localhost:8000/start_generation
```

Stop it anytime with:
```bash
curl -X POST http://localhost:8000/stop_generation
```

---

## API Reference

### Generator Control

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/start_generation` | Start publishing simulated ad events to Redpanda |
| `POST` | `/stop_generation` | Stop the event generator |
| `GET` | `/status` | Check if the generator is running |

### Analytics

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/analytics/summary` | CTR breakdown per campaign (impressions, clicks, CTR%) |
| `GET` | `/analytics/realtime` | Latest N events from Pinot (default: 10) |

#### Example: Summary Response

```json
{
  "data": [
    {
      "campaign_id": "camp_spring",
      "total_events": 4200,
      "impressions": 3780,
      "clicks": 420,
      "ctr": 11.11
    }
  ]
}
```

---

## Service UIs

| Service | URL | Description |
|---------|-----|-------------|
| FastAPI Swagger | http://localhost:8000/docs | Interactive API explorer |
| Flink Web UI | http://localhost:8081 | Monitor Flink job status & metrics |
| Pinot Data Explorer | http://localhost:9000 | Run ad-hoc SQL queries against Pinot |

---

## Project Structure

```
real-time-ad-analysis/
├── docker-compose.yml          # Full infrastructure stack
├── .dockerignore               # Docker build context exclusions
├── scripts/
│   └── setup.sh                # One-shot initialization script
├── pinot/
│   ├── schema.json             # Pinot table schema definition
│   └── table.json              # Pinot real-time table config (Kafka ingestion)
├── flink/
│   └── processor.py            # PyFlink Table API streaming job
└── app/
    ├── Dockerfile              # FastAPI app image
    ├── requirements.txt        # Python dependencies
    └── main.py                 # FastAPI app (generator + analytics API)
```

## Data Flow Details

1. **FastAPI Generator** creates synthetic ad events (impressions ~90%, clicks ~10%) and publishes them to the `ad-events` Redpanda topic at ~10 events/second.
2. **Flink** consumes `ad-events`, validates records (filters nulls), and sinks valid events to `enriched-ad-events`.
3. **Pinot** has a real-time table auto-ingesting from `enriched-ad-events` as messages arrive.
4. **FastAPI Analytics** queries Pinot via SQL (`pinotdb`) to compute CTR, event counts, and recency queries — with sub-second latency even over millions of rows.
