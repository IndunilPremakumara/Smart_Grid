# Smart Grid Energy Monitoring & Billing Pipeline

> **Applied Big Data Engineering — Mini Project**
> Use Case 3: Smart Grid Energy Monitoring & Billing
> Architecture: **Lambda** (Speed Layer + Batch Layer + Serving Layer)

---

## Architecture Overview

```
Smart Meters (simulated)
        │  every 2 s
        ▼
  [ Kafka ]  ──────────────────────────────────────────────┐
  topic: meter.readings                                    │
        │                                                  │
        ▼  Spark Structured Streaming                      │ immutable
  [ Speed Layer ]                                          │ Parquet
  • windowed zone aggregates (1-hr sim windows)            │ master dataset
  • per-household running totals                           │ (MinIO / S3A)
  • threshold alerts → rt_alerts                           │
        │                                                  │
        ▼                                                  │
  [ PostgreSQL ]  rt_zone_load, rt_household_running       │
        │                                                  │
        │                     ┌────────────────────────────┘
        │                     │  once per sim day
        │                     ▼
        │           [ Batch Producer ]
        │           tariffs.csv + weather.csv → MinIO
        │                     │
        │                     ▼  Airflow-orchestrated Spark job
        │           [ Batch Layer ]
        │           Parquet ⨝ tariffs ⨝ weather → exact bills
        │                     │
        │                     ▼
        │           [ PostgreSQL ]  batch_household_bill, batch_zone_daily
        │                     │
        └──────────┬──────────┘
                   ▼
          [ FastAPI Serving Layer ]
          /realtime/*  → speed layer (approximate)
          /billing/*   → batch layer (exact, settled)
          /households  → Lambda merge (batch preferred)
```

**Simulated clock:** 1 simulated day = **5 wall-clock minutes** (288× compression).
Each meter emits one reading every 2 wall-clock seconds.

---

## Tech Stack

| Layer | Tool | Justification |
|---|---|---|
| Streaming ingestion | **Apache Kafka** | Durable, partitioned, replayable log — the Lambda master dataset is built from topic replay |
| Stream processing | **Apache Spark Structured Streaming** | Native watermarking, event-time windowing, Kafka source, S3A sink in one unified API |
| Batch orchestration | **Apache Airflow** | DAG-based scheduling with XCom, idempotent work-discovery pattern, built-in retry/alert |
| Storage | **PostgreSQL** | ACID guarantees for the serving tables; joins between speed and batch layers trivial in SQL |
| Object store | **MinIO (S3-compatible)** | Parquet master dataset + daily-batch landing zone; S3A protocol supported by Spark natively |
| Serving | **FastAPI** | Async, auto-documented, Prometheus `/metrics` endpoint built in |
| Observability | **Prometheus + Grafana** | Industry-standard pull-based metrics; dashboards auto-provisioned via JSON |

---

## Prerequisites

- Docker ≥ 24 and Docker Compose v2 (`docker compose` command)
- At least **6 GB RAM** allocated to Docker
- Ports free: 8000, 8080–8082, 9000–9001, 9090, 9092, 3000, 4040, 5432

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO

# 2. Copy and (optionally) edit environment variables
cp .env.example .env

# 3. Build images  (first time ~5 min, downloads JARs from Maven Central)
docker compose build

# 4. Start the full stack
docker compose up -d

# 5. Wait ~60 s for all health checks to pass, then check status
docker compose ps
```

### Verify the pipeline is running

```bash
# Real-time zone load (speed layer)
curl http://localhost:8000/api/v1/realtime/zones | python -m json.tool

# Active alerts
curl http://localhost:8000/api/v1/realtime/alerts | python -m json.tool

# Pipeline health
curl http://localhost:8000/health

# Per-household data (Lambda merge)
curl http://localhost:8000/api/v1/households | python -m json.tool
```

After one simulated day (~5 min), the batch layer produces a bill:

```bash
# Replace YYYY-MM-DD with the sim start date from .env (default 2026-01-01)
curl http://localhost:8000/api/v1/billing/2026-01-01 | python -m json.tool

# HTML report (open in browser)
open http://localhost:8000/api/v1/billing/2026-01-01/report
```

---

## Web UIs

| Service | URL | Default credentials |
|---|---|---|
| FastAPI docs (Swagger) | http://localhost:8000/docs | — |
| Spark Master | http://localhost:8080 | — |
| Airflow | http://localhost:8082 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |

---

## Project Structure

```
.
├── producers/
│   ├── stream_meters.py      # Continuous Kafka producer (smart meter readings)
│   └── batch_tariffs.py      # Daily-batch producer (tariffs + weather → MinIO)
├── streaming/
│   └── speed_layer.py        # Spark Structured Streaming job
├── batch/
│   └── jobs/
│       └── billing_reconciliation.py  # Spark batch job (Airflow-submitted)
├── airflow/
│   └── dags/
│       ├── daily_billing_dag.py       # Batch layer orchestration
│       └── pipeline_health_dag.py     # End-to-end health checks
├── serving/
│   └── api.py                # FastAPI serving layer
├── common/                   # Shared utilities (config, logging, metrics, domain)
├── sql/
│   └── init.sql              # PostgreSQL schema (speed + batch + ops tables + views)
├── observability/
│   ├── prometheus.yml        # Prometheus scrape config
│   ├── alert_rules.yml       # Alerting rules (staleness, error rate, renewables)
│   └── grafana/              # Auto-provisioned Grafana datasource + dashboard
├── docker/
│   ├── Dockerfile.spark      # Spark master/worker/driver image
│   ├── Dockerfile.airflow    # Airflow image with Spark + JARs
│   └── Dockerfile.python     # Lightweight image for producers and API
├── tests/
│   ├── test_speed_layer.py   # Unit tests: _evaluate_alerts, _zone_rows_from_batch
│   └── test_domain.py        # Unit tests: registry, solar_factor, demand_factor
├── docker-compose.yml
├── .env.example
└── requirements-*.txt
```

---

## Configuration

All settings are in `.env` (copy from `.env.example`). Key knobs:

| Variable | Default | Effect |
|---|---|---|
| `SIM_DAY_SECONDS` | `300` | Wall-clock seconds per simulated day (5 min) |
| `NUM_HOUSEHOLDS` | `60` | Number of simulated meters |
| `NUM_ZONES` | `4` | Grid zones |
| `ANOMALY_RATE` | `0.03` | Fraction of readings that are implausible spikes |
| `DUPLICATE_RATE` | `0.02` | Fraction of readings that are duplicates |
| `RENEWABLE_LOW_PCT` | `15.0` | Alert threshold: renewable contribution floor |
| `ZONE_OVERLOAD_KWH` | `45.0` | Alert threshold: zone load ceiling per window |

---

## Running Tests

```bash
# Install dev dependencies (no Spark or Kafka required)
pip install -r requirements-dev.txt

# Run all unit tests
pytest tests/ -v
```

---

## Stopping the Stack

```bash
# Stop without deleting data
docker compose down

# Stop AND delete all volumes (full reset)
docker compose down -v
```

---

## Key Design Decisions

### Why Lambda and not Kappa?

The billing use case has a hard requirement: the **tariff file arrives once per day**, after all meter readings for that day are in. The speed layer cannot produce an authoritative bill because:

1. The tariff reference data does not exist at reading time.
2. Billing must be **exact** — global de-duplication, all late events accounted for.
3. Bills must be **recomputable** months later if a tariff is restated.

These three requirements together make Lambda the correct choice. Kappa would require the stream to re-process the entire topic whenever a tariff changes, making corrections expensive and error-prone. Lambda provides a clean separation: the stream gives approximate real-time visibility, the batch layer gives the authoritative settled bill.

### Simulated time

One simulated day = 5 wall-clock minutes (288× compression). Windows and watermarks are expressed in **simulated seconds**, not wall-clock seconds, so the Grafana dashboard shows 24 one-hour windows per simulated day at a readable cadence of one window per ~12.5 wall seconds.

---

## Limitations

- Single Kafka broker — not suitable for production; add replication factor ≥ 3.
- Spark runs in standalone mode, not YARN/Kubernetes.
- Airflow uses LocalExecutor — production would use CeleryExecutor or KubernetesExecutor.
- MinIO is a single node; production would use distributed MinIO or AWS S3.
