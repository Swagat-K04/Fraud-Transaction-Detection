# Real-Time Credit Card Fraud Detection Pipeline (2025)

A production-grade modernised rebuild of a Kafka + Spark + Cassandra fraud detection pipeline, updated with current best-in-class technologies, Claude AI for explainable fraud reasoning, and a React dashboard.

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![XGBoost](https://img.shields.io/badge/ML-XGBoost-orange)](https://xgboost.readthedocs.io)
[![Kafka](https://img.shields.io/badge/Streaming-Kafka-black)](https://kafka.apache.org)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-green)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/Dashboard-React-blue)](https://react.dev)

---

## Architecture

```
producer/ (Python)
    │ synthetic transactions every 2s
    ▼
Kafka (creditcard.transactions — 3 partitions)
    │
    ▼
consumer/ (Python + XGBoost + Claude AI)
    │ feature engineering → ML prediction → AI explanation
    ├──► PostgreSQL / TimescaleDB  (persistence)
    └──► Redis Pub/Sub             (real-time push)
                │
                ▼
          FastAPI (REST + WebSocket)
                │
                ▼
        React Dashboard (Vite + Recharts)
```

## What Changed From the Original (2019 → 2025)

| Component | Original | This Version |
|-----------|----------|-------------|
| Language | Scala / Java | Python |
| ML Model | Spark MLlib Random Forest | XGBoost + SHAP explainability |
| AI Reasoning | None | Claude claude-sonnet-4-20250514 |
| Storage | Cassandra | PostgreSQL + TimescaleDB |
| Real-time push | DB polling every 5s | Redis Pub/Sub → WebSocket |
| API | Spring Boot + Flask | FastAPI (async, OpenAPI docs) |
| Frontend | Basic HTML/JS | React + Vite + Recharts + Tailwind |
| Setup | Manual | Docker Compose (one command) |

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 24+
- An [Anthropic API key](https://console.anthropic.com) (optional — falls back to rule-based explanations if not set)

---

## Setup & Running

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/fraud-detection.git
cd fraud-detection
```

### 2. Configure environment

```bash
cp .env.example .env
```

Open `.env` and add your Anthropic API key:
```env
ANTHROPIC_API_KEY=sk-ant-your-key-here
```

> The app works without an API key — it falls back to rule-based fraud explanations automatically.

### 3. Start infrastructure

```bash
docker compose up -d zookeeper kafka postgres redis
```

**Windows (PowerShell):**
```powershell
docker compose up -d zookeeper kafka postgres redis
Start-Sleep -Seconds 25
```

**Mac/Linux:**
```bash
sleep 25
```

### 4. Create Kafka topics

```bash
docker compose up kafka-init
```

Expected output:
```
Created topic creditcard.transactions.
Created topic fraud.alerts.
Topics created successfully
```

### 5. Train the ML model (first time only)

```bash
docker compose run --rm consumer python train.py
```

This generates synthetic training data, trains XGBoost with K-Means undersampling, and saves the model to `consumer/models/`. Takes ~1 minute.

Expected output:
```
Generated 12000 synthetic training records
K-Means undersampling: 11239 → 761 non-fraud samples
CV best round: 187 | AUC: 0.9614
Model saved → /app/models/xgb_fraud.json
```

### 6. Create the missing kafka_offsets table

```bash
docker compose exec postgres psql -U fraud -d frauddb -c "
CREATE TABLE IF NOT EXISTS kafka_offsets (
    topic            TEXT NOT NULL,
    kafka_partition  INT  NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (topic, kafka_partition)
);"
```

### 7. Start all services

```bash
docker compose up -d producer consumer api dashboard
```

### 8. Open the dashboard

| Service | URL |
|---------|-----|
| Dashboard | http://localhost:5173 |
| API docs | http://localhost:8000/docs |

---

## Useful Commands

```bash
# Watch live transaction predictions
docker compose logs -f consumer

# Watch all service logs
docker compose logs -f

# Stop everything
docker compose down

# Full reset (wipes database)
docker compose down
docker volume rm fraud-detection_postgres_data

# Rebuild after code changes
docker rmi fraud-detection-consumer --force
docker compose build consumer
docker compose up -d consumer
```

---

## Project Structure

```
fraud-detection/
├── producer/
│   ├── generator.py        # Synthetic transaction + customer data generator
│   ├── producer.py         # Kafka producer — streams transactions every 2s
│   └── requirements.txt
├── consumer/
│   ├── consumer.py         # Kafka consumer — orchestrates the full pipeline
│   ├── features.py         # Feature engineering (age, distance, velocity, z-score)
│   ├── model.py            # XGBoost wrapper + SHAP explainability
│   ├── train.py            # Offline training script
│   ├── explainer.py        # Claude AI reasoning integration
│   ├── generator.py        # Shared data generator (copy of producer/generator.py)
│   └── requirements.txt
├── api/
│   ├── main.py             # FastAPI app — REST + WebSocket
│   ├── db.py               # Async PostgreSQL pool
│   └── routes/
│       ├── transactions.py
│       ├── customers.py
│       └── stats.py
├── dashboard/
│   └── src/
│       ├── App.jsx
│       └── components/
│           ├── StatsBar.jsx
│           ├── RiskChart.jsx
│           ├── FeedPanel.jsx
│           ├── FraudAlerts.jsx
│           └── CustomerDrawer.jsx
├── docker/
│   ├── init.sql            # PostgreSQL + TimescaleDB schema
│   ├── Dockerfile.producer
│   ├── Dockerfile.consumer
│   └── Dockerfile.api
├── docker-compose.yml
├── .env.example            # Template — copy to .env and fill in your key
└── README.md
```

---

## How It Works

1. **Producer** generates a realistic credit card transaction every 2 seconds and sends it to Kafka
2. **Consumer** reads from Kafka, engineers 7 ML features (age, distance from home, amount z-score, transaction velocity, hour of day, day of week, merchant category), runs XGBoost prediction, calls Claude for a human-readable explanation, then writes to PostgreSQL and publishes to Redis
3. **FastAPI** serves REST endpoints for charts/stats and maintains a WebSocket connection that pushes every transaction to the browser instantly via Redis Pub/Sub
4. **Dashboard** displays the live feed, fraud alerts with SHAP feature attribution bars, KPI stats, and a clickable customer detail drawer

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/transactions` | Recent transactions (filter by fraud, risk level, card) |
| GET | `/api/transactions/:id` | Single transaction detail |
| GET | `/api/customer/:cc_num` | Customer profile + transaction history |
| GET | `/api/statement/:cc_num` | Non-fraud statement for a card |
| GET | `/api/stats/summary` | 24h KPI summary |
| GET | `/api/stats/hourly` | Hourly transaction volume |
| GET | `/api/stats/by-category` | Fraud breakdown by merchant category |
| WS  | `/ws` | Real-time transaction stream |

Full interactive docs: http://localhost:8000/docs

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | — | Claude API key (optional) |
| `KAFKA_BOOTSTRAP` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `creditcard.transactions` | Topic name |
| `POSTGRES_URL` | `postgresql://fraud:fraud@postgres:5432/frauddb` | Database URL |
| `REDIS_URL` | `redis://redis:6379` | Redis URL |
| `STREAM_INTERVAL_MS` | `2000` | Transaction generation interval |
| `PRODUCER_MODE` | `live` | `live` or `replay` (replay from CSV) |

---

## Original Project

This is a modernised rebuild of [J-An-dev/real-time-fraud-detection](https://github.com/J-An-dev/real-time-fraud-detection).