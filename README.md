# SELIC Data Pipeline

Data pipeline orchestrated with Apache Airflow consuming the Brazilian Central Bank (BCB) public API.
Implements Medallion architecture (Bronze → Silver → Gold) for daily SELIC rate data (2020–2024),
with 35 isolated unit tests and quality gates between layers.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Tests](https://img.shields.io/badge/Tests-35%20passing-brightgreen)

## Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Ingestion | Python · BCB public API |
| Storage | Parquet (partitioned by layer) |
| Transformation | Pandas · custom quality gates |
| Testing | pytest · monkeypatch · tmp_path |
| Infrastructure | Docker · Docker Compose |

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      Apache Airflow DAG                          │
│                        selic_pipeline                            │
│                                                                  │
│  ┌─────────────────┐      ┌──────────────────┐      ┌─────────┐ │
│  │  bronze_ingest  │ ───► │ silver_transform  │ ───► │  gold   │ │
│  │                 │      │                  │      │aggregate│ │
│  │  BCB API → raw  │      │ type casting +   │      │monthly/ │ │
│  │  Parquet        │      │ derived columns  │      │annual   │ │
│  └─────────────────┘      └──────────────────┘      └─────────┘ │
└──────────────────────────────────────────────────────────────────┘
         │                         │                       │
         ▼                         ▼                       ▼
data/bronze/              data/silver/              data/gold/
selic_raw.parquet         selic_trusted.parquet     selic_mensal.parquet
                                                    selic_anual.parquet
```

### Layers

| Layer | File | Description |
|---|---|---|
| **Bronze** | `selic_raw.parquet` | Raw API response — string types, no transformation |
| **Silver** | `selic_trusted.parquet` | `date` as `datetime64`, `value` as `float64`, derived columns (`year`, `month`, `year_month`, `weekday`) |
| **Gold** | `selic_mensal.parquet` | Daily average, compounded monthly rate, delta vs previous month (pp) |
| **Gold** | `selic_anual.parquet` | Daily average, compounded annual rate |

## Key design decisions

### Compound rate vs arithmetic mean

Accumulated rate uses **compounding**: `(∏(1 + r_i/100) − 1) × 100`

This is the correct formula for SELIC — each day accrues on the updated balance, not on the original principal. Arithmetic mean would be incorrect for multi-day periods.

### `schedule_interval=None` (manual trigger)

The dataset is historical and static (2020–2024). A daily schedule would generate useless runs.
The DAG is designed for on-demand execution — in production, it would be parameterized for configurable date windows.

### Modules inside `dags/` with `PYTHONPATH`

`bronze/`, `silver/`, `gold/` modules live inside `dags/` and are importable via `PYTHONPATH=/opt/airflow/dags`.
Alternative would be `plugins/`, but that requires Airflow restart on every change — `dags/` reloads automatically via the scheduler.

### Quality gates between layers

Each layer validates data before persisting — raises `ValueError` on failure, failing the task without writing invalid data downstream. No external frameworks (Great Expectations would be overengineering for this scope).

| Layer | Gate |
|---|---|
| Bronze | Row count ≥ 1,000 |
| Silver | `value` ∈ [0, 0.2] · years ∈ [2020, 2024] |
| Gold | Business days per month ≥ 10 · annual rate ∈ [0%, 25%] |

### Retry strategy

`retries=3`, `retry_delay=5min` — BCB's public API shows sporadic instability outside business hours.

## Tests

35 unit tests covering quality gates, transformations, and compound rate logic.
Tests are fully isolated — no Airflow dependency, no real I/O. Uses `monkeypatch` on path constants and pytest's `tmp_path` for filesystem isolation.

```bash
pip install -r requirements-dev.txt
pytest
pytest --cov=. --cov-report=term-missing
```

| Module | Tests | Coverage |
|---|---|---|
| `test_bronze.py` | 9 | API schema, dynamic row count bounds, parameterized URL |
| `test_silver.py` | 14 | Type parsing, null ratio gate, derived columns, sort, quality gates |
| `test_gold.py` | 12 | `_compound_rate`, date sort, `is_base_month`, quality gates |

## Setup

### Docker (recommended)

```bash
git clone https://github.com/YOUR_USERNAME/selic-data-pipeline
cd selic-data-pipeline

cp .env.example .env

mkdir -p data

docker compose up --build airflow-init
docker compose up -d airflow-webserver airflow-scheduler

# Access http://localhost:8080 (admin / admin)
# DAGs → selic_pipeline → Enable → Trigger DAG ▶
```

Inspect output:

```bash
docker exec -it $(docker compose ps -q airflow-scheduler) \
  python -c "
import pandas as pd
df = pd.read_parquet('/opt/airflow/data/gold/selic_anual.parquet')
print(df.to_string())
"
```

Teardown:

```bash
docker compose down -v
```

### Local (no Docker)

```bash
python -m venv .venv && .venv\Scripts\activate
pip install "apache-airflow==2.9.1" -r requirements.txt

export AIRFLOW_HOME=$(pwd)/airflow_home
export PYTHONPATH=$(pwd)/dags
airflow db migrate
airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@local.com

# Two separate terminals:
airflow webserver --port 8080
airflow scheduler
```

## Repository structure

```
selic-data-pipeline/
├── dags/
│   ├── selic_pipeline.py       # Main DAG
│   ├── bronze/
│   │   └── ingest.py           # Task 1: BCB API → raw Parquet
│   ├── silver/
│   │   └── transform.py        # Task 2: type casting + derived columns
│   └── gold/
│       └── aggregate.py        # Task 3: monthly/annual aggregations
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── data/                       # Generated at runtime (gitignored)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── requirements-dev.txt
```
