# Sentinel Fraud Pipeline — Master Spec

> **Documento canónico de alcance.** Define **qué** se construye en cada fase.
> El proceso de trabajo (cómo, cuándo commitear, micro-pasos) vive en [`/CLAUDE.md`](../CLAUDE.md).
> Si hay conflicto: `CLAUDE.md` manda en proceso, este archivo manda en alcance.

---

## Context

Estoy construyendo **sentinel-fraud-pipeline**: un pipeline end-to-end de detección de fraude usando el dataset [Kaggle Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud). Todo el stack corre localmente con Docker Compose, cero dependencias cloud.

## Tech stack

| Component          | Tool                                       |
|--------------------|--------------------------------------------|
| Streaming          | Apache Kafka + Python producer             |
| Processing         | **PySpark** (batch + structured streaming) |
| Orchestration      | Apache Airflow                             |
| Object storage     | MinIO (local S3)                           |
| Table format       | Delta Lake (on Spark)                      |
| Transformations    | PySpark (Bronze → Silver → Gold)           |
| ML training        | Scikit-learn or Spark MLlib                |
| Experiment tracking| MLflow                                     |
| Dashboard          | Grafana                                    |
| Infrastructure     | Docker Compose                             |
| Language           | Python 3.11+                               |

## Architecture (Medallion)

```
CSV → Kafka Producer → [transactions topic]
                            ↓
                    Spark Structured Streaming
                            ↓
                    Bronze (raw Delta on MinIO)
                            ↓
                    Spark batch job
                            ↓
                    Silver (cleaned + features, Delta on MinIO)
                            ↓
                    Spark batch job + ML inference
                            ↓
                    Gold (aggregations + fraud scores, Delta on MinIO)
                            ↓
                    Grafana dashboard
```

## How we work

**Incremental approach — one phase at a time.** After each phase:
- Give me the complete files (no placeholders, no TODOs).
- Include a "Smoke test" section: exact commands I can run to verify it works.
- Wait for my confirmation before moving to the next phase.

**Code quality rules (all phases):**
- Python: type hints, docstrings, error handling, logging.
- Every config value from env vars with sensible defaults.
- `pathlib.Path` over `os.path`, `logging` over `print`.

---

## Phase 1 — Infrastructure foundation

**Goal:** `docker compose up -d` boots the core services and everything is green.

**Deliver:**
1. `docker-compose.yml` with these services only:
   - Zookeeper + Kafka (with health checks, auto-create topic `transactions`)
   - MinIO + init container (create buckets: `bronze`, `silver`, `gold`, `mlflow`)
   - Spark master + 1 worker (use `bitnami/spark:3.5` or similar, configured to access MinIO)
   - Postgres (for Airflow metadata — just set it up now, Airflow comes later)
2. `.env.example` with all variables
3. `Makefile` with: `up`, `down`, `logs`, `health`, `clean`
4. `scripts/health_check.sh` — verifies every service responds

**Smoke test I expect:**
```bash
cp .env.example .env
make up
make health        # all services green
# MinIO console at localhost:9001 — buckets visible
# Spark master UI at localhost:8080
```

---

## Phase 2 — Kafka producer + Bronze ingestion

**Goal:** Messages flow from CSV → Kafka → Bronze Delta table on MinIO.

**Deliver:**
1. `src/producer/transaction_producer.py`
   - Reads `data/creditcard.csv` row by row → publishes JSON to `transactions` topic
   - Adds: `event_id` (UUID), `event_timestamp` (ISO 8601)
   - Env var `PRODUCER_MODE`: `slow` (1 msg/s), `fast` (no delay), `realistic` (random 0.01–0.5s)
   - Progress bar with `tqdm`, graceful shutdown on SIGINT
2. `src/consumers/bronze_consumer.py`
   - **PySpark Structured Streaming** reading from Kafka
   - Writes raw data as Delta Lake to `s3a://bronze/transactions/`
   - Zero transformations — store exactly what arrives
   - Add audit columns: `_ingested_at`, `_kafka_offset`, `_kafka_partition`
   - Checkpoint to MinIO for exactly-once semantics

**Smoke test I expect:**
```bash
# Terminal 1: start the producer (send ~500 messages in slow mode)
make producer ARGS="--mode slow --limit 500"

# Terminal 2: start the bronze consumer
make bronze

# Verify:
# 1. Messages visible in Kafka (kafka-console-consumer)
# 2. Delta table exists in MinIO at bronze/transactions/
# 3. Row count matches: spark.read.format("delta").load("s3a://bronze/transactions/").count() == 500
```

---

## Phase 3 — Silver layer (PySpark batch)

**Goal:** Clean, deduplicate, and feature-engineer Bronze data into Silver Delta tables.

**Deliver:**
1. `src/transformations/silver.py` — PySpark batch job that:
   - Reads Bronze Delta table
   - Deduplicates by `event_id`
   - Casts types (V1-V28 as DoubleType, Amount as DecimalType, Class as IntegerType)
   - Filters nulls/invalid rows
   - Adds: `transaction_hour`, `transaction_day_of_week`, `amount_bin` (low/medium/high/very_high)
   - Adds features: `avg_amount_last_5` (window), `amount_zscore`, `is_high_amount`, `hour_sin`, `hour_cos`
   - Writes to `s3a://silver/transactions/` as Delta
2. `tests/test_silver.py` — unit tests using a small fixture DataFrame (no Kafka needed):
   - Test deduplication removes dupes
   - Test null filtering works
   - Test `amount_bin` assigns correct categories
   - Test feature columns are present and non-null

**Smoke test I expect:**
```bash
# Run silver job (assumes bronze has data from Phase 2)
make silver

# Verify:
# 1. Delta table at silver/transactions/ in MinIO
# 2. No duplicates: count of distinct event_id == total count
# 3. All feature columns exist
# 4. Unit tests pass: make test-silver
```

---

## Phase 4 — ML training + MLflow

**Goal:** Train a model, log everything to MLflow, register it.

**Deliver:**
1. Add `mlflow` service to `docker-compose.yml` (port 5000, artifacts on MinIO `s3://mlflow/`)
2. `src/ml/train.py`:
   - Load Silver Delta table via PySpark, convert to Pandas for sklearn
   - Train Random Forest (and optionally XGBoost for comparison)
   - Handle imbalance: `class_weight='balanced'`
   - 80/20 stratified split
   - Log to MLflow: params, metrics (precision, recall, F1, AUC-ROC), confusion matrix PNG, feature importance PNG
   - Register best model as `sentinel-fraud-model`
3. `src/ml/predict.py`:
   - Load latest Production model from MLflow
   - Score a PySpark DataFrame (via Pandas UDF or collect to Pandas)
   - Write predictions to `s3a://gold/predictions/` as Delta

**Smoke test I expect:**
```bash
make train

# Verify:
# 1. MLflow UI at localhost:5000 — experiment visible with runs
# 2. Metrics logged: precision, recall, f1, roc_auc
# 3. Artifacts: confusion_matrix.png, feature_importance.png, model
# 4. Model registered in Model Registry
```

---

## Phase 5 — Gold layer + inference

**Goal:** Aggregation tables and fraud scores ready for dashboarding.

**Deliver:**
1. `src/transformations/gold.py` — PySpark batch job:
   - `gld_fraud_scores`: join Silver features with predictions, include fraud_probability, fraud_label, model_version
   - `gld_agg_by_customer`: simulate customer_id via hash(V1,V2,V3), aggregate totals/fraud_rate
   - `gld_agg_by_merchant`: simulate merchant_id via hash(V4,V5,V6), aggregate totals/fraud_rate
   - `gld_agg_by_hour`: group by transaction_hour, totals/fraud_rate/p95_amount
   - All written as Delta to `s3a://gold/`

**Smoke test I expect:**
```bash
make gold

# Verify:
# 1. All 4 tables exist in MinIO gold/
# 2. gld_fraud_scores has fraud_probability between 0 and 1
# 3. gld_agg_by_hour has 24 rows (one per hour)
# 4. Fraud rates are reasonable (dataset is ~0.17% fraud)
```

---

## Phase 6 — Airflow orchestration

**Goal:** One DAG runs the full pipeline automatically.

**Deliver:**
1. Add Airflow services to `docker-compose.yml` (webserver + scheduler + init)
2. `airflow/dags/sentinel_pipeline_dag.py`:
   - DAG: `sentinel_fraud_pipeline`, schedule `@hourly`
   - Tasks: `check_bronze` → `run_silver` → `run_inference` → `run_gold` → `notify`
   - Retries=3, SLA, failure alerts
   - Use `SparkSubmitOperator` or `BashOperator` calling spark-submit

**Smoke test I expect:**
```bash
# Airflow UI at localhost:8081 — DAG visible, trigger manually, all tasks green
```

---

## Phase 7 — Grafana dashboard + README

**Goal:** Visual dashboard and professional documentation.

**Deliver:**
1. Grafana dashboard JSON (auto-provisioned): transaction feed, fraud distribution, alerts, model metrics
2. `README.md`: overview, architecture, business context ($32B fraud losses), quick start, service URLs, tech decisions

**Smoke test I expect:**
```bash
# Grafana at localhost:3000 — dashboard loads with real data
# README renders correctly on GitHub
```

---

## Working order

Always start with Phase 1. Do not jump phases. After each phase: smoke test → commit → wait for explicit confirmation → next phase.

Within each phase, the granular micro-step breakdown lives in [`/CLAUDE.md`](../CLAUDE.md).
