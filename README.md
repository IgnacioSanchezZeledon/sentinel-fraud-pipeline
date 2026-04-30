# sentinel-fraud-pipeline

End-to-end fraud detection pipeline built on the Kaggle Credit Card Fraud Detection dataset.
The full stack runs locally with Docker Compose — zero cloud dependencies.

> 🚧 **Work in progress.** This repository is being built incrementally, phase by phase.
> The full README (architecture diagrams, business context, screenshots, setup) lands in Phase 7.

## Stack

| Component           | Tool                                         |
|---------------------|----------------------------------------------|
| Streaming           | Apache Kafka + Python producer               |
| Processing          | PySpark (batch + structured streaming)       |
| Orchestration       | Apache Airflow                               |
| Object storage      | MinIO (local S3)                             |
| Table format        | Delta Lake on Spark                          |
| ML training         | scikit-learn / Spark MLlib                   |
| Experiment tracking | MLflow                                       |
| Dashboard           | Grafana                                      |
| Infrastructure      | Docker Compose                               |
| Language            | Python 3.11+                                 |

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
            Silver (cleaned + features, Delta)
                            ↓
                Spark batch + ML inference
                            ↓
        Gold (aggregations + fraud scores, Delta)
                            ↓
                    Grafana dashboard
```

## Build status

| Phase | Description                              | Status |
|-------|------------------------------------------|:------:|
| 0     | Git foundation                           |   ✅   |
| 1     | Infrastructure (Docker Compose)          |   ⏳   |
| 2     | Kafka producer + Bronze ingestion        |   ⏳   |
| 3     | Silver layer (PySpark batch)             |   ⏳   |
| 4     | ML training + MLflow                     |   ⏳   |
| 5     | Gold layer + inference                   |   ⏳   |
| 6     | Airflow orchestration                    |   ⏳   |
| 7     | Grafana dashboard + full documentation   |   ⏳   |

## License

MIT — see [LICENSE](LICENSE).
