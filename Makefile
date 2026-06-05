.DEFAULT_GOAL := help
.PHONY: help up down logs health clean smoke-phase1 producer bronze-consumer silver test-silver

help: ## Show this help message
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-13s %s\n", $$1, $$2}'

up: ## Start all services in background
	docker compose up -d

down: ## Stop and remove containers (volumes are kept)
	docker compose down

logs: ## Follow logs from all services
	docker compose logs -f --tail=100

health: ## Run the cluster health check script
	./scripts/health_check.sh

clean: ## Stop containers AND delete all volumes (destructive)
	docker compose down -v

smoke-phase1: ## Phase 1 smoke test from a clean slate (destructive: wipes volumes)
	$(MAKE) clean
	cp .env.example .env
	$(MAKE) up
	@echo "==> Waiting 30s for services to stabilize..."
	@sleep 30
	$(MAKE) health
	@echo "==> Phase 1 smoke test PASSED."

producer: ## Run the producer. Use ARGS="--mode slow --limit 10" to pass flags
	.venv/bin/python -m src.producer.transaction_producer $(ARGS)

bronze-consumer: ## Run the bronze structured streaming consumer (foreground; Ctrl+C to stop)
	docker compose exec -T spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /opt/app/src/consumers/bronze_consumer.py

silver: ## Run the silver batch job (bronze -> silver delta)
	docker compose exec -T spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /opt/app/src/transformations/silver_job.py

test-silver: ## Run silver transformation unit tests (local spark fixture; no docker)
	.venv/bin/pytest tests/transformations -v
