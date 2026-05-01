.DEFAULT_GOAL := help
.PHONY: help up down logs health clean

help: ## Show this help message
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

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
