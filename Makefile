.PHONY: up down logs ps clean rebuild scoring-logs generator-logs shell-scoring shell-postgres

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

clean:
	docker compose down -v --remove-orphans

rebuild:
	docker compose down
	docker compose up -d --build

scoring-logs:
	docker compose logs -f fraud-scoring

generator-logs:
	docker compose logs -f transaction-generator

shell-scoring:
	docker compose exec fraud-scoring bash

shell-postgres:
	docker compose exec postgres psql -U fraud -d frauddb

urls:
	@echo "Dashboard:       http://localhost:3001"
	@echo "Fraud Scoring:   http://localhost:8001/docs"
	@echo "Enrichment:      http://localhost:8002/docs"
	@echo "Case Mgmt:       http://localhost:8003/docs"
	@echo "Kafdrop:         http://localhost:9000"
	@echo "Grafana:         http://localhost:3000  (admin/admin123)"
	@echo "Prometheus:      http://localhost:9090"
