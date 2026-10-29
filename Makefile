.DEFAULT_GOAL := help
COMPOSE := docker compose
FAULT ?= none
PSQL := $(COMPOSE) exec -T postgres psql -U dagfox -d dagfox -P pager=off

help: ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-10s %s\n", $$1, $$2}'

.env:
	cp .env.example .env

up: .env ## Build and start Airflow + Postgres, seed GLEIF data (one command)
	mkdir -p logs landing data/cache
	$(COMPOSE) up -d --build --wait
	@echo ""
	@echo "Airflow UI: http://localhost:8080  (ops_analyst / data_steward, password from .env)"

down: ## Stop the stack (keeps data)
	$(COMPOSE) down

reset: ## Stop the stack and delete all data (re-seeds on next `make up`)
	$(COMPOSE) down -v
	rm -rf landing logs

run: ## Trigger entities_daily. FAULT=none|late_file|bad_records|mass_change
	$(COMPOSE) exec -T airflow-scheduler airflow dags trigger entities_daily \
		--run-id "manual__$(FAULT)__$$(date +%Y%m%dT%H%M%S)" --conf '{"fault": "$(FAULT)"}'

audit: ## Show who decided what, newest last
	$(PSQL) -c "SELECT to_char(event_time,'HH24:MI:SS') AS time, split_part(dag_run_id,'__',2) AS run, \
		event_type, actor, band, human_decision, approver, final_action FROM dagfox.v_decisions;"

audit-verify: ## Verify the audit log hash chain
	$(COMPOSE) exec -T airflow-scheduler python -m dagfox.audit.log verify

test: ## Run unit tests (no network, LLM mocked)
	python -m pytest

test-docker: ## Run unit tests inside the Airflow image
	$(COMPOSE) exec -T airflow-scheduler python -m pytest /opt/dagfox/tests -p no:cacheprovider

.PHONY: help up down reset run audit audit-verify test test-docker
