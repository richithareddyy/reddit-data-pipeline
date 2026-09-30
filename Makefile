SCALE ?= small
COMPOSE := docker compose
RUN := $(COMPOSE) run --rm pipeline

.PHONY: help build up generate load report check test pipeline psql down clean

help:
	@echo "make up                   start PostgreSQL"
	@echo "make generate SCALE=...   write synthetic dumps to ./data (tiny|small|medium|full)"
	@echo "make load                 run the pipeline against ./data"
	@echo "make report               print the latest run summary"
	@echo "make check                re-run data quality checks"
	@echo "make test                 run unit and integration tests"
	@echo "make pipeline             generate + load"
	@echo "make psql                 open a psql shell"
	@echo "make down / make clean    stop services / also delete the database volume and ./data"

build:
	$(COMPOSE) build pipeline

up:
	$(COMPOSE) up -d --wait postgres

generate: build
	$(RUN) generate --scale $(SCALE) --out /data

load: up build
	$(RUN) run --data-dir /data

report:
	$(RUN) report

check:
	$(RUN) check

test: up build
	$(COMPOSE) run --rm --entrypoint pytest pipeline -q -p no:cacheprovider

pipeline: generate load

psql: up
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-reddit} -d $${POSTGRES_DB:-reddit}

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v
	rm -rf data
