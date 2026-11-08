SCALE ?= small
SUBREDDIT ?= dataengineering
COMPOSE := docker compose
RUN := $(COMPOSE) run --rm pipeline

.PHONY: help build up generate fetch load load-real report report-real check test pipeline psql down clean

help:
	@echo "make up                   start PostgreSQL"
	@echo "make generate SCALE=...   write synthetic dumps to ./data (tiny|small|medium|full)"
	@echo "make load                 run the pipeline against ./data"
	@echo "make fetch SUBREDDIT=...  download a real subreddit to ./data/<name> (resumable)"
	@echo "make load-real            load that download into the reddit_real database"
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

fetch: build
	$(RUN) fetch --subreddit $(SUBREDDIT) --out /data/$(SUBREDDIT)

load: up build
	$(RUN) run --data-dir /data

# Real data gets its own database so it never mixes with generated benchmark rows.
load-real: up build
	echo "SELECT 'CREATE DATABASE reddit_real' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'reddit_real')\gexec" \
		| $(COMPOSE) exec -T postgres psql -q -U $${POSTGRES_USER:-reddit} -d postgres
	$(COMPOSE) run --rm -e POSTGRES_DB=reddit_real pipeline run --data-dir /data/$(SUBREDDIT) --derive-authors

report-real:
	$(COMPOSE) run --rm -e POSTGRES_DB=reddit_real pipeline report

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
