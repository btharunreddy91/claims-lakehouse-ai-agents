# Prefer the repo venv when it exists, else fall back to python3.
# (Plain `python` does not exist on this VM / many Linux installs.)
PY := $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
PYTEST := $(shell [ -x .venv/bin/pytest ] && echo .venv/bin/pytest || echo pytest)

.PHONY: test gen bronze up down produce consume produce-dry consume-dry
test:
	$(PYTEST) -q
gen:
	$(PY) -m src.generator.generate_claims --count 1000
bronze:
	$(PY) -m src.ingestion.bronze_ingest --input data/raw/claims.jsonl
up:
	docker compose up -d
down:
	docker compose down
produce:
	$(PY) -m src.generator.kafka_producer --count 100
consume:
	$(PY) -m src.ingestion.kafka_consumer --count 100 --to-bronze
produce-dry:
	$(PY) -m src.generator.kafka_producer --count 100 --dry-run
consume-dry:
	$(PY) -m src.ingestion.kafka_consumer --count 100 --dry-run --to-bronze
