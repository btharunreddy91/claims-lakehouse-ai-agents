# Prefer the repo venv when it exists, else fall back to python3.
# (Plain `python` does not exist on this VM / many Linux installs.)
PY := $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python3)
PYTEST := $(shell [ -x .venv/bin/pytest ] && echo .venv/bin/pytest || echo pytest)

.PHONY: test gen bronze silver silver-spark up down produce consume produce-dry consume-dry
test:
	$(PYTEST) -q
gen:
	$(PY) -m src.generator.generate_claims --count 1000
bronze:
	$(PY) -m src.ingestion.bronze_ingest --input data/raw/claims.jsonl
silver:
	$(PY) -m src.streaming.silver_transform --bronze data/bronze --silver data/silver
silver-spark:
	spark-submit --packages io.delta:delta-spark_2.12:3.2.0 -m src.streaming.bronze_to_silver --bronze data/bronze --silver data/silver --trigger once
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
