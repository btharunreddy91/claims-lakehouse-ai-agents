.PHONY: test gen bronze up down
test:
	pytest -q
gen:
	python -m src.generator.generate_claims --count 1000
bronze:
	python -m src.ingestion.bronze_ingest --input data/raw/claims.jsonl
up:
	docker compose up -d
down:
	docker compose down
