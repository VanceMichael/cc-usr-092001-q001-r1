.PHONY: test migrate run
test:
	python -m pytest tests -q
migrate:
	python -m scripts.migrate
run:
	python -m src.app
