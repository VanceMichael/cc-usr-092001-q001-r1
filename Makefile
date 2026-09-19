.PHONY: test migrate run
test:
	python -m unittest discover -s tests
migrate:
	python -m scripts.migrate
run:
	python -m src.app
