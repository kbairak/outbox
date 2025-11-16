.PHONY: all ruff mypy ty lint test

all: lint test

ruff:
	uv run ruff format .
	uv run ruff check --fix .

mypy:
	uv run mypy .

ty:
	uv run ty check .

lint: ruff mypy ty

test:
	uv run pytest -vv --cov --cov-report=term-missing
