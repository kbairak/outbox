.PHONY: all ruff mypy ty lint test

all: lint test

ruff:
	uv run ruff format .
	uv run ruff check --fix .

ty:
	uv run ty check .

mypy:
	uv run mypy .

lint: ruff ty mypy

test:
	uv run pytest -vv --cov --cov-report=term-missing
