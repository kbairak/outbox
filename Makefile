.PHONY: all ruff mypy ty lint test

all: lint test

ruff:
	uv run ruff format .
	uv run ruff check --fix .

mypy:
ifdef MYPY_PYTHON_VERSION
	uv run mypy --python-version=$(MYPY_PYTHON_VERSION) .
else
	uv run mypy .
endif

ty:
	uv run ty check .

lint: ruff mypy ty

test:
	uv run pytest -vv --cov --cov-report=term-missing
