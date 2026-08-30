.PHONY: install lint format format-check typecheck test test-cov check build clean

install:
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check .

format:
	ruff format .

format-check:
	ruff format --check .

typecheck:
	mypy src tests

test:
	pytest -q

test-cov:
	pytest -q --cov=edgeguard --cov-report=term-missing

check: lint format-check typecheck test

build:
	python -m build

clean:
	rm -rf dist build *.egg-info .pytest_cache .mypy_cache .ruff_cache .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
