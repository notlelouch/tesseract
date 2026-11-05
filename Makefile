PY ?= python3.12
VENV := .venv
BIN := $(VENV)/bin
CONFIG ?= config.toml

.PHONY: install test run run-dev summary clean

install:
	uv venv -p $(PY) $(VENV) || $(PY) -m venv $(VENV)
	uv pip install -p $(BIN)/python -e '.[dev]' || $(BIN)/pip install -e '.[dev]'

test:
	$(BIN)/pytest -q

run:
	$(BIN)/tesseract-record --config $(CONFIG)

# DURATION in seconds, e.g. make run-dev DURATION=3600
run-dev:
	$(BIN)/tesseract-record --config $(CONFIG) --duration $(or $(DURATION),3600)

summary:
	$(BIN)/python -m tesseract.recorder.summary --data-dir $(or $(DATA),data)
