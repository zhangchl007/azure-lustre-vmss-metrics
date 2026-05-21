# Copilot instructions for this repository

## Test and validation workflow

- This repository has an existing Python virtual environment at `.venv/`.
- Before running Python tests, lint, or validation commands, activate it first:
  - `source .venv/bin/activate`
- Review and prefer the `Makefile` targets instead of inventing ad-hoc commands:
  - `make test` runs `pytest -q`.
  - `make lint` runs `ruff check .`.
  - `make validate` runs tests and lint.
- For small changes, run targeted tests first to avoid wasting time and compute cost, then run broader validation when needed.
- Do not reinstall dependencies unless the existing `.venv` is missing a required package or dependency changes were made.

## Project notes

- Python package code lives under `src/vmss_metrics_exporter/`.
- Tests live under `tests/`.
- Kubernetes, Grafana, and alerting manifests live under `deploy/`.
- Pressure-test scripts live under `scripts/`.
