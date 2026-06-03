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

## Image build and deployment workflow

- make image pushes the image to Docker Hub 
- make deploy applies the Kubernetes manifests in `deploy/` to the current kubectl context cluster
## Project notes

- Python package code lives under `src/vmss_metrics_exporter/`.
- Tests live under `tests/`.
- Kubernetes, Grafana, and alerting manifests live under `deploy/`.
- Pressure-test scripts live under `scripts/`.
## code style
- Follow PEP 8 for Python code style.
- For Kubernetes manifests, follow the standard Kubernetes YAML style and conventions.
- For Grafana dashboards, follow the standard Grafana JSON style and conventions.

- For any new files added to the repository, ensure they are included in the appropriate sections above and follow the relevant style guidelines.

