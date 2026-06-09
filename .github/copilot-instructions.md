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

- Always build container images as multi-arch (`linux/amd64,linux/arm64`) using `make image-multiarch TAG=<tag>`. AKS nodes are `amd64`, so a single-arch image built on an `aarch64` host (or vice versa) will fail to pull with `no match for platform in manifest`.
- A `docker-container` buildx builder is required for true multi-arch builds; create it once with `docker buildx create --name multi --use --driver docker-container`.
- If the `multi` docker-container builder repeatedly fails while resolving Docker Hub base-image metadata or auth tokens, use the verified default-builder fallback through the Makefile instead of switching to single-arch builds:
  - `make image-multiarch TAG=<tag> DOCKER_BUILD_ARGS='--builder default'`
  - This still builds and pushes a `linux/amd64,linux/arm64` multi-arch manifest. Example verified command: `make image-multiarch TAG=v39-target-operation-split DOCKER_BUILD_ARGS='--builder default'`.
- `make image` builds a single-arch image for the host architecture (use only for local `docker-run` smoke tests, never for AKS deploys).
- `make push` pushes a single-arch image to Docker Hub; `make image-multiarch` builds AND pushes the multi-arch manifest in one step.
- `make deploy` applies the Kubernetes manifests in `deploy/` to the current kubectl context cluster.
- After bumping the image tag, update `deploy/kubernetes.yaml` (`image:` field) and run `make deploy` followed by `make rollout` to wait for the new pods.
## Project notes

- Python package code lives under `src/vmss_metrics_exporter/`.
- Tests live under `tests/`.
- Kubernetes, Grafana, and alerting manifests live under `deploy/`.
- Pressure-test scripts live under `scripts/`.
- C++ simulator code and usage docs for reproducing the UAE `map` Azure Managed Lustre metadata issue live under `simulation/`.

## Simulation workflow

- Keep all C++ simulator source, local simulator Makefiles, and simulator-specific usage docs under `simulation/`.
- The `simulation/` area is for reproducing the UAE `map` Lustre metadata/cache-fill scenario, including cold-cache fan-out, `.lock` file creation/removal, local temp download simulation, and cache writes to a Lustre-backed path.
- Do not place simulator source or docs under `scripts/`; `scripts/` is for operational pressure-test and helper scripts.
- Use safe defaults for simulator examples. Full-scale runs such as 20M files, 260 KiB average file size, 130 pods, and 6 threads per pod must require an explicit guard such as `--force` and should offer a dry-run mode.
- Validate C++ simulator changes from `simulation/` with:
  - `make`
  - `make smoke`
  - `make dry-run-cx` when the full UAE map scenario parameters are relevant.
- Do not commit compiled simulator binaries or generated cache/test data.
## code style
- Follow PEP 8 for Python code style.
- For C++ simulator code, keep it self-contained, prefer standard library facilities, build with the local `simulation/Makefile`, and avoid external dependencies unless they are explicitly required.
- For Kubernetes manifests, follow the standard Kubernetes YAML style and conventions.
- For Grafana dashboards, follow the standard Grafana JSON style and conventions.

- For any new files added to the repository, ensure they are included in the appropriate sections above and follow the relevant style guidelines.

