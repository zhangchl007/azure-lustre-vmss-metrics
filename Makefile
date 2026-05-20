SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

IMAGE ?= zhangchl007/vmss-metrics-exporter
TAG ?= v1
IMAGE_REF := $(IMAGE):$(TAG)
CONTAINER_NAME ?= vmss-metrics-exporter
PORT ?= 8000
SUBSCRIPTION_IDS ?= 00000000-0000-0000-0000-000000000000
KUBE_NAMESPACE ?= default
KUBE_MANIFEST ?= deploy/kubernetes.yaml
PYTHON ?= python
DOCKER ?= docker
KUBECTL ?= kubectl

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make <target> [IMAGE=repo/name] [TAG=tag]\n\nTargets:\n"} /^[a-zA-Z0-9_.-]+:.*##/ {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: install
install: ## Install the package with development dependencies into the active Python environment.
	$(PYTHON) -m pip install -e '.[dev]'

.PHONY: test
test: ## Run unit tests.
	pytest -q

.PHONY: lint
lint: ## Run Ruff lint checks.
	ruff check .

.PHONY: validate
validate: test lint ## Run all local validation checks.

.PHONY: once
once: ## Run one Azure Resource Graph collection and print VMSS counts.
	$(PYTHON) -m vmss_metrics_exporter.main --once

.PHONY: run
run: ## Run the exporter locally from Python.
	$(PYTHON) -m vmss_metrics_exporter.main

.PHONY: image
image: check-docker ## Build the container image.
	$(DOCKER) build -t $(IMAGE_REF) .

.PHONY: image-no-cache
image-no-cache: check-docker ## Build the container image without Docker cache.
	$(DOCKER) build --no-cache -t $(IMAGE_REF) .

.PHONY: push
push: check-docker ## Push the container image.
	$(DOCKER) push $(IMAGE_REF)

.PHONY: docker-run
docker-run: check-docker ## Run the container locally on PORT with SUBSCRIPTION_IDS.
	$(DOCKER) run --rm --name $(CONTAINER_NAME) \
		-p $(PORT):8000 \
		-e AZURE_SUBSCRIPTION_IDS=$(SUBSCRIPTION_IDS) \
		-e PORT=8000 \
		$(IMAGE_REF)

.PHONY: docker-stop
docker-stop: check-docker ## Stop the local exporter container if it is running.
	-$(DOCKER) stop $(CONTAINER_NAME)

.PHONY: deploy
deploy: check-kubectl ## Apply the Kubernetes manifest.
	$(KUBECTL) apply -n $(KUBE_NAMESPACE) -f $(KUBE_MANIFEST)

.PHONY: deploy-image
deploy-image: check-kubectl ## Update the Kubernetes deployment image after applying the manifest.
	$(KUBECTL) set image -n $(KUBE_NAMESPACE) deployment/vmss-metrics-exporter exporter=$(IMAGE_REF)

.PHONY: rollout
rollout: check-kubectl ## Wait for the Kubernetes deployment rollout.
	$(KUBECTL) rollout status -n $(KUBE_NAMESPACE) deployment/vmss-metrics-exporter

.PHONY: logs
logs: check-kubectl ## Tail exporter logs from Kubernetes.
	$(KUBECTL) logs -n $(KUBE_NAMESPACE) -l app.kubernetes.io/name=vmss-metrics-exporter -f

.PHONY: port-forward
port-forward: check-kubectl ## Port-forward the exporter Service to localhost:PORT.
	$(KUBECTL) port-forward -n $(KUBE_NAMESPACE) service/vmss-metrics-exporter $(PORT):8000

.PHONY: av-workload-script
av-workload-script: check-kubectl ## Publish scripts/av_lustre_workload.py as a ConfigMap.
	$(KUBECTL) -n lustre-pressure-test create configmap av-lustre-workload-script \
		--from-file=av_lustre_workload.py=scripts/av_lustre_workload.py \
		--dry-run=client -o yaml | $(KUBECTL) apply -f -

.PHONY: print-image
print-image: ## Print the resolved image reference.
	@echo $(IMAGE_REF)

.PHONY: check-docker
check-docker:
	@$(DOCKER) version >/dev/null 2>&1 || { \
		echo "ERROR: Docker is not available. Install Docker or enable Docker Desktop WSL integration." >&2; \
		exit 1; \
	}

.PHONY: check-kubectl
check-kubectl:
	@command -v $(KUBECTL) >/dev/null 2>&1 || { \
		echo "ERROR: kubectl not found. Install kubectl or set KUBECTL=/path/to/kubectl." >&2; \
		exit 1; \
	}
