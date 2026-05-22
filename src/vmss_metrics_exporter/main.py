"""Command-line entry point for the VMSS metrics exporter."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
from contextlib import suppress

from prometheus_client import start_http_server

from .azure_managed_lustre import (
    AzureManagedLustreCollector,
    MetricsQueryClientProtocol,
    create_metrics_query_client,
    summarize_lustre_metrics,
)
from .azure_resource_graph import (
    AzureResourceGraphVmssCollector,
    ResourceGraphClientProtocol,
    create_resource_graph_client,
    summarize_counts,
)
from .collector import VmssMetricsExporter
from .config import Settings, load_settings
from .credentials import create_credential
from .leader_election import (
    LeaderElectionConfig,
    LeaderElectionRunner,
    load_incluster_kube_config,
)

LOGGER = logging.getLogger(__name__)
LEADER_SERVICE_LABEL = "vmss-metrics-exporter-leader"


def main(argv: list[str] | None = None) -> int:
    """Run the exporter or perform a one-shot collection."""

    parser = argparse.ArgumentParser(description="Export Azure VMSS instance counts to Prometheus.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Collect once, print a tab-separated summary, then exit without starting /metrics.",
    )
    args = parser.parse_args(argv)

    credential: object | None = None
    resource_graph_client: ResourceGraphClientProtocol | None = None
    metrics_query_client: MetricsQueryClientProtocol | None = None
    exporter: VmssMetricsExporter | None = None
    leader_election_runner: LeaderElectionRunner | None = None
    try:
        settings = load_settings(require_subscription_ids=True)
        _configure_logging(settings.log_level)
        credential = create_credential()
        resource_graph_client = create_resource_graph_client(credential)
        collector = AzureResourceGraphVmssCollector(
            resource_graph_client,
            settings.subscription_ids,
            page_size=settings.arg_page_size,
            max_retries=settings.arg_max_retries,
            retry_base_delay_seconds=settings.arg_retry_base_delay_seconds,
        )
        if settings.enable_managed_lustre_metrics:
            metrics_query_client = create_metrics_query_client(credential)
        lustre_collector = _create_lustre_collector(
            settings,
            resource_graph_client,
            metrics_query_client,
        )

        if args.once:
            print(summarize_counts(collector.collect()))
            if lustre_collector is not None:
                print()
                print("Azure Managed Lustre metrics")
                print(summarize_lustre_metrics(lustre_collector.collect()))
            return 0

        exporter = VmssMetricsExporter(
            collector.collect,
            collect_lustre_metrics=lustre_collector.collect if lustre_collector else None,
            poll_interval_seconds=settings.poll_interval_seconds,
            lustre_poll_interval_seconds=settings.lustre_poll_interval_seconds,
            leader_election_enabled=settings.leader_election_enabled,
        )
        leader_election_runner = _start_leader_election(settings, exporter)
        start_http_server(settings.port, addr=settings.host)
        LOGGER.info(
            "VMSS metrics exporter listening on http://%s:%s/metrics; VMSS polling every %ss; "
            "Managed Lustre metrics %s%s; leader election %s",
            settings.host,
            settings.port,
            settings.poll_interval_seconds,
            "enabled" if lustre_collector else "disabled",
            f" every {settings.lustre_poll_interval_seconds}s" if lustre_collector else "",
            "enabled" if leader_election_runner else "disabled",
        )
        exporter.start()
        _wait_for_shutdown_signal()
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # noqa: BLE001 - CLI should present actionable errors.
        logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(name)s: %(message)s")
        LOGGER.exception("Exporter failed to start: %s", exc)
        return 1
    finally:
        if leader_election_runner is not None:
            with suppress(Exception):
                leader_election_runner.release(notify_stopped=False)
        if exporter is not None:
            with suppress(Exception):
                exporter.stop()
        _close_if_supported(metrics_query_client)
        _close_if_supported(resource_graph_client)
        _close_if_supported(credential)


def _create_lustre_collector(
    settings: Settings,
    resource_graph_client: ResourceGraphClientProtocol,
    metrics_query_client: MetricsQueryClientProtocol | None,
) -> AzureManagedLustreCollector | None:
    """Create the Azure Managed Lustre collector when enabled.

    ``metrics_query_client`` must be supplied by the caller so the Azure Monitor
    client shares the same ``ResilientAzureCredential`` as the Resource Graph
    client (see issue #9). Passing ``None`` while Managed Lustre metrics are
    enabled is a programming error.
    """

    if not settings.enable_managed_lustre_metrics:
        return None
    if metrics_query_client is None:
        raise RuntimeError(
            "metrics_query_client is required when enable_managed_lustre_metrics "
            "is True; pass the shared credential-backed client from main()."
        )
    return AzureManagedLustreCollector(
        resource_graph_client,
        metrics_query_client,
        settings.subscription_ids,
        page_size=settings.arg_page_size,
        lookback_minutes=settings.lustre_metrics_lookback_minutes,
        interval=settings.lustre_metrics_interval,
        max_workers=settings.lustre_metrics_max_workers,
        request_jitter_seconds=settings.lustre_metrics_request_jitter_seconds,
        max_retries=settings.arg_max_retries,
        retry_base_delay_seconds=settings.arg_retry_base_delay_seconds,
    )


def _start_leader_election(
    settings: Settings,
    exporter: VmssMetricsExporter,
) -> LeaderElectionRunner | None:
    """Start the leader-election supervisor in a daemon thread when enabled."""

    if not settings.leader_election_enabled:
        return None
    load_incluster_kube_config()
    config = LeaderElectionConfig(
        lock_name=settings.leader_election_lock_name,
        lock_namespace=settings.leader_election_namespace,
        identity=settings.leader_election_identity,
        lease_duration_seconds=settings.leader_election_lease_duration_seconds,
        renew_deadline_seconds=settings.leader_election_renew_deadline_seconds,
        retry_period_seconds=settings.leader_election_retry_period_seconds,
    )
    runner = LeaderElectionRunner(
        config,
        on_started_leading=lambda: _on_started_leading(settings, exporter),
        on_stopped_leading=lambda: _on_stopped_leading(settings, exporter),
    )
    thread = threading.Thread(
        target=runner.run_forever,
        name="leader-election",
        daemon=True,
    )
    thread.start()
    LOGGER.info(
        "Leader-election supervisor started for lock %s/%s as %s (lease=%ss, renew=%ss, retry=%ss)",
        config.lock_namespace,
        config.lock_name,
        config.identity,
        config.lease_duration_seconds,
        config.renew_deadline_seconds,
        config.retry_period_seconds,
    )
    return runner


def _on_started_leading(settings: Settings, exporter: VmssMetricsExporter) -> None:
    """Promote this pod and make it eligible for Service traffic.

    The Kubernetes Service is intentionally scoped to the leader pod so a
    Service-based scraper never randomly hits an idle follower with zero
    resource series. Populate gauges once before adding the leader label; that
    avoids a short Service-visible blank window on a newly promoted pod.
    """

    exporter.set_leader(True)
    with suppress(Exception):
        exporter.collect_once()
    _patch_pod_leader_label(settings, is_leader=True)


def _on_stopped_leading(settings: Settings, exporter: VmssMetricsExporter) -> None:
    """Demote this pod and remove it from leader-only Service endpoints."""

    _patch_pod_leader_label(settings, is_leader=False)
    exporter.set_leader(False)


def _patch_pod_leader_label(settings: Settings, *, is_leader: bool) -> None:
    """Best-effort patch of this pod's leader Service selector label."""

    try:
        from kubernetes import client

        client.CoreV1Api().patch_namespaced_pod(
            name=settings.leader_election_identity,
            namespace=settings.leader_election_namespace,
            body={
                "metadata": {
                    "labels": {
                        LEADER_SERVICE_LABEL: "true" if is_leader else None,
                    },
                },
            },
            _content_type="application/merge-patch+json",
        )
    except Exception:  # noqa: BLE001 - label updates must not break election callbacks
        LOGGER.exception(
            "Failed to patch pod leader label %s=%s on %s/%s; suppressed",
            LEADER_SERVICE_LABEL,
            "true" if is_leader else "<removed>",
            settings.leader_election_namespace,
            settings.leader_election_identity,
        )


def _configure_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if level > logging.DEBUG:
        logging.getLogger("azure").setLevel(logging.WARNING)


def _close_if_supported(resource: object | None) -> None:
    """Best-effort close for Azure SDK clients and credentials."""

    if resource is None:
        return
    close = getattr(resource, "close", None)
    if callable(close):
        with suppress(Exception):
            close()


def _wait_for_shutdown_signal() -> None:
    stop_event = threading.Event()

    def _request_stop(signum: int, _frame: object) -> None:
        LOGGER.info("Received signal %s; shutting down", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    stop_event.wait()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
