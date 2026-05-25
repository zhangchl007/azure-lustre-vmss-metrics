#!/usr/bin/env python3
"""Generator for Scenario G (200-client fill-to-ENOSPC) Kubernetes assets.

Produces two kinds of artifacts:

1. PV + PVC fan-out (mode `pvc-fanout`, default): N pairs of static PVs and
   RWX PVCs, all pointing to the same Azure Managed Lustre filesystem via a
   shared volumeHandle. The PV/PVC names differ (`pv-almfstestcluster02-fill-NNN`
   / `lustre-fill-pvc-NNN`) but every mount lands in the same `lustrefs`
   namespace. Reclaim policy is `Retain` because `Delete` on a static PV with
   a shared volumeHandle would attempt to destroy the AMLFS itself.

2. Single-pod Jobs (mode `jobs`): one Job per pod, each consuming either its
   dedicated PVC (default) or a shared PVC via subPath (fallback when the
   CSI driver rejects duplicate volumeHandle). Each Job runs
   av_lustre_workload.py in MODE=write-only.

See docs/lustre-pressure-test.md § 15 for the runbook context.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from textwrap import dedent

DEFAULT_FILESYSTEM_NAME = "almfstestcluster02"
DEFAULT_STORAGE_CLASS = "sc-almfstestcluster02-static"
DEFAULT_VOLUME_HANDLE = "594308f7-40d4-429d-9120-978be2fab316"
DEFAULT_MGS_IP = "10.10.16.5"
DEFAULT_FS_NAME = "lustrefs"
DEFAULT_CAPACITY = "8.0Ti"
DEFAULT_NAMESPACE = "lustre-pressure-test"
DEFAULT_NODE_POOL = "juicefspool"
DEFAULT_CONFIGMAP = "av-lustre-workload-config"
DEFAULT_SCRIPT_CONFIGMAP = "av-lustre-workload-script"
DEFAULT_IMAGE = "python:3.12-slim"
DEFAULT_SHARED_PVC = "lustre-pressure-test-pvc"
DEFAULT_RESULT_ROOT = "/mnt/lustre/pressure-tests/fill-results"


def _pv_block(index: int, *, capacity: str, storage_class: str, volume_handle: str,
              mgs_ip: str, fs_name: str, filesystem_name: str) -> str:
    name = f"pv-{filesystem_name}-fill-{index:03d}"
    return dedent(f"""\
        ---
        apiVersion: v1
        kind: PersistentVolume
        metadata:
          name: {name}
          labels:
            app.kubernetes.io/name: lustre-pressure-test
            app.kubernetes.io/component: fill
            app.kubernetes.io/part-of: azure-managed-lustre-pressure-test
            azure.workload/resource-name: {filesystem_name}
        spec:
          accessModes:
            - ReadWriteMany
          capacity:
            storage: {capacity}
          csi:
            driver: azurelustre.csi.azure.com
            volumeAttributes:
              fs-name: {fs_name}
              mgs-ip-address: {mgs_ip}
            volumeHandle: {volume_handle}
          mountOptions:
            - noatime
            - flock
          persistentVolumeReclaimPolicy: Retain
          storageClassName: {storage_class}
          claimRef:
            namespace: {DEFAULT_NAMESPACE}
            name: lustre-fill-pvc-{index:03d}
        """)


def _pvc_block(index: int, *, capacity: str, storage_class: str,
               filesystem_name: str) -> str:
    pv = f"pv-{filesystem_name}-fill-{index:03d}"
    name = f"lustre-fill-pvc-{index:03d}"
    return dedent(f"""\
        ---
        apiVersion: v1
        kind: PersistentVolumeClaim
        metadata:
          name: {name}
          namespace: {DEFAULT_NAMESPACE}
          labels:
            app.kubernetes.io/name: lustre-pressure-test
            app.kubernetes.io/component: fill
            app.kubernetes.io/part-of: azure-managed-lustre-pressure-test
        spec:
          accessModes:
            - ReadWriteMany
          volumeName: {pv}
          storageClassName: {storage_class}
          resources:
            requests:
              storage: {capacity}
        """)


def _job_block(index: int, *, run_id: str, target_bytes: int, file_size_bytes: str,
               fanout: int, image: str, configmap: str, script_configmap: str,
               result_root: str, pvc_mode: str, shared_pvc: str) -> str:
    """Render one single-pod Job for pod index `index`.

    pvc_mode = "dedicated"          -> mount lustre-fill-pvc-NNN at /mnt/lustre.
                                       Pod's RESULT_ROOT remains shared.
    pvc_mode = "dedicated-isolated" -> mount lustre-fill-pvc-NNN at /mnt/lustre
                                       with subPath isolation. Pod sees only
                                       its own subdir; DATASET_ROOT is
                                       overridden so disjoint-roots passes.
    pvc_mode = "subpath"            -> mount shared_pvc at /mnt/lustre with
                                       subPath isolation. Same view as
                                       dedicated-isolated but uses one PVC.
    """
    job_name = f"lustre-fill-{index:03d}"
    dataset_root_override = None
    if pvc_mode == "dedicated":
        pvc_claim_block = (
            f"        - name: lustre\n"
            f"          persistentVolumeClaim:\n"
            f"            claimName: lustre-fill-pvc-{index:03d}"
        )
        volume_mount_block = "            - name: lustre\n              mountPath: /mnt/lustre"
        pod_result_root = result_root
    elif pvc_mode == "dedicated-isolated":
        subpath = f"pressure-tests/fill-results/{run_id}/lustre-fill-{index:03d}"
        pvc_claim_block = (
            f"        - name: lustre\n"
            f"          persistentVolumeClaim:\n"
            f"            claimName: lustre-fill-pvc-{index:03d}"
        )
        volume_mount_block = (
            "            - name: lustre\n"
            "              mountPath: /mnt/lustre\n"
            f"              subPath: {subpath}"
        )
        # With subPath, the pod's view of /mnt/lustre is its own subtree only.
        # The dataset is invisible from inside the pod, but the workload still
        # runs assert_disjoint_roots(DATASET_ROOT, RESULT_ROOT). Override
        # DATASET_ROOT to a clearly-disjoint absolute path so the check passes.
        pod_result_root = "/mnt/lustre"
        dataset_root_override = "/no-dataset"
    else:
        subpath = f"pressure-tests/fill-results/{run_id}/lustre-fill-{index:03d}"
        pvc_claim_block = (
            f"        - name: lustre\n"
            f"          persistentVolumeClaim:\n"
            f"            claimName: {shared_pvc}"
        )
        volume_mount_block = (
            "            - name: lustre\n"
            "              mountPath: /mnt/lustre\n"
            f"              subPath: {subpath}"
        )
        pod_result_root = "/mnt/lustre"
        dataset_root_override = "/no-dataset"

    if dataset_root_override is None:
        dataset_root_env = (
            "            - name: DATASET_ROOT\n"
            "              valueFrom:\n"
            "                configMapKeyRef:\n"
            f"                  name: {configmap}\n"
            "                  key: DATASET_ROOT\n"
        )
    else:
        dataset_root_env = (
            "            - name: DATASET_ROOT\n"
            f"              value: \"{dataset_root_override}\"\n"
        )

    return (
        "---\n"
        "apiVersion: batch/v1\n"
        "kind: Job\n"
        "metadata:\n"
        f"  name: {job_name}\n"
        f"  namespace: {DEFAULT_NAMESPACE}\n"
        "  labels:\n"
        "    app.kubernetes.io/name: lustre-fill\n"
        "    app.kubernetes.io/component: fill\n"
        "    app.kubernetes.io/part-of: azure-managed-lustre-pressure-test\n"
        f"    scenario-g/run-id: \"{run_id}\"\n"
        f"    scenario-g/pod-index: \"{index:03d}\"\n"
        "spec:\n"
        "  parallelism: 1\n"
        "  completions: 1\n"
        "  backoffLimit: 0\n"
        "  ttlSecondsAfterFinished: 86400\n"
        "  activeDeadlineSeconds: 14400\n"
        "  template:\n"
        "    metadata:\n"
        "      labels:\n"
        "        app.kubernetes.io/name: lustre-fill\n"
        "        app.kubernetes.io/component: fill\n"
        f"        scenario-g/run-id: \"{run_id}\"\n"
        "    spec:\n"
        "      restartPolicy: Never\n"
        "      nodeSelector:\n"
        f"        kubernetes.azure.com/agentpool: {DEFAULT_NODE_POOL}\n"
        "      terminationGracePeriodSeconds: 30\n"
        "      containers:\n"
        "        - name: av-workload\n"
        f"          image: {image}\n"
        "          imagePullPolicy: IfNotPresent\n"
        "          env:\n"
        "            - name: MODE\n"
        "              value: write-only\n"
        "            - name: POD_NAME\n"
        "              valueFrom:\n"
        "                fieldRef:\n"
        "                  fieldPath: metadata.name\n"
        "            - name: POD_COUNT\n"
        "              value: \"200\"\n"
        "            - name: JOB_COMPLETION_INDEX\n"
        f"              value: \"{index}\"\n"
        "            - name: RUN_ID\n"
        f"              value: \"{run_id}\"\n"
        f"{dataset_root_env}"
        "            - name: RESULT_ROOT\n"
        f"              value: \"{pod_result_root}\"\n"
        "            - name: WRITE_ONLY_FILE_SIZE_BYTES\n"
        f"              value: \"{file_size_bytes}\"\n"
        "            - name: WRITE_ONLY_TARGET_BYTES_PER_POD\n"
        f"              value: \"{target_bytes}\"\n"
        "            - name: WRITE_ONLY_DIR_FANOUT\n"
        f"              value: \"{fanout}\"\n"
        "            - name: WRITE_ONLY_CHUNK_SIZE_BYTES\n"
        "              value: \"4MiB\"\n"
        "            - name: WARMUP_SECONDS\n"
        "              value: \"30\"\n"
        "            - name: STATS_INTERVAL_SECONDS\n"
        "              value: \"30\"\n"
        "            - name: FAIL_ON_WRITE_ERROR\n"
        "              value: \"true\"\n"
        "            - name: MAX_RECORDED_ERRORS\n"
        "              value: \"20\"\n"
        "          command:\n"
        "            - python\n"
        "            - /opt/av/av_lustre_workload.py\n"
        "          resources:\n"
        "            requests:\n"
        "              cpu: 200m\n"
        "              memory: 512Mi\n"
        "            limits:\n"
        "              cpu: \"2\"\n"
        "              memory: 2Gi\n"
        "          volumeMounts:\n"
        f"{volume_mount_block}\n"
        "            - name: av-script\n"
        "              mountPath: /opt/av\n"
        "              readOnly: true\n"
        "      volumes:\n"
        f"{pvc_claim_block}\n"
        "        - name: av-script\n"
        "          configMap:\n"
        f"            name: {script_configmap}\n"
        "            defaultMode: 493\n"
    )


def render_pvc_fanout(*, count: int, capacity: str, storage_class: str,
                      volume_handle: str, mgs_ip: str, fs_name: str,
                      filesystem_name: str) -> str:
    header_line_a = (
        "# Generated by scripts/gen_lustre_fill_pvcs.py for Scenario G "
        "(docs/lustre-pressure-test.md § 15)."
    )
    header_line_b = (
        "# Do not hand-edit \u2014 regenerate with "
        f"`python scripts/gen_lustre_fill_pvcs.py --count {count} --out <path>`."
    )
    storage_class_block = dedent(f"""\
        {header_line_a}
        {header_line_b}
        ---
        apiVersion: storage.k8s.io/v1
        kind: StorageClass
        metadata:
          name: {storage_class}
          labels:
            app.kubernetes.io/name: lustre-pressure-test
            app.kubernetes.io/component: fill
            app.kubernetes.io/part-of: azure-managed-lustre-pressure-test
        provisioner: azurelustre.csi.azure.com
        parameters:
          fs-name: {fs_name}
          mgs-ip-address: {mgs_ip}
        reclaimPolicy: Retain
        volumeBindingMode: Immediate
        mountOptions:
          - noatime
          - flock
        """)
    parts = [storage_class_block]
    for i in range(count):
        parts.append(_pv_block(i, capacity=capacity, storage_class=storage_class,
                               volume_handle=volume_handle, mgs_ip=mgs_ip,
                               fs_name=fs_name, filesystem_name=filesystem_name))
        parts.append(_pvc_block(i, capacity=capacity, storage_class=storage_class,
                                filesystem_name=filesystem_name))
    return "".join(parts)


def render_jobs(*, count: int, run_id: str, target_bytes: int,
                file_size_bytes: str, fanout: int, image: str,
                configmap: str, script_configmap: str, result_root: str,
                pvc_mode: str, shared_pvc: str) -> str:
    header = dedent(f"""\
        # Generated by scripts/gen_lustre_fill_pvcs.py for Scenario G (RUN_ID={run_id}).
        # PVC mode: {pvc_mode}
        # Per-pod target_bytes: {target_bytes}
        """)
    parts = [header]
    for i in range(count):
        parts.append(_job_block(i, run_id=run_id, target_bytes=target_bytes,
                                file_size_bytes=file_size_bytes, fanout=fanout,
                                image=image, configmap=configmap,
                                script_configmap=script_configmap,
                                result_root=result_root, pvc_mode=pvc_mode,
                                shared_pvc=shared_pvc))
    return "".join(parts)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--count", type=int, default=200,
                   help="Number of PVCs (and Jobs) to generate.")
    p.add_argument("--jobs", action="store_true",
                   help="Generate single-pod Jobs instead of PV/PVC fan-out.")
    p.add_argument("--mode", choices=("dedicated", "dedicated-isolated", "subpath"),
                   default="dedicated",
                   help="PVC mode for --jobs. 'dedicated-isolated' adds per-pod "
                        "subPath on top of the dedicated PVC so the pod's view "
                        "of /mnt/lustre is its own subdir only.")
    p.add_argument("--shared-pvc", default=DEFAULT_SHARED_PVC,
                   help="Shared PVC name for --mode=subpath.")
    p.add_argument("--run-id", default="fill-200-default",
                   help="RUN_ID for --jobs.")
    p.add_argument("--target-bytes-per-pod", type=int, default=42949672960,
                   help="WRITE_ONLY_TARGET_BYTES_PER_POD value (default ~40 GiB).")
    p.add_argument("--file-size-bytes", default="64MiB",
                   help="WRITE_ONLY_FILE_SIZE_BYTES value.")
    p.add_argument("--dir-fanout", type=int, default=1024,
                   help="WRITE_ONLY_DIR_FANOUT value.")
    p.add_argument("--capacity", default=DEFAULT_CAPACITY)
    p.add_argument("--storage-class", default=DEFAULT_STORAGE_CLASS)
    p.add_argument("--volume-handle", default=DEFAULT_VOLUME_HANDLE)
    p.add_argument("--mgs-ip", default=DEFAULT_MGS_IP)
    p.add_argument("--fs-name", default=DEFAULT_FS_NAME)
    p.add_argument("--filesystem-name", default=DEFAULT_FILESYSTEM_NAME)
    p.add_argument("--image", default=DEFAULT_IMAGE)
    p.add_argument("--configmap", default=DEFAULT_CONFIGMAP)
    p.add_argument("--script-configmap", default=DEFAULT_SCRIPT_CONFIGMAP)
    p.add_argument("--result-root", default=DEFAULT_RESULT_ROOT)
    p.add_argument("--out", required=True, help="Output YAML path.")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.count < 1 or args.count > 1000:
        print("--count must be between 1 and 1000", file=sys.stderr)
        return 2
    if args.jobs:
        body = render_jobs(
            count=args.count,
            run_id=args.run_id,
            target_bytes=args.target_bytes_per_pod,
            file_size_bytes=args.file_size_bytes,
            fanout=args.dir_fanout,
            image=args.image,
            configmap=args.configmap,
            script_configmap=args.script_configmap,
            result_root=args.result_root,
            pvc_mode=args.mode,
            shared_pvc=args.shared_pvc,
        )
    else:
        body = render_pvc_fanout(
            count=args.count,
            capacity=args.capacity,
            storage_class=args.storage_class,
            volume_handle=args.volume_handle,
            mgs_ip=args.mgs_ip,
            fs_name=args.fs_name,
            filesystem_name=args.filesystem_name,
        )
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(body)
    print(f"wrote {out_path} ({len(body)} bytes, {args.count} entries)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
