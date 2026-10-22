"""Kubernetes access: kubeconfig sources, a resource registry, raw REST calls, row summaries and workload actions.

All API calls go through :func:`request`, which talks to the API server with plain JSON. That keeps one generic
code path for every resource kind and gives the UI exactly what ``kubectl get -o yaml`` would show.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import gevent
import urllib3
import yaml
from kubernetes import client, config

log = logging.getLogger(__name__)

MAX_KUBECONFIG_BYTES = 1_000_000
MAX_YAML_BYTES = 1_000_000
SECRET_MASK = "<hidden by KubeLite>"
NAME_RE = re.compile(r"^[a-z0-9]([-a-z0-9.]{0,251}[a-z0-9])?$")
RESTARTED_AT = "kubectl.kubernetes.io/restartedAt"


class K8sError(Exception):
    """An error with an HTTP status and a message that is safe to show in the UI."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class Kind:
    group: str
    version: str
    plural: str
    namespaced: bool
    kind: str

    @property
    def api_version(self) -> str:
        return f"{self.group}/{self.version}" if self.group else self.version


KINDS: dict[str, Kind] = {
    k.plural: k
    for k in [
        Kind("", "v1", "pods", True, "Pod"),
        Kind("apps", "v1", "deployments", True, "Deployment"),
        Kind("apps", "v1", "statefulsets", True, "StatefulSet"),
        Kind("apps", "v1", "daemonsets", True, "DaemonSet"),
        Kind("apps", "v1", "replicasets", True, "ReplicaSet"),
        Kind("batch", "v1", "jobs", True, "Job"),
        Kind("batch", "v1", "cronjobs", True, "CronJob"),
        Kind("", "v1", "services", True, "Service"),
        Kind("networking.k8s.io", "v1", "ingresses", True, "Ingress"),
        Kind("networking.k8s.io", "v1", "networkpolicies", True, "NetworkPolicy"),
        Kind("", "v1", "configmaps", True, "ConfigMap"),
        Kind("", "v1", "secrets", True, "Secret"),
        Kind("", "v1", "persistentvolumeclaims", True, "PersistentVolumeClaim"),
        Kind("", "v1", "serviceaccounts", True, "ServiceAccount"),
        Kind("autoscaling", "v2", "horizontalpodautoscalers", True, "HorizontalPodAutoscaler"),
        Kind("", "v1", "events", True, "Event"),
        Kind("", "v1", "nodes", False, "Node"),
        Kind("", "v1", "namespaces", False, "Namespace"),
        Kind("", "v1", "persistentvolumes", False, "PersistentVolume"),
        Kind("storage.k8s.io", "v1", "storageclasses", False, "StorageClass"),
    ]
}

# Only namespaced workload resources can be changed. Cluster-scoped kinds are view-only by design.
WRITABLE = {p for p, k in KINDS.items() if k.namespaced and p != "events"}
SCALABLE = {"deployments", "statefulsets", "replicasets"}
RESTARTABLE = {"deployments", "statefulsets", "daemonsets"}
HAS_PODS = {"deployments", "statefulsets", "daemonsets", "replicasets", "jobs"}
GRAPH_KINDS = frozenset(KINDS.keys())
_VOLUME_WORKLOADS = ("deployments", "statefulsets", "daemonsets", "jobs", "cronjobs")
_OWNER_PLURAL = {
    "Deployment": "deployments",
    "ReplicaSet": "replicasets",
    "StatefulSet": "statefulsets",
    "DaemonSet": "daemonsets",
    "Job": "jobs",
    "CronJob": "cronjobs",
}
_HPA_TARGET_PLURAL = {
    "Deployment": "deployments",
    "StatefulSet": "statefulsets",
    "ReplicaSet": "replicasets",
}
_NAMESPACE_MAP_KINDS = (
    ("deployments", "Deployments"),
    ("statefulsets", "StatefulSets"),
    ("daemonsets", "DaemonSets"),
    ("services", "Services"),
    ("ingresses", "Ingress"),
    ("pods", "Pods"),
    ("configmaps", "ConfigMaps"),
    ("secrets", "Secrets"),
    ("persistentvolumeclaims", "PVCs"),
    ("horizontalpodautoscalers", "Autoscalers"),
)


def get_kind(plural: str) -> Kind:
    kind = KINDS.get(plural)
    if kind is None:
        raise K8sError(404, f"Unknown resource type: {plural}")
    return kind


def check_name(value: str | None, what: str = "name") -> str:
    if not value or not NAME_RE.match(value):
        raise K8sError(400, f"Invalid {what}: {value!r}")
    return value


def resource_path(kind: Kind, namespace: str | None = None, name: str | None = None, sub: str | None = None) -> str:
    """Build an API path. ``namespace=None`` on a namespaced kind means "all namespaces" (list/watch only)."""
    path = f"/apis/{kind.group}/{kind.version}" if kind.group else "/api/v1"
    if kind.namespaced and namespace:
        path += f"/namespaces/{quote(check_name(namespace, 'namespace'))}"
    path += f"/{kind.plural}"
    if name:
        path += f"/{quote(check_name(name))}"
    if sub:
        path += f"/{sub}"
    return path


# --------------------------------------------------------------------------------------------------------------------
# Kubeconfig sources and API clients
# --------------------------------------------------------------------------------------------------------------------


def _stable_id(seed: str) -> str:
    """Same kubeconfig -> same id across restarts, so saved namespaces and shared log links keep working."""
    return hashlib.sha256(seed.encode()).hexdigest()[:8]


def parse_kubeconfig(text: str) -> dict:
    if len(text.encode()) > MAX_KUBECONFIG_BYTES:
        raise K8sError(413, "Kubeconfig is larger than 1 MB")
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise K8sError(400, f"Not valid YAML: {exc}") from None
    if not isinstance(data, dict) or not data.get("contexts") or not data.get("clusters"):
        raise K8sError(400, "This does not look like a kubeconfig file (no contexts or clusters found)")
    return data


@dataclass
class Source:
    id: str
    label: str
    config: dict
    path: Path | None  # set when loaded from disk, so relative certificate paths resolve


class ClusterManager:
    """Holds loaded kubeconfigs and caches one API client per (source, context)."""

    def __init__(self, store_dir: Path) -> None:
        self.store_dir = store_dir
        self.sources: dict[str, Source] = {}
        self._clients: dict[tuple[str, str], client.ApiClient] = {}

    def load_file(self, path: Path, label: str | None = None) -> Source:
        cfg = parse_kubeconfig(path.read_text(encoding="utf-8"))
        for src in self.sources.values():
            if src.path and src.path.resolve() == path.resolve():
                return src
        src = Source(_stable_id(str(path.resolve())), label or str(path), cfg, path)
        self.sources[src.id] = src
        return src

    def load_remembered(self) -> None:
        if self.store_dir.is_dir():
            for path in sorted(self.store_dir.glob("*.yaml")):
                try:
                    self.load_file(path, label=f"{path.stem} (remembered)")
                except (K8sError, OSError) as exc:
                    log.warning("Skipping remembered kubeconfig %s: %s", path.name, exc)

    def add_upload(self, text: str, label: str, remember: bool) -> Source:
        cfg = parse_kubeconfig(text)
        label = re.sub(r"[^\w.\- ]", "_", label)[:80] or "uploaded"
        src_id = _stable_id(text)
        if src_id in self.sources:
            return self.sources[src_id]
        path = None
        if remember:
            self.store_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            path = self.store_dir / f"{src_id}-{re.sub(r'[^A-Za-z0-9_-]', '_', label)[:40]}.yaml"
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(text)
            except FileExistsError:  # same content and label already remembered
                pass
        src = Source(src_id, f"{label} (remembered)" if remember else f"{label} (this session)", cfg, path)
        self.sources[src.id] = src
        return src

    def remove(self, source_id: str) -> None:
        src = self.sources.pop(source_id, None)
        if src is None:
            raise K8sError(404, "Unknown kubeconfig")
        for key in [k for k in self._clients if k[0] == source_id]:
            self._clients.pop(key).close()
        if src.path and src.path.parent == self.store_dir:
            src.path.unlink(missing_ok=True)

    def contexts(self) -> list[dict]:
        out = []
        for src in self.sources.values():
            for ctx in src.config.get("contexts") or []:
                details = ctx.get("context") or {}
                out.append(
                    {
                        "id": f"{src.id}|{ctx.get('name')}",
                        "source": src.id,
                        "sourceLabel": src.label,
                        "name": ctx.get("name"),
                        "cluster": details.get("cluster"),
                        "namespace": details.get("namespace") or "default",
                        "current": ctx.get("name") == src.config.get("current-context"),
                        "removable": src.path is None or src.path.parent == self.store_dir,
                    }
                )
        return out

    def api(self, ctx_id: str | None) -> client.ApiClient:
        if not ctx_id or "|" not in ctx_id:
            raise K8sError(400, "No cluster selected")
        source_id, context = ctx_id.split("|", 1)
        cached = self._clients.get((source_id, context))
        if cached is not None:
            return cached
        src = self.sources.get(source_id)
        if src is None:
            raise K8sError(404, "That kubeconfig is no longer loaded")
        cfg = client.Configuration()
        try:
            if src.path:
                config.load_kube_config(
                    config_file=str(src.path), context=context, client_configuration=cfg, persist_config=False
                )
            else:
                config.load_kube_config_from_dict(
                    src.config, context=context, client_configuration=cfg, persist_config=False
                )
        except Exception as exc:  # the loader raises many types, including auth plugin failures
            raise K8sError(400, f"Could not load context {context!r}: {exc}") from None
        cfg.connection_pool_maxsize = 64  # long-lived log/watch streams each hold a connection
        api = client.ApiClient(cfg)
        self._clients[(source_id, context)] = api
        return api


# --------------------------------------------------------------------------------------------------------------------
# Raw REST calls
# --------------------------------------------------------------------------------------------------------------------


def request(
    api: client.ApiClient,
    method: str,
    path: str,
    query: list[tuple[str, str]] | None = None,
    body: Any = None,
    content_type: str | None = None,
    stream: bool = False,
    timeout: Any = 30,
) -> Any:
    """Call the API server. Returns parsed JSON, or the raw urllib3 response when ``stream`` is true."""
    headers = {"Accept": "application/json"}
    if content_type or body is not None:
        headers["Content-Type"] = content_type or "application/json"
    try:
        m, url, h, b, post = api.param_serialize(
            method, path, query_params=query or [], header_params=headers, body=body, auth_settings=["BearerToken"]
        )
        resp = api.call_api(m, url, header_params=h, body=b, post_params=post, _request_timeout=timeout)
    except urllib3.exceptions.HTTPError as exc:
        raise K8sError(502, f"Cannot reach the cluster: {exc}") from None
    except Exception as exc:  # e.g. exec auth plugin missing or failing
        raise K8sError(502, f"Request to the cluster failed: {exc}") from None
    if resp.status >= 400:
        raise K8sError(resp.status, friendly_api_error(resp.status, resp.read()))
    if stream:
        return resp.response
    data = resp.read()
    return json.loads(data) if data else None


def _api_message(data: bytes, status: int) -> str:
    try:
        return json.loads(data).get("message") or f"HTTP {status}"
    except (ValueError, AttributeError):
        return data.decode(errors="replace")[:300] or f"HTTP {status}"


def friendly_api_error(status: int, data: bytes) -> str:
    """Turn Kubernetes API errors into short text safe to show in a toast."""
    raw = _api_message(data, status)
    low = raw.lower()
    if status == 401:
        return (
            "The cluster rejected your credentials. Log in again for this context "
            "(for example aws sso login, gcloud auth login, or az aks get-credentials)."
        )
    if status == 403:
        if "cannot list" in low:
            return "You don't have permission to list this resource in this namespace."
        if "cannot get" in low or "cannot watch" in low:
            return "You don't have permission to view this resource."
        if "cannot create" in low:
            return "You don't have permission to create this resource."
        if "cannot update" in low or "cannot patch" in low:
            return "You don't have permission to change this resource."
        if "cannot delete" in low:
            return "You don't have permission to delete this resource."
        if "forbidden" in low or "is forbidden" in low:
            return f"You don't have access. {raw}" if raw != f"HTTP {status}" else "You don't have permission to perform this action."
        return raw or "You don't have permission to perform this action."
    if status == 404:
        return raw if raw != f"HTTP {status}" else "That resource was not found (it may have been deleted)."
    return raw


def get_object(api: client.ApiClient, plural: str, namespace: str | None, name: str) -> dict:
    return request(api, "GET", resource_path(get_kind(plural), namespace, name))


def list_objects(api: client.ApiClient, plural: str, namespace: str | None, **params: str) -> dict:
    query = [(k, v) for k, v in params.items() if v]
    return request(api, "GET", resource_path(get_kind(plural), namespace), query=query, timeout=60)


# --------------------------------------------------------------------------------------------------------------------
# Status and row summaries
# --------------------------------------------------------------------------------------------------------------------


def pod_status(pod: dict) -> str:
    """The STATUS column exactly as ``kubectl get pods`` derives it (phase alone hides crash loops)."""
    meta, status = pod.get("metadata") or {}, pod.get("status") or {}
    reason = status.get("reason") or status.get("phase") or "Unknown"
    init_statuses = status.get("initContainerStatuses") or []
    for i, cs in enumerate(init_statuses):
        state = cs.get("state") or {}
        term, wait = state.get("terminated"), state.get("waiting")
        if term and term.get("exitCode") == 0:
            continue
        if term:
            reason = "Init:" + (
                term.get("reason")
                or (f"Signal:{term['signal']}" if term.get("signal") else f"ExitCode:{term.get('exitCode')}")
            )
        elif wait and wait.get("reason") and wait["reason"] != "PodInitializing":
            reason = "Init:" + wait["reason"]
        else:
            reason = f"Init:{i}/{len(init_statuses)}"
        break
    else:
        has_running = False
        for cs in reversed(status.get("containerStatuses") or []):
            state = cs.get("state") or {}
            wait, term = state.get("waiting"), state.get("terminated")
            if wait and wait.get("reason"):
                reason = wait["reason"]
            elif term and term.get("reason"):
                reason = term["reason"]
            elif term:
                reason = f"Signal:{term['signal']}" if term.get("signal") else f"ExitCode:{term.get('exitCode')}"
            elif cs.get("ready") and state.get("running"):
                has_running = True
        if reason == "Completed" and has_running:
            ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions") or [])
            reason = "Running" if ready else "NotReady"
    if meta.get("deletionTimestamp"):
        reason = "Unknown" if status.get("reason") == "NodeLost" else "Terminating"
    return reason


def _tone_for_pod(status: str, ready: int, total: int) -> str:
    if status == "Running":
        return "ok" if ready == total else "warn"
    if status in ("Completed", "Succeeded"):
        return "muted"
    if status in ("Pending", "ContainerCreating", "PodInitializing", "Terminating", "NotReady") or re.match(
        r"^Init:\d+/\d+$", status
    ):
        return "warn"
    return "bad"


def _age_source(obj: dict) -> str | None:
    return (obj.get("metadata") or {}).get("creationTimestamp")


def _selector_string(selector: dict | None) -> str:
    labels = (selector or {}).get("matchLabels") or {}
    return ",".join(f"{k}={v}" for k, v in labels.items())


def _replica_tone(ready: int, desired: int) -> str:
    if desired == 0 or ready >= desired:
        return "ok" if desired else "muted"
    return "bad" if ready == 0 else "warn"


COLUMNS: dict[str, list[str]] = {
    "pods": ["Ready", "Restarts", "Node", "IP"],
    "deployments": ["Ready", "Up-to-date", "Available", "Images"],
    "statefulsets": ["Ready", "Images"],
    "daemonsets": ["Desired", "Current", "Ready", "Images"],
    "replicasets": ["Desired", "Current", "Ready"],
    "jobs": ["Completions", "Duration"],
    "cronjobs": ["Schedule", "Suspended", "Active", "Last run"],
    "services": ["Type", "Cluster IP", "Ports"],
    "ingresses": ["Class", "Hosts", "Address"],
    "networkpolicies": ["Pod selector"],
    "configmaps": ["Keys"],
    "secrets": ["Type", "Keys"],
    "persistentvolumeclaims": ["Volume", "Capacity", "Storage class"],
    "serviceaccounts": [],
    "horizontalpodautoscalers": ["Target", "Min", "Max", "Replicas"],
    "events": ["Type", "Reason", "Object", "Message", "Count"],
    "nodes": ["Roles", "Version", "Internal IP"],
    "namespaces": [],
    "persistentvolumes": ["Capacity", "Access", "Claim", "Storage class"],
    "storageclasses": ["Provisioner", "Reclaim", "Default"],
}


def summarize(plural: str, obj: dict) -> dict:
    """One table row: identity, status, tone (ok|warn|bad|muted) and the kind-specific cells in COLUMNS order."""
    meta = obj.get("metadata") or {}
    spec, st = obj.get("spec") or {}, obj.get("status") or {}
    row: dict[str, Any] = {
        "uid": meta.get("uid"),
        "name": meta.get("name"),
        "namespace": meta.get("namespace"),
        "created": _age_source(obj),
        "labels": meta.get("labels") or {},
        "status": "",
        "tone": "muted",
        "cells": [],
    }
    def images(s):
        return ", ".join(
            c.get("image", "") for c in ((s.get("template") or {}).get("spec") or {}).get("containers") or []
        )

    if plural == "pods":
        statuses = st.get("containerStatuses") or []
        containers = spec.get("containers") or []
        ready = sum(1 for c in statuses if c.get("ready"))
        restarts = sum(c.get("restartCount", 0) for c in statuses)
        row["status"] = pod_status(obj)
        row["tone"] = _tone_for_pod(row["status"], ready, len(containers))
        row["cells"] = [f"{ready}/{len(containers)}", restarts, spec.get("nodeName") or "-", st.get("podIP") or "-"]
        owners = meta.get("ownerReferences") or []
        row["controller"] = next((o.get("kind") for o in owners if o.get("controller")), None)
        row["containers"] = [c["name"] for c in containers]
        row["ports"] = [
            {"container": c["name"], "port": p["containerPort"], "name": p.get("name")}
            for c in containers
            for p in c.get("ports") or []
            if p.get("protocol", "TCP") == "TCP"
        ]
        row["phase"] = st.get("phase")
    elif plural in ("deployments", "statefulsets", "replicasets"):
        desired = spec.get("replicas", 1)
        ready = st.get("readyReplicas", 0)
        row["status"] = f"{ready}/{desired} ready"
        row["tone"] = _replica_tone(ready, desired)
        row["replicas"] = desired
        row["selector"] = _selector_string(spec.get("selector"))
        if plural == "deployments":
            row["cells"] = [
                f"{ready}/{desired}",
                st.get("updatedReplicas", 0),
                st.get("availableReplicas", 0),
                images(spec),
            ]
        elif plural == "statefulsets":
            row["cells"] = [f"{ready}/{desired}", images(spec)]
        else:
            row["cells"] = [desired, st.get("replicas", 0), ready]
    elif plural == "daemonsets":
        desired, ready = st.get("desiredNumberScheduled", 0), st.get("numberReady", 0)
        row["status"] = f"{ready}/{desired} ready"
        row["tone"] = _replica_tone(ready, desired)
        row["selector"] = _selector_string(spec.get("selector"))
        row["cells"] = [desired, st.get("currentNumberScheduled", 0), ready, images(spec)]
    elif plural == "jobs":
        conds = {c.get("type"): c for c in st.get("conditions") or [] if c.get("status") == "True"}
        if "Complete" in conds:
            row["status"], row["tone"] = "Complete", "muted"
        elif "Failed" in conds:
            row["status"], row["tone"] = "Failed", "bad"
        elif spec.get("suspend"):
            row["status"], row["tone"] = "Suspended", "muted"
        else:
            row["status"], row["tone"] = "Running", "ok"
        start, end = st.get("startTime"), st.get("completionTime")
        duration = "-"
        if start:
            secs = int(
                (_parse_time(end) if end else datetime.now(timezone.utc)).timestamp() - _parse_time(start).timestamp()
            )
            duration = _human_seconds(secs)
        row["cells"] = [f"{st.get('succeeded', 0)}/{spec.get('completions', 1)}", duration]
        row["selector"] = _selector_string(spec.get("selector"))
    elif plural == "cronjobs":
        suspended = bool(spec.get("suspend"))
        row["status"], row["tone"] = ("Suspended", "muted") if suspended else ("Active", "ok")
        row["suspended"] = suspended
        row["cells"] = [
            spec.get("schedule"),
            "Yes" if suspended else "No",
            len(st.get("active") or []),
            st.get("lastScheduleTime") or "-",
        ]
    elif plural == "services":
        ports = spec.get("ports") or []
        row["status"], row["tone"] = spec.get("type", "ClusterIP"), "ok"
        row["cells"] = [
            spec.get("type", "ClusterIP"),
            spec.get("clusterIP") or "-",
            ", ".join(f"{p.get('port')}/{p.get('protocol', 'TCP')}" for p in ports) or "-",
        ]
        row["ports"] = [
            {"port": p.get("port"), "name": p.get("name"), "targetPort": p.get("targetPort")} for p in ports
        ]
        row["hasSelector"] = bool(spec.get("selector"))
    elif plural == "ingresses":
        lb = ((st.get("loadBalancer") or {}).get("ingress")) or []
        address = ", ".join(i.get("ip") or i.get("hostname") or "" for i in lb)
        row["status"], row["tone"] = ("Ready", "ok") if address else ("No address", "warn")
        row["cells"] = [
            spec.get("ingressClassName") or "-",
            ", ".join(r.get("host", "*") for r in spec.get("rules") or []) or "*",
            address or "-",
        ]
    elif plural == "networkpolicies":
        row["cells"] = [_selector_string(spec.get("podSelector")) or "(all pods)"]
    elif plural == "configmaps":
        row["cells"] = [len(obj.get("data") or {}) + len(obj.get("binaryData") or {})]
    elif plural == "secrets":
        row["cells"] = [obj.get("type", "Opaque"), len(obj.get("data") or {})]
    elif plural == "persistentvolumeclaims":
        phase = st.get("phase", "Unknown")
        row["status"], row["tone"] = phase, "ok" if phase == "Bound" else "warn"
        row["cells"] = [
            spec.get("volumeName") or "-",
            (st.get("capacity") or {}).get("storage", "-"),
            spec.get("storageClassName") or "-",
        ]
    elif plural == "horizontalpodautoscalers":
        ref = spec.get("scaleTargetRef") or {}
        row["target"] = f"{ref.get('kind')}/{ref.get('name')}"
        row["cells"] = [
            row["target"],
            spec.get("minReplicas", 1),
            spec.get("maxReplicas"),
            st.get("currentReplicas", 0),
        ]
    elif plural == "events":
        involved = obj.get("involvedObject") or {}
        etype = obj.get("type", "Normal")
        row["status"], row["tone"] = etype, "bad" if etype == "Warning" else "muted"
        row["last"] = obj.get("lastTimestamp") or obj.get("eventTime") or _age_source(obj)
        row["involved"] = {
            "kind": involved.get("kind"),
            "name": involved.get("name"),
            "namespace": involved.get("namespace"),
        }
        row["cells"] = [
            etype,
            obj.get("reason"),
            f"{involved.get('kind', '')}/{involved.get('name', '')}",
            obj.get("message", ""),
            obj.get("count", 1),
        ]
    elif plural == "nodes":
        ready = next((c for c in st.get("conditions") or [] if c.get("type") == "Ready"), {})
        is_ready = ready.get("status") == "True"
        row["status"] = ("Ready" if is_ready else "NotReady") + (
            ",SchedulingDisabled" if spec.get("unschedulable") else ""
        )
        row["tone"] = "ok" if is_ready and not spec.get("unschedulable") else "warn" if is_ready else "bad"
        roles = [k.split("/", 1)[1] for k in row["labels"] if k.startswith("node-role.kubernetes.io/")]
        ip = next((a.get("address") for a in st.get("addresses") or [] if a.get("type") == "InternalIP"), "-")
        row["cells"] = [", ".join(roles) or "<none>", (st.get("nodeInfo") or {}).get("kubeletVersion", "-"), ip]
    elif plural == "namespaces":
        phase = st.get("phase", "Active")
        row["status"], row["tone"] = phase, "ok" if phase == "Active" else "warn"
    elif plural == "persistentvolumes":
        phase = st.get("phase", "Unknown")
        row["status"], row["tone"] = phase, "ok" if phase in ("Bound", "Available") else "warn"
        claim = spec.get("claimRef") or {}
        row["cells"] = [
            (spec.get("capacity") or {}).get("storage", "-"),
            ",".join(spec.get("accessModes") or []),
            f"{claim.get('namespace')}/{claim.get('name')}" if claim else "-",
            spec.get("storageClassName") or "-",
        ]
    elif plural == "storageclasses":
        default = (meta.get("annotations") or {}).get("storageclass.kubernetes.io/is-default-class") == "true"
        row["cells"] = [obj.get("provisioner"), obj.get("reclaimPolicy", "Delete"), "Yes" if default else "No"]
    return row


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _human_seconds(secs: int) -> str:
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m{secs % 60}s"
    return f"{secs // 3600}h{(secs % 3600) // 60}m"


# --------------------------------------------------------------------------------------------------------------------
# Object views
# --------------------------------------------------------------------------------------------------------------------


def to_display_yaml(obj: dict, reveal: bool = False) -> str:
    obj = copy.deepcopy(obj)
    (obj.get("metadata") or {}).pop("managedFields", None)
    if obj.get("kind") == "Secret" and not reveal:
        obj["data"] = {k: SECRET_MASK for k in obj.get("data") or {}}
        obj.pop("stringData", None)
    return yaml.safe_dump(obj, sort_keys=False, default_flow_style=False, allow_unicode=True)


def secret_values(api: client.ApiClient, namespace: str, name: str) -> dict[str, str]:
    obj = get_object(api, "secrets", namespace, name)
    out = {}
    for key, value in (obj.get("data") or {}).items():
        raw = base64.b64decode(value)
        try:
            out[key] = raw.decode("utf-8")
        except UnicodeDecodeError:
            out[key] = f"<{len(raw)} bytes of binary data>"
    return out


def events_for(api: client.ApiClient, kind: Kind, namespace: str | None, name: str) -> list[dict]:
    selector = f"involvedObject.name={name},involvedObject.kind={kind.kind}"
    data = list_objects(api, "events", namespace, fieldSelector=selector)
    rows = [summarize("events", e) for e in data.get("items") or []]
    return sorted(rows, key=lambda r: r.get("last") or "", reverse=True)


def pods_for_workload(api: client.ApiClient, plural: str, namespace: str, name: str) -> list[dict]:
    obj = get_object(api, plural, namespace, name)
    selector = _selector_string((obj.get("spec") or {}).get("selector"))
    if not selector:
        raise K8sError(400, "This workload has no label selector")
    pods = list_objects(api, "pods", namespace, labelSelector=selector).get("items") or []
    return [summarize("pods", p) for p in pods]


def _owned_by(obj: dict, kind: str, name: str) -> bool:
    for ref in (obj.get("metadata") or {}).get("ownerReferences") or []:
        if ref.get("kind") == kind and ref.get("name") == name:
            return True
    return False


def _labels_match_selector(labels: dict, selector: dict | None) -> bool:
    if not selector:
        return False
    match = selector.get("matchLabels") if "matchLabels" in (selector or {}) else selector
    if not isinstance(match, dict) or not match:
        return False
    return all(labels.get(k) == v for k, v in match.items())


def _graph_node(plural: str, obj: dict, children: list[dict] | None = None, hint: str = "") -> dict:
    return {"plural": plural, "row": summarize(plural, obj), "children": children or [], "hint": hint}


def _list_pod_objects(api: client.ApiClient, namespace: str, label_selector: str) -> list[dict]:
    if not label_selector:
        return []
    return list_objects(api, "pods", namespace, labelSelector=label_selector).get("items") or []


def _pod_children(api: client.ApiClient, namespace: str, label_selector: str) -> list[dict]:
    return [_graph_node("pods", p) for p in _list_pod_objects(api, namespace, label_selector)]


def _template_labels(obj: dict, plural: str) -> dict:
    spec = obj.get("spec") or {}
    if plural == "cronjobs":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    template = spec.get("template") or {}
    return (template.get("metadata") or {}).get("labels") or {}


def _workload_label_selector(obj: dict, plural: str) -> str:
    spec = obj.get("spec") or {}
    if plural == "cronjobs":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    return _selector_string(spec.get("selector"))


def _services_for_labels(api: client.ApiClient, namespace: str, labels: dict) -> list[dict]:
    if not labels:
        return []
    out: list[dict] = []
    for svc in list_objects(api, "services", namespace).get("items") or []:
        sel = (svc.get("spec") or {}).get("selector") or {}
        if not sel or not _labels_match_selector(labels, sel):
            continue
        pod_sel = _selector_string({"matchLabels": sel})
        out.append(_graph_node("services", svc, _pod_children(api, namespace, pod_sel)))
    return out


def _ingress_service_names(ing: dict) -> list[str]:
    names: list[str] = []
    spec = ing.get("spec") or {}
    for rule in spec.get("rules") or []:
        for path in (rule.get("http") or {}).get("paths") or []:
            svc = (path.get("backend") or {}).get("service") or {}
            if svc.get("name"):
                names.append(svc["name"])
    default = (spec.get("defaultBackend") or {}).get("service") or {}
    if default.get("name"):
        names.append(default["name"])
    return names


def _ingresses_for_services(api: client.ApiClient, namespace: str, service_names: set[str]) -> list[dict]:
    if not service_names:
        return []
    out: list[dict] = []
    for ing in list_objects(api, "ingresses", namespace).get("items") or []:
        refs = set(_ingress_service_names(ing))
        hit = refs & service_names
        if hit:
            out.append(_graph_node("ingresses", ing, hint=", ".join(sorted(hit))))
    return out


def _deployment_tree(api: client.ApiClient, namespace: str, dep: dict) -> dict:
    dep_row = summarize("deployments", dep)
    name = dep_row["name"]
    selector = _workload_label_selector(dep, "deployments")
    pod_objs = _list_pod_objects(api, namespace, selector)
    rs_objs = [
        rs
        for rs in list_objects(api, "replicasets", namespace, labelSelector=selector).get("items") or []
        if _owned_by(rs, "Deployment", name)
    ]
    rs_objs.sort(key=lambda o: (o.get("metadata") or {}).get("creationTimestamp") or "", reverse=True)
    children: list[dict] = []
    claimed: set[str] = set()
    for rs in rs_objs:
        rs_name = (rs.get("metadata") or {}).get("name")
        rs_pods = [p for p in pod_objs if _owned_by(p, "ReplicaSet", rs_name)]
        for p in rs_pods:
            claimed.add((p.get("metadata") or {}).get("uid", ""))
        children.append(_graph_node("replicasets", rs, [_graph_node("pods", p) for p in rs_pods]))
    orphans = [p for p in pod_objs if (p.get("metadata") or {}).get("uid") not in claimed]
    children.extend(_graph_node("pods", p) for p in orphans)
    return _graph_node("deployments", dep, children)


def _flat_workload_tree(api: client.ApiClient, plural: str, namespace: str, obj: dict) -> dict:
    selector = _workload_label_selector(obj, plural)
    pods = _list_pod_objects(api, namespace, selector)
    return _graph_node(plural, obj, [_graph_node("pods", p) for p in pods])


def _cronjob_tree(api: client.ApiClient, namespace: str, cj: dict) -> dict:
    name = (cj.get("metadata") or {}).get("name")
    selector = _workload_label_selector(cj, "cronjobs")
    jobs = [
        j
        for j in list_objects(api, "jobs", namespace, labelSelector=selector).get("items") or []
        if _owned_by(j, "CronJob", name)
    ]
    job_nodes: list[dict] = []
    for job in sorted(jobs, key=lambda o: (o.get("metadata") or {}).get("creationTimestamp") or "", reverse=True)[:10]:
        j_sel = _workload_label_selector(job, "jobs")
        pods = _list_pod_objects(api, namespace, j_sel)
        pods = [p for p in pods if _owned_by(p, "Job", (job.get("metadata") or {}).get("name"))]
        job_nodes.append(_graph_node("jobs", job, [_graph_node("pods", p) for p in pods]))
    return _graph_node("cronjobs", cj, job_nodes)


def _container_specs(pod_spec: dict) -> list[dict]:
    spec = pod_spec or {}
    return list(spec.get("containers") or []) + list(spec.get("initContainers") or [])


def _configmap_names_in_pod_spec(pod_spec: dict) -> set[str]:
    names: set[str] = set()
    for vol in (pod_spec or {}).get("volumes") or []:
        cm = vol.get("configMap")
        if cm and cm.get("name"):
            names.add(cm["name"])
        for src in (vol.get("projected") or {}).get("sources") or []:
            ref = src.get("configMap")
            if ref and ref.get("name"):
                names.add(ref["name"])
    for c in _container_specs(pod_spec):
        for env in c.get("env") or []:
            ref = (env.get("valueFrom") or {}).get("configMapKeyRef")
            if ref and ref.get("name"):
                names.add(ref["name"])
        for ef in c.get("envFrom") or []:
            ref = ef.get("configMapRef")
            if ref and ref.get("name"):
                names.add(ref["name"])
    return names


def _secret_names_in_pod_spec(pod_spec: dict) -> set[str]:
    names: set[str] = set()
    for vol in (pod_spec or {}).get("volumes") or []:
        sec = vol.get("secret")
        if sec and sec.get("secretName"):
            names.add(sec["secretName"])
        for src in (vol.get("projected") or {}).get("sources") or []:
            ref = src.get("secret")
            if ref and ref.get("name"):
                names.add(ref["name"])
    for c in _container_specs(pod_spec):
        for env in c.get("env") or []:
            ref = (env.get("valueFrom") or {}).get("secretKeyRef")
            if ref and ref.get("name"):
                names.add(ref["name"])
        for ef in c.get("envFrom") or []:
            ref = ef.get("secretRef")
            if ref and ref.get("name"):
                names.add(ref["name"])
    return names


def _pvc_names_in_pod_spec(pod_spec: dict) -> set[str]:
    names: set[str] = set()
    for vol in (pod_spec or {}).get("volumes") or []:
        pvc = vol.get("persistentVolumeClaim")
        if pvc and pvc.get("claimName"):
            names.add(pvc["claimName"])
    return names


def _workload_pod_spec(obj: dict, plural: str) -> dict:
    spec = obj.get("spec") or {}
    if plural == "cronjobs":
        spec = (spec.get("jobTemplate") or {}).get("spec") or {}
    if plural == "jobs":
        return (spec.get("template") or {}).get("spec") or {}
    return (spec.get("template") or {}).get("spec") or {}


def _pods_referencing(api: client.ApiClient, namespace: str, names_fn, ref_name: str) -> list[dict]:
    return [
        p
        for p in list_objects(api, "pods", namespace).get("items") or []
        if ref_name in names_fn(p.get("spec") or {})
    ]


def _workloads_referencing(api: client.ApiClient, namespace: str, names_fn, ref_name: str) -> list[dict]:
    nodes: list[dict] = []
    for plural in _VOLUME_WORKLOADS:
        for obj in list_objects(api, plural, namespace).get("items") or []:
            if ref_name in names_fn(_workload_pod_spec(obj, plural)):
                nodes.append(_graph_node(plural, obj, hint="pod template"))
    return nodes


def _config_secret_graph(
    api: client.ApiClient,
    namespace: str,
    plural: str,
    obj: dict,
    names_fn,
) -> list[dict[str, Any]]:
    ref_name = (obj.get("metadata") or {}).get("name") or ""
    title = "ConfigMap" if plural == "configmaps" else "Secret"
    sections: list[dict[str, Any]] = [{"title": title, "nodes": [_graph_node(plural, obj)]}]
    pods = _pods_referencing(api, namespace, names_fn, ref_name)
    if pods:
        sections.append({"title": "Pods", "nodes": [_graph_node("pods", p) for p in pods]})
    workloads = _workloads_referencing(api, namespace, names_fn, ref_name)
    if workloads:
        sections.append({"title": "Workloads", "nodes": workloads})
    return sections


def _pvc_graph(api: client.ApiClient, namespace: str, obj: dict) -> list[dict[str, Any]]:
    claim_name = (obj.get("metadata") or {}).get("name") or ""
    children: list[dict] = []
    vol_name = (obj.get("status") or {}).get("volumeName")
    if vol_name:
        try:
            children.append(_graph_node("persistentvolumes", get_object(api, "persistentvolumes", None, vol_name)))
        except K8sError:
            pass
    sc_name = (obj.get("spec") or {}).get("storageClassName")
    if sc_name:
        try:
            children.append(_graph_node("storageclasses", get_object(api, "storageclasses", None, sc_name), hint="class"))
        except K8sError:
            pass
    sections: list[dict[str, Any]] = [{"title": "Storage", "nodes": [_graph_node("persistentvolumeclaims", obj, children)]}]
    pods = _pods_referencing(api, namespace, _pvc_names_in_pod_spec, claim_name)
    if pods:
        sections.append({"title": "Pods", "nodes": [_graph_node("pods", p) for p in pods]})
    return sections


def _pv_graph(api: client.ApiClient, obj: dict) -> list[dict[str, Any]]:
    children: list[dict] = []
    claim = (obj.get("spec") or {}).get("claimRef") or {}
    if claim.get("name") and claim.get("namespace"):
        try:
            children.append(
                _graph_node(
                    "persistentvolumeclaims",
                    get_object(api, "persistentvolumeclaims", claim["namespace"], claim["name"]),
                )
            )
        except K8sError:
            pass
    sc_name = (obj.get("spec") or {}).get("storageClassName")
    if sc_name:
        try:
            children.append(_graph_node("storageclasses", get_object(api, "storageclasses", None, sc_name), hint="class"))
        except K8sError:
            pass
    return [{"title": "Volume", "nodes": [_graph_node("persistentvolumes", obj, children)]}]


def _plural_for_kind(kind: str) -> str | None:
    for k in KINDS.values():
        if k.kind == kind:
            return k.plural
    return None


def _pods_matching_pod_selector(api: client.ApiClient, namespace: str, selector: dict | None) -> list[dict]:
    pods = list_objects(api, "pods", namespace).get("items") or []
    if not selector:
        return pods
    return [
        p
        for p in pods
        if _labels_match_selector((p.get("metadata") or {}).get("labels") or {}, selector)
    ]


def _hpas_for_scale_target(api: client.ApiClient, namespace: str, kind: str, target_name: str) -> list[dict]:
    out: list[dict] = []
    for hpa in list_objects(api, "horizontalpodautoscalers", namespace).get("items") or []:
        ref = (hpa.get("spec") or {}).get("scaleTargetRef") or {}
        if ref.get("kind") == kind and ref.get("name") == target_name:
            out.append(hpa)
    return out


def _append_hpa_section(
    api: client.ApiClient, namespace: str, kind: str, target_name: str, sections: list[dict[str, Any]]
) -> None:
    hpas = _hpas_for_scale_target(api, namespace, kind, target_name)
    if hpas:
        sections.append(
            {"title": "Autoscaler", "nodes": [_graph_node("horizontalpodautoscalers", h) for h in hpas]}
        )


def _networkpolicy_graph(api: client.ApiClient, namespace: str, obj: dict) -> list[dict[str, Any]]:
    sel = (obj.get("spec") or {}).get("podSelector")
    pods = _pods_matching_pod_selector(api, namespace, sel if sel is not None else {})
    sections: list[dict[str, Any]] = [{"title": "Policy", "nodes": [_graph_node("networkpolicies", obj)]}]
    if pods:
        sections.append({"title": "Selected pods", "nodes": [_graph_node("pods", p) for p in pods[:80]]})
    return sections


def _serviceaccount_graph(api: client.ApiClient, namespace: str, obj: dict) -> list[dict[str, Any]]:
    sa_name = (obj.get("metadata") or {}).get("name") or ""
    pods = [
        p
        for p in list_objects(api, "pods", namespace).get("items") or []
        if ((p.get("spec") or {}).get("serviceAccountName") or "default") == sa_name
    ]
    secret_nodes: list[dict] = []
    for ref in (obj.get("secrets") or []) + (obj.get("imagePullSecrets") or []):
        sec_name = ref.get("name")
        if not sec_name:
            continue
        try:
            secret_nodes.append(_graph_node("secrets", get_object(api, "secrets", namespace, sec_name), hint="linked"))
        except K8sError:
            continue
    children = secret_nodes if secret_nodes else None
    sections: list[dict[str, Any]] = [
        {"title": "Service account", "nodes": [_graph_node("serviceaccounts", obj, children)]}
    ]
    if pods:
        sections.append({"title": "Pods", "nodes": [_graph_node("pods", p) for p in pods[:80]]})
    return sections


def _hpa_graph(api: client.ApiClient, namespace: str, obj: dict) -> list[dict[str, Any]]:
    ref = (obj.get("spec") or {}).get("scaleTargetRef") or {}
    kind, target_name = ref.get("kind"), ref.get("name")
    plural = _HPA_TARGET_PLURAL.get(kind or "") or _plural_for_kind(kind or "")
    children: list[dict] = []
    if plural and target_name:
        try:
            target = get_object(api, plural, namespace, target_name)
            if plural in HAS_PODS:
                children.append(_flat_workload_tree(api, plural, namespace, target))
            else:
                children.append(_graph_node(plural, target, hint="scale target"))
        except K8sError:
            pass
    return [{"title": "Autoscaler", "nodes": [_graph_node("horizontalpodautoscalers", obj, children)]}]


def _node_graph(api: client.ApiClient, obj: dict) -> list[dict[str, Any]]:
    name = (obj.get("metadata") or {}).get("name") or ""
    pods = list_objects(api, "pods", None, fieldSelector=f"spec.nodeName={name}").get("items") or []
    sections: list[dict[str, Any]] = [{"title": "Node", "nodes": [_graph_node("nodes", obj)]}]
    if pods:
        sections.append({"title": "Pods", "nodes": [_graph_node("pods", p) for p in pods[:100]]})
    return sections


def _namespace_graph(api: client.ApiClient, obj: dict) -> list[dict[str, Any]]:
    ns = (obj.get("metadata") or {}).get("name") or ""
    sections: list[dict[str, Any]] = [{"title": "Namespace", "nodes": [_graph_node("namespaces", obj)]}]
    for plural, title in _NAMESPACE_MAP_KINDS:
        items = list_objects(api, plural, ns).get("items") or []
        if not items:
            continue
        sections.append({"title": title, "nodes": [_graph_node(plural, i) for i in items[:25]]})
    return sections


def _event_graph(api: client.ApiClient, obj: dict) -> list[dict[str, Any]]:
    involved = obj.get("involvedObject") or {}
    children: list[dict] = []
    plural = _plural_for_kind(involved.get("kind") or "")
    inv_name = involved.get("name")
    inv_ns = involved.get("namespace")
    if plural and inv_name:
        try:
            ns = inv_ns if get_kind(plural).namespaced else None
            children.append(
                _graph_node(plural, get_object(api, plural, ns, inv_name), hint=involved.get("kind") or "involved")
            )
        except K8sError:
            pass
    return [{"title": "Event", "nodes": [_graph_node("events", obj, children)]}]


def _replicaset_graph(api: client.ApiClient, namespace: str, obj: dict) -> list[dict[str, Any]]:
    rs_name = (obj.get("metadata") or {}).get("name")
    selector = _workload_label_selector(obj, "replicasets")
    pod_objs = _list_pod_objects(api, namespace, selector)
    pod_objs = [p for p in pod_objs if _owned_by(p, "ReplicaSet", rs_name)]
    rs_node = _graph_node("replicasets", obj, [_graph_node("pods", p) for p in pod_objs])
    for ref in (obj.get("metadata") or {}).get("ownerReferences") or []:
        if ref.get("kind") != "Deployment" or not ref.get("name"):
            continue
        try:
            dep = get_object(api, "deployments", namespace, ref["name"])
            return [{"title": "Rollout", "nodes": [_graph_node("deployments", dep, [rs_node])]}]
        except K8sError:
            break
    return [{"title": "ReplicaSet", "nodes": [rs_node]}]


def _job_graph(api: client.ApiClient, namespace: str, obj: dict) -> list[dict[str, Any]]:
    selector = _workload_label_selector(obj, "jobs")
    pods = _list_pod_objects(api, namespace, selector)
    pods = [p for p in pods if _owned_by(p, "Job", (obj.get("metadata") or {}).get("name"))]
    job_node = _graph_node("jobs", obj, [_graph_node("pods", p) for p in pods])
    for ref in (obj.get("metadata") or {}).get("ownerReferences") or []:
        if ref.get("kind") != "CronJob" or not ref.get("name"):
            continue
        try:
            cj = get_object(api, "cronjobs", namespace, ref["name"])
            return [{"title": "CronJob", "nodes": [_graph_node("cronjobs", cj, [job_node])]}]
        except K8sError:
            break
    return [{"title": "Job", "nodes": [job_node]}]


def _pod_refs_graph(api: client.ApiClient, namespace: str, pod_spec: dict) -> list[dict]:
    nodes: list[dict] = []
    sa = (pod_spec or {}).get("serviceAccountName") or "default"
    try:
        nodes.append(_graph_node("serviceaccounts", get_object(api, "serviceaccounts", namespace, sa), hint="identity"))
    except K8sError:
        pass
    for cm_name in sorted(_configmap_names_in_pod_spec(pod_spec)):
        try:
            nodes.append(_graph_node("configmaps", get_object(api, "configmaps", namespace, cm_name), hint="mount/env"))
        except K8sError:
            pass
    for sec_name in sorted(_secret_names_in_pod_spec(pod_spec)):
        try:
            nodes.append(_graph_node("secrets", get_object(api, "secrets", namespace, sec_name), hint="mount/env"))
        except K8sError:
            pass
    for claim in sorted(_pvc_names_in_pod_spec(pod_spec)):
        try:
            nodes.append(
                _graph_node("persistentvolumeclaims", get_object(api, "persistentvolumeclaims", namespace, claim))
            )
        except K8sError:
            pass
    return nodes


def _storageclass_graph(api: client.ApiClient, obj: dict) -> list[dict[str, Any]]:
    sc_name = (obj.get("metadata") or {}).get("name") or ""
    pvc_nodes: list[dict] = []
    for pvc in list_objects(api, "persistentvolumeclaims", None).get("items") or []:
        if (pvc.get("spec") or {}).get("storageClassName") != sc_name:
            continue
        ns = (pvc.get("metadata") or {}).get("namespace") or ""
        pvc_nodes.append(_graph_node("persistentvolumeclaims", pvc, hint=ns))
    pv_nodes: list[dict] = []
    for pv in list_objects(api, "persistentvolumes", None).get("items") or []:
        if (pv.get("spec") or {}).get("storageClassName") != sc_name:
            continue
        pv_nodes.append(_graph_node("persistentvolumes", pv))
    sections: list[dict[str, Any]] = [{"title": "Storage class", "nodes": [_graph_node("storageclasses", obj)]}]
    if pvc_nodes:
        sections.append({"title": "Claims", "nodes": pvc_nodes[:50]})
    if pv_nodes:
        sections.append({"title": "Volumes", "nodes": pv_nodes[:50]})
    return sections


def _pod_tree(api: client.ApiClient, namespace: str, pod: dict) -> dict:
    meta = pod.get("metadata") or {}
    labels = meta.get("labels") or {}
    children: list[dict] = []
    for ref in meta.get("ownerReferences") or []:
        if not ref.get("controller"):
            continue
        kind, owner_name = ref.get("kind"), ref.get("name")
        plural = _OWNER_PLURAL.get(kind or "")
        if not plural or not owner_name:
            continue
        try:
            owner = get_object(api, plural, namespace, owner_name)
        except K8sError:
            continue
        children.append(_graph_node(plural, owner, hint="controller"))
        break
    for svc in list_objects(api, "services", namespace).get("items") or []:
        sel = (svc.get("spec") or {}).get("selector") or {}
        if sel and _labels_match_selector(labels, sel):
            children.append(_graph_node("services", svc, hint="selects this pod"))
    children.extend(_pod_refs_graph(api, namespace, pod.get("spec") or {}))
    return _graph_node("pods", pod, children)


def resource_graph(api: client.ApiClient, plural: str, namespace: str | None, name: str) -> dict:
    """Related resources as nested trees for the drawer Map tab."""
    if plural not in KINDS:
        return {"sections": []}
    ns = namespace if get_kind(plural).namespaced else None
    obj = get_object(api, plural, ns, name)
    nns = namespace or ""
    sections: list[dict[str, Any]] = []

    if plural == "deployments":
        root = _deployment_tree(api, nns, obj)
        sections.append({"title": "Rollout", "nodes": [root]})
        labels = _template_labels(obj, plural)
        svcs = _services_for_labels(api, nns, labels)
        if svcs:
            sections.append({"title": "Services & pods", "nodes": svcs})
        ing = _ingresses_for_services(api, nns, {s["row"]["name"] for s in svcs})
        if ing:
            sections.append({"title": "Ingress", "nodes": ing})
        _append_hpa_section(api, nns, "Deployment", name, sections)
    elif plural == "cronjobs":
        sections.append({"title": "Jobs & pods", "nodes": [_cronjob_tree(api, nns, obj)]})
    elif plural == "replicasets":
        sections = _replicaset_graph(api, nns, obj)
    elif plural == "jobs":
        sections = _job_graph(api, nns, obj)
    elif plural in HAS_PODS:
        sections.append({"title": "Pods", "nodes": [_flat_workload_tree(api, plural, nns, obj)]})
        if plural == "statefulsets":
            labels = _template_labels(obj, plural)
            svcs = _services_for_labels(api, nns, labels)
            if svcs:
                sections.append({"title": "Services & pods", "nodes": svcs})
            _append_hpa_section(api, nns, "StatefulSet", name, sections)
        elif plural == "daemonsets":
            labels = _template_labels(obj, plural)
            svcs = _services_for_labels(api, nns, labels)
            if svcs:
                sections.append({"title": "Services & pods", "nodes": svcs})
    elif plural == "pods":
        sections.append({"title": "Related", "nodes": [_pod_tree(api, nns, obj)]})
    elif plural == "services":
        sel = (obj.get("spec") or {}).get("selector") or {}
        pod_sel = _selector_string({"matchLabels": sel}) if sel else ""
        root = _graph_node("services", obj, _pod_children(api, nns, pod_sel))
        sections.append({"title": "Endpoints", "nodes": [root]})
        ing = _ingresses_for_services(api, nns, {name})
        if ing:
            sections.append({"title": "Ingress", "nodes": ing})
    elif plural == "ingresses":
        trees: list[dict] = []
        for svc_name in _ingress_service_names(obj):
            try:
                svc = get_object(api, "services", nns, svc_name)
            except K8sError:
                continue
            sel = (svc.get("spec") or {}).get("selector") or {}
            pod_sel = _selector_string({"matchLabels": sel}) if sel else ""
            trees.append(_graph_node("services", svc, _pod_children(api, nns, pod_sel)))
        if trees:
            sections.append({"title": "Backends", "nodes": trees})
        sections.insert(0, {"title": "Ingress", "nodes": [_graph_node("ingresses", obj)]})
    elif plural == "configmaps":
        sections = _config_secret_graph(api, nns, plural, obj, _configmap_names_in_pod_spec)
    elif plural == "secrets":
        sections = _config_secret_graph(api, nns, plural, obj, _secret_names_in_pod_spec)
    elif plural == "persistentvolumeclaims":
        sections = _pvc_graph(api, nns, obj)
    elif plural == "persistentvolumes":
        sections = _pv_graph(api, obj)
    elif plural == "storageclasses":
        sections = _storageclass_graph(api, obj)
    elif plural == "networkpolicies":
        sections = _networkpolicy_graph(api, nns, obj)
    elif plural == "serviceaccounts":
        sections = _serviceaccount_graph(api, nns, obj)
    elif plural == "horizontalpodautoscalers":
        sections = _hpa_graph(api, nns, obj)
    elif plural == "nodes":
        sections = _node_graph(api, obj)
    elif plural == "namespaces":
        sections = _namespace_graph(api, obj)
    elif plural == "events":
        sections = _event_graph(api, obj)
    else:
        kind = get_kind(plural)
        sections = [{"title": kind.kind, "nodes": [_graph_node(plural, obj)]}]
    return {"sections": sections}


def permissions(api: client.ApiClient, plural: str, namespace: str) -> dict[str, bool]:
    """Ask the cluster (SelfSubjectAccessReview) which actions the current user may perform."""
    kind = get_kind(plural)
    checks = {"delete": ("delete", kind, None), "update": ("update", kind, None), "patch": ("patch", kind, None)}
    if plural == "pods":
        checks["exec"] = ("create", kind, "exec")
        checks["portforward"] = ("create", kind, "portforward")
        checks["logs"] = ("get", kind, "log")
    if plural == "cronjobs":
        checks["trigger"] = ("create", KINDS["jobs"], None)
    if plural == "services":
        checks["portforward"] = ("create", KINDS["pods"], "portforward")

    def ask(verb: str, k: Kind, sub: str | None) -> bool:
        attrs = {"namespace": namespace, "verb": verb, "group": k.group, "resource": k.plural}
        if sub:
            attrs["subresource"] = sub
        body = {
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectAccessReview",
            "spec": {"resourceAttributes": attrs},
        }
        try:
            res = request(api, "POST", "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews", body=body)
            return bool((res.get("status") or {}).get("allowed"))
        except K8sError:
            return False  # cannot confirm access — treat as denied in the UI

    jobs = {key: gevent.spawn(ask, *args) for key, args in checks.items()}
    gevent.joinall(list(jobs.values()), timeout=10)
    return {key: bool(job.value) if job.successful() else False for key, job in jobs.items()}


def overview(api: client.ApiClient, namespace: str | None) -> dict:
    pods_job = gevent.spawn(list_objects, api, "pods", namespace)
    deps_job = gevent.spawn(list_objects, api, "deployments", namespace)
    events_job = gevent.spawn(list_objects, api, "events", namespace, fieldSelector="type=Warning")
    gevent.joinall([pods_job, deps_job, events_job], raise_error=True)
    pods = [summarize("pods", p) for p in pods_job.value.get("items") or []]
    deps = [summarize("deployments", d) for d in deps_job.value.get("items") or []]
    warnings = sorted(
        (summarize("events", e) for e in events_job.value.get("items") or []),
        key=lambda r: r.get("last") or "",
        reverse=True,
    )
    counts: dict[str, int] = {}
    for p in pods:
        counts[p["tone"]] = counts.get(p["tone"], 0) + 1
    return {
        "pods": {"total": len(pods), "running": sum(1 for p in pods if p["status"] == "Running"), "byTone": counts},
        "restarts": sum(p["cells"][1] for p in pods),
        "problems": [p for p in pods if p["tone"] in ("bad", "warn")][:20],
        "deployments": {"total": len(deps), "degraded": [d for d in deps if d["tone"] in ("warn", "bad")]},
        "warnings": warnings[:15],
    }


# --------------------------------------------------------------------------------------------------------------------
# Actions (namespaced workloads only)
# --------------------------------------------------------------------------------------------------------------------


def writable_kind(plural: str) -> Kind:
    kind = get_kind(plural)
    if plural not in WRITABLE:
        raise K8sError(403, f"{kind.kind} resources are view-only in KubeLite")
    return kind


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def delete(api: client.ApiClient, plural: str, namespace: str, name: str, grace: int | None, force: bool) -> None:
    kind = writable_kind(plural)
    body: dict[str, Any] = {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Background"}
    if force:
        body["gracePeriodSeconds"] = 0
    elif grace is not None:
        body["gracePeriodSeconds"] = max(0, min(grace, 3600))
    request(api, "DELETE", resource_path(kind, namespace, name), body=body)


def restart(api: client.ApiClient, plural: str, namespace: str, name: str) -> str:
    kind = writable_kind(plural)
    if plural == "pods":
        pod = get_object(api, "pods", namespace, name)
        owners = (pod.get("metadata") or {}).get("ownerReferences") or []
        if not any(o.get("controller") for o in owners):
            raise K8sError(
                400, "This pod has no controller, so deleting it would not bring it back. Use Delete instead."
            )
        if (pod.get("status") or {}).get("phase") == "Succeeded":
            raise K8sError(400, "This pod has already completed; restarting it would not re-run anything.")
        delete(api, "pods", namespace, name, None, False)
        return "Pod deleted; its controller will create a replacement."
    if plural not in RESTARTABLE:
        raise K8sError(400, f"{kind.kind} resources cannot be restarted")
    patch = {"spec": {"template": {"metadata": {"annotations": {RESTARTED_AT: _now_iso()}}}}}
    request(api, "PATCH", resource_path(kind, namespace, name), body=patch, content_type="application/merge-patch+json")
    return "Rolling restart started."


def scale(api: client.ApiClient, plural: str, namespace: str, name: str, replicas: int) -> None:
    kind = writable_kind(plural)
    if plural not in SCALABLE:
        raise K8sError(400, f"{kind.kind} resources cannot be scaled")
    if not isinstance(replicas, int) or not 0 <= replicas <= 1000:
        raise K8sError(400, "Replicas must be a whole number between 0 and 1000")
    hpas = list_objects(api, "horizontalpodautoscalers", namespace).get("items") or []
    for hpa in hpas:
        ref = (hpa.get("spec") or {}).get("scaleTargetRef") or {}
        if ref.get("kind") == kind.kind and ref.get("name") == name:
            raise K8sError(
                409, f"Replica count is managed by the autoscaler {hpa['metadata']['name']!r}; change it there."
            )
    request(
        api,
        "PATCH",
        resource_path(kind, namespace, name),
        body={"spec": {"replicas": replicas}},
        content_type="application/merge-patch+json",
    )


def hpa_targets(api: client.ApiClient, namespace: str | None) -> list[str]:
    hpas = list_objects(api, "horizontalpodautoscalers", namespace).get("items") or []
    return [
        f"{h['metadata']['namespace']}/{(h.get('spec') or {}).get('scaleTargetRef', {}).get('kind')}/"
        f"{(h.get('spec') or {}).get('scaleTargetRef', {}).get('name')}"
        for h in hpas
    ]


def set_suspend(api: client.ApiClient, namespace: str, name: str, suspend: bool) -> None:
    request(
        api,
        "PATCH",
        resource_path(writable_kind("cronjobs"), namespace, name),
        body={"spec": {"suspend": bool(suspend)}},
        content_type="application/merge-patch+json",
    )


def trigger_cronjob(api: client.ApiClient, namespace: str, name: str) -> str:
    """Same as ``kubectl create job --from=cronjob/<name>``."""
    cj = get_object(api, "cronjobs", namespace, name)
    template = (cj.get("spec") or {}).get("jobTemplate") or {}
    job_name = f"{name[:42]}-manual-{int(time.time())}"
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": (template.get("metadata") or {}).get("labels") or {},
            "annotations": {
                **((template.get("metadata") or {}).get("annotations") or {}),
                "cronjob.kubernetes.io/instantiate": "manual",
            },
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "CronJob",
                    "name": name,
                    "uid": cj["metadata"]["uid"],
                    "controller": True,
                    "blockOwnerDeletion": True,
                }
            ],
        },
        "spec": template.get("spec") or {},
    }
    request(api, "POST", resource_path(writable_kind("jobs"), namespace), body=job)
    return job_name


def rollout_history(api: client.ApiClient, namespace: str, name: str) -> list[dict]:
    dep = get_object(api, "deployments", namespace, name)
    selector = _selector_string((dep.get("spec") or {}).get("selector"))
    rsets = list_objects(api, "replicasets", namespace, labelSelector=selector).get("items") or []
    uid = dep["metadata"]["uid"]
    current = (dep["metadata"].get("annotations") or {}).get("deployment.kubernetes.io/revision")
    out = []
    for rs in rsets:
        meta = rs.get("metadata") or {}
        if not any(o.get("uid") == uid for o in meta.get("ownerReferences") or []):
            continue
        ann = meta.get("annotations") or {}
        rev = ann.get("deployment.kubernetes.io/revision")
        if not rev:
            continue
        containers = (((rs.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers") or []
        out.append(
            {
                "revision": int(rev),
                "current": rev == current,
                "created": meta.get("creationTimestamp"),
                "images": [c.get("image") for c in containers],
                "changeCause": ann.get("kubernetes.io/change-cause"),
                "replicaSet": meta.get("name"),
            }
        )
    return sorted(out, key=lambda r: r["revision"], reverse=True)


def rollback(api: client.ApiClient, namespace: str, name: str, revision: int | None) -> int:
    """Same as ``kubectl rollout undo``: put an earlier ReplicaSet's pod template back on the Deployment."""
    history = rollout_history(api, namespace, name)
    current = next((h["revision"] for h in history if h["current"]), None)
    if revision is None:
        older = [h for h in history if current is None or h["revision"] < current]
        if not older:
            raise K8sError(400, "There is no earlier revision to roll back to")
        target = older[0]
    else:
        target = next((h for h in history if h["revision"] == revision), None)
        if target is None:
            raise K8sError(404, f"Revision {revision} not found")
    rs = get_object(api, "replicasets", namespace, target["replicaSet"])
    template = copy.deepcopy((rs.get("spec") or {}).get("template") or {})
    (template.get("metadata") or {}).get("labels", {}).pop("pod-template-hash", None)
    request(
        api,
        "PATCH",
        resource_path(writable_kind("deployments"), namespace, name),
        body=[{"op": "replace", "path": "/spec/template", "value": template}],
        content_type="application/json-patch+json",
    )
    return target["revision"]


def replace_from_yaml(api: client.ApiClient, plural: str, namespace: str, name: str, text: str) -> None:
    kind = writable_kind(plural)
    if len(text.encode()) > MAX_YAML_BYTES:
        raise K8sError(413, "YAML is larger than 1 MB")
    try:
        obj = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise K8sError(400, f"Not valid YAML: {exc}") from None
    if not isinstance(obj, dict):
        raise K8sError(400, "Expected a single YAML object")
    meta = obj.get("metadata") or {}
    if obj.get("kind") != kind.kind or meta.get("name") != name or meta.get("namespace", namespace) != namespace:
        raise K8sError(
            400, f"The YAML must stay {kind.kind} {namespace}/{name}; kind, name and namespace cannot change"
        )
    if not meta.get("resourceVersion"):
        raise K8sError(400, "metadata.resourceVersion is missing; it is needed to detect conflicting edits")
    if plural == "secrets" and SECRET_MASK in (obj.get("data") or {}).values():
        raise K8sError(400, "Reveal the Secret before editing it, otherwise hidden values would be overwritten")
    meta["namespace"] = namespace
    try:
        request(api, "PUT", resource_path(kind, namespace, name), body=obj)
    except K8sError as exc:
        if exc.status == 409:
            raise K8sError(
                409, "Someone changed this resource after you opened it. Reload it and apply your edit again."
            ) from None
        raise


def resolve_service_target(api: client.ApiClient, namespace: str, name: str, port: int) -> tuple[str, int]:
    """Pick a ready pod behind a Service and translate the service port into that pod's container port."""
    svc = get_object(api, "services", namespace, name)
    spec = svc.get("spec") or {}
    selector = ",".join(f"{k}={v}" for k, v in (spec.get("selector") or {}).items())
    if not selector:
        raise K8sError(400, "This Service has no selector, so there is no pod to forward to")
    svc_port = next((p for p in spec.get("ports") or [] if p.get("port") == port), None)
    if svc_port is None:
        raise K8sError(400, f"Service has no port {port}")
    pods = list_objects(api, "pods", namespace, labelSelector=selector).get("items") or []
    ready = [p for p in pods if summarize("pods", p)["tone"] == "ok"]
    if not ready:
        raise K8sError(409, "No ready pod is behind this Service right now")
    pod = ready[0]
    target = svc_port.get("targetPort", port)
    if isinstance(target, str):
        for c in (pod.get("spec") or {}).get("containers") or []:
            for p in c.get("ports") or []:
                if p.get("name") == target:
                    return pod["metadata"]["name"], int(p["containerPort"])
        raise K8sError(400, f"No container port named {target!r} in pod {pod['metadata']['name']}")
    return pod["metadata"]["name"], int(target)
