"""Offline checks: kubectl-equivalent pod status, row summaries, kubeconfig handling and the HTTP guard.

Run with ``pytest`` (no cluster needed).
"""

from pathlib import Path

import pytest

from kubelite import k8s
from kubelite.k8s import friendly_api_error
from kubelite.app import COOKIE, SESSION_HEADER, create_app

TOKEN = "test-token"
PORT = 7462
HOST = f"localhost:{PORT}"
ORIGIN = f"http://{HOST}"

KUBECONFIG = """
apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: "https://127.0.0.1:1"}}]
users: [{name: u, user: {token: abc}}]
contexts: [{name: dev, context: {cluster: c, user: u, namespace: team}}]
current-context: dev
"""


def pod(phase="Running", containers=(), init=(), deleting=False, ready=True):
    return {
        "metadata": {
            "name": "p",
            "namespace": "n",
            **({"deletionTimestamp": "2026-01-01T00:00:00Z"} if deleting else {}),
        },
        "spec": {"containers": [{"name": f"c{i}"} for i in range(len(containers))]},
        "status": {
            "phase": phase,
            "containerStatuses": list(containers),
            "initContainerStatuses": list(init),
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
        },
    }


def running(ready=True):
    return {"ready": ready, "restartCount": 0, "state": {"running": {}}}


def waiting(reason):
    return {"ready": False, "restartCount": 4, "state": {"waiting": {"reason": reason}}}


@pytest.mark.parametrize(
    ("obj", "expected"),
    [
        (pod(containers=[running()]), "Running"),
        (pod(containers=[waiting("CrashLoopBackOff")]), "CrashLoopBackOff"),
        (pod(phase="Pending", containers=[waiting("ImagePullBackOff")]), "ImagePullBackOff"),
        (pod(phase="Pending"), "Pending"),
        (
            pod(phase="Succeeded", containers=[{"state": {"terminated": {"reason": "Completed", "exitCode": 0}}}]),
            "Completed",
        ),
        (pod(phase="Failed", containers=[{"state": {"terminated": {"exitCode": 3}}}]), "ExitCode:3"),
        (pod(phase="Pending", init=[waiting("CrashLoopBackOff")]), "Init:CrashLoopBackOff"),
        (pod(phase="Pending", init=[{"state": {"running": {}}}, {"state": {}}]), "Init:0/2"),
        (pod(containers=[running()], deleting=True), "Terminating"),
    ],
)
def test_pod_status_matches_kubectl(obj, expected):
    assert k8s.pod_status(obj) == expected


def test_friendly_api_error_for_403_list():
    body = b'{"message":"deployments is forbidden: User \\"u\\" cannot list resource \\"deployments\\" in API group \\"apps\\""}'
    msg = friendly_api_error(403, body)
    assert "don't have permission to list" in msg.lower()


def test_summarize_pod_row():
    row = k8s.summarize("pods", pod(containers=[running(), waiting("CrashLoopBackOff")]))
    assert row["status"] == "CrashLoopBackOff"
    assert row["tone"] == "bad"
    assert len(row["cells"]) == len(k8s.COLUMNS["pods"])


def test_every_kind_has_columns():
    assert set(k8s.KINDS) <= set(k8s.COLUMNS)


def test_upload_is_validated_deduplicated_and_stable(tmp_path: Path):
    manager = k8s.ClusterManager(tmp_path)
    with pytest.raises(k8s.K8sError):
        manager.add_upload("not: [yaml", "bad", remember=False)
    with pytest.raises(k8s.K8sError):
        manager.add_upload("foo: bar", "bad", remember=False)
    first = manager.add_upload(KUBECONFIG, "dev.yaml", remember=True)
    again = manager.add_upload(KUBECONFIG, "dev.yaml", remember=True)
    assert first is again
    assert [c["id"] for c in manager.contexts()] == [f"{first.id}|dev"]
    saved = list(tmp_path.glob("*.yaml"))
    assert len(saved) == 1 and (saved[0].stat().st_mode & 0o777) == 0o600
    # a fresh process sees the same ids, so saved namespaces and shared log links survive restarts
    assert k8s.ClusterManager(tmp_path).add_upload(KUBECONFIG, "dev.yaml", remember=False).id == first.id


@pytest.fixture
def make_client(tmp_path: Path):
    def make(read_only=False):
        manager = k8s.ClusterManager(tmp_path)
        manager.add_upload(KUBECONFIG, "dev.yaml", remember=False)
        return create_app(manager, TOKEN, PORT, read_only=read_only).test_client()

    return make


def signed_in(client):
    client.set_cookie(COOKIE, TOKEN, domain="localhost")
    return client


def test_homepage_sets_session_cookie(make_client):
    client = make_client()
    assert client.get("/api/state", headers={"Host": HOST}).status_code == 401
    resp = client.get("/", headers={"Host": HOST})
    assert resp.status_code == 200
    assert "KubeLite" in resp.get_data(as_text=True)
    cookie = resp.headers["Set-Cookie"]
    assert "HttpOnly" in cookie and "SameSite=Strict" in cookie


def test_blocks_foreign_host_and_origin(make_client):
    client = signed_in(make_client())
    assert client.get("/api/state", headers={"Host": HOST}).status_code == 200
    assert client.get("/api/state", headers={"Host": f"evil.example:{PORT}"}).status_code == 403
    assert (
        client.post("/api/kubeconfigs", json={}, headers={"Host": HOST, "Origin": "http://evil.example"}).status_code
        == 403
    )
    assert client.post("/api/kubeconfigs", json={}, headers={"Host": HOST}).status_code == 403  # no Origin at all


def test_security_headers(make_client):
    resp = signed_in(make_client()).get("/", headers={"Host": HOST})
    assert resp.status_code == 200
    assert "frame-ancestors 'none'" in resp.headers["Content-Security-Policy"]
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "no-referrer"
    assert "Built by" in resp.get_data(as_text=True)


def test_session_header_auth(make_client):
    client = make_client()
    headers = {"Host": HOST, SESSION_HEADER: TOKEN}
    assert client.get("/api/state", headers=headers).status_code == 200
    assert client.get("/api/state", headers={"Host": HOST, SESSION_HEADER: "wrong"}).status_code == 401


def test_writes_require_edit_mode(make_client):
    client = signed_in(make_client())
    ctx = client.get("/api/state", headers={"Host": HOST}).get_json()["contexts"][0]["id"]
    headers = {"Host": HOST, "Origin": ORIGIN}
    resp = client.delete(f"/api/r/nodes/_/node-1?ctx={ctx}", headers=headers)
    assert resp.status_code == 403
    assert "Edit mode" in resp.get_json()["error"]
    headers["X-KubeLite-Edit-Mode"] = "1"
    resp = client.delete(f"/api/r/nodes/_/node-1?ctx={ctx}", headers=headers)
    assert resp.status_code in (400, 403, 405)
    assert "Edit mode" not in (resp.get_json().get("error") or "")


def test_read_only_refuses_writes(make_client):
    client = signed_in(make_client(read_only=True))
    ctx = client.get("/api/state", headers={"Host": HOST}).get_json()["contexts"][0]["id"]
    headers = {"Host": HOST, "Origin": ORIGIN}
    resp = client.delete(f"/api/r/pods/team/web-1?ctx={ctx}", headers=headers)
    assert resp.status_code == 403 and "read-only" in resp.get_json()["error"]
    resp = client.post(f"/api/r/deployments/team/web/scale?ctx={ctx}", json={"replicas": 2}, headers=headers)
    assert resp.status_code == 403


def test_configmap_names_in_pod_spec():
    spec = {
        "volumes": [{"configMap": {"name": "app-config"}}],
        "containers": [
            {
                "env": [{"valueFrom": {"configMapKeyRef": {"name": "other-cm", "key": "k"}}}],
                "envFrom": [{"configMapRef": {"name": "app-config"}}],
            }
        ],
    }
    names = k8s._configmap_names_in_pod_spec(spec)
    assert names == {"app-config", "other-cm"}


def test_labels_match_selector():
    assert k8s._labels_match_selector({"app": "web", "tier": "front"}, {"app": "web"})
    assert not k8s._labels_match_selector({"app": "web"}, {"app": "api"})
    assert k8s._labels_match_selector({"app": "web"}, {"matchLabels": {"app": "web"}})
    assert not k8s._labels_match_selector({}, {"app": "web"})


def test_cluster_scoped_kinds_are_not_writable(make_client):
    client = signed_in(make_client())
    ctx = client.get("/api/state", headers={"Host": HOST}).get_json()["contexts"][0]["id"]
    resp = client.delete(f"/api/r/nodes/_/node-1?ctx={ctx}", headers={"Host": HOST, "Origin": ORIGIN})
    assert resp.status_code in (400, 403, 405)
