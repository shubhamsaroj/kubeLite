"""HTTP layer: access control, security headers and the JSON/SSE/WebSocket routes used by the UI."""

from __future__ import annotations

import hmac
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from flask import Flask, Response, abort, jsonify, make_response, redirect, render_template, request, stream_with_context
from flask_sock import Sock

from . import __version__, k8s
from .k8s import K8sError, check_name, get_kind
from .streams import LogTarget, PortForwards, exec_bridge, log_events, watch_events

log = logging.getLogger(__name__)
COOKIE = "kubelite_session"
SESSION_HEADER = "X-KubeLite-Session"
EDIT_HEADER = "X-KubeLite-Edit-Mode"
MAX_LOG_TARGETS = 20


def create_app(manager: k8s.ClusterManager, token: str, port: int, read_only: bool = False) -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024
    sock = Sock(app)
    forwards = PortForwards()
    hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}
    origins = {f"http://{h}" for h in hosts}

    # ---------------------------------------------------------------------------------------------- access control

    def session_ok() -> bool:
        for value in (
            request.cookies.get(COOKIE, ""),
            request.headers.get(SESSION_HEADER, ""),
            request.args.get("session", ""),
        ):
            if value and hmac.compare_digest(value, token):
                return True
        return False

    @app.before_request
    def guard() -> Any:
        if request.host not in hosts:  # blocks DNS-rebinding attacks from web pages
            abort(403)
        if request.path.startswith("/static/"):
            return None
        if not session_ok():
            # Localhost-only app: opening / signs you in (cookie). No ?token= URL needed.
            if request.path == "/" and request.method in ("GET", "HEAD"):
                resp = make_response(render_template("index.html", version=__version__, read_only=read_only))
                resp.set_cookie(COOKIE, token, httponly=True, samesite="Strict", path="/")
                return resp
            return jsonify(error="Not signed in. Open http://localhost in the browser where KubeLite is running."), 401
        if request.method not in ("GET", "HEAD") or request.path.startswith("/ws/"):
            if request.headers.get("Origin") not in origins:
                abort(403)
        return None

    @app.after_request
    def headers(resp: Response) -> Response:
        ws = " ".join(f"ws://{h}" for h in hosts)
        resp.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; "
            f"img-src 'self' data:; connect-src 'self' {ws}; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        )
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["Referrer-Policy"] = "no-referrer"
        resp.headers["X-Frame-Options"] = "DENY"
        if request.path.startswith("/api/"):
            resp.headers["Cache-Control"] = "no-store"
        elif request.path.startswith("/static/") and request.path.rsplit(".", 1)[-1] in ("js", "css"):
            resp.headers["Cache-Control"] = "no-cache"
        return resp

    @app.errorhandler(K8sError)
    def k8s_error(exc: K8sError) -> Any:
        return jsonify(error=exc.message), exc.status if 400 <= exc.status < 600 else 500

    @app.errorhandler(Exception)
    def unexpected(exc: Exception) -> Any:
        if hasattr(exc, "code") and hasattr(exc, "description"):  # werkzeug HTTP errors
            return jsonify(error=exc.description), exc.code
        log.exception("Unhandled error on %s", request.path)
        return jsonify(error="Unexpected error; details are in the terminal running KubeLite."), 500

    def cluster_writes_allowed() -> None:
        if read_only:
            raise K8sError(403, "KubeLite was started with --read-only")
        # Browsers cannot set custom headers on WebSocket; exec passes edit=1 in the query string instead.
        if request.headers.get(EDIT_HEADER) != "1" and request.args.get("edit") != "1":
            raise K8sError(403, "Edit mode is off. Turn it on in KubeLite before making changes.")

    def writes(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            cluster_writes_allowed()
            return fn(*args, **kwargs)

        return wrapper

    def api() -> Any:
        return manager.api(request.args.get("ctx"))

    def ns_arg() -> str | None:
        ns = request.args.get("ns") or "_all"
        return None if ns == "_all" else check_name(ns, "namespace")

    def body() -> dict:
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            raise K8sError(400, "Expected a JSON object")
        return data

    def event_stream(gen: Any) -> Response:
        return Response(
            stream_with_context(gen),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    # ---------------------------------------------------------------------------------------------- pages & config

    @app.get("/")
    def index() -> str:
        return render_template("index.html", version=__version__, read_only=read_only)

    @app.get("/api/state")
    def state() -> Any:
        return jsonify(
            version=__version__,
            readOnly=read_only,
            editModeRequired=not read_only,
            contexts=manager.contexts(),
            forwards=forwards.list(),
        )

    @app.post("/api/kubeconfigs")
    def upload_kubeconfig() -> Any:
        data = body()
        content = data.get("content")
        if not isinstance(content, str) or not content.strip():
            raise K8sError(400, "The file is empty")
        src = manager.add_upload(content, str(data.get("name") or "uploaded"), bool(data.get("remember")))
        return jsonify(source=src.id, contexts=manager.contexts())

    @app.delete("/api/kubeconfigs/<source_id>")
    def remove_kubeconfig(source_id: str) -> Any:
        manager.remove(source_id)
        return jsonify(contexts=manager.contexts())

    # ---------------------------------------------------------------------------------------------- reading

    @app.get("/api/namespaces")
    def namespaces() -> Any:
        try:
            items = k8s.list_objects(api(), "namespaces", None).get("items") or []
            return jsonify(namespaces=sorted(i["metadata"]["name"] for i in items), canList=True)
        except K8sError as exc:
            if exc.status == 403:  # users limited to one namespace can still work in it
                return jsonify(namespaces=[], canList=False)
            raise

    @app.get("/api/overview")
    def overview() -> Any:
        return jsonify(k8s.overview(api(), ns_arg()))

    @app.get("/api/list/<plural>")
    def list_rows(plural: str) -> Any:
        kind = get_kind(plural)
        data = k8s.list_objects(
            api(), plural, ns_arg() if kind.namespaced else None, labelSelector=request.args.get("selector", "")
        )
        return jsonify(columns=k8s.COLUMNS[plural], rows=[k8s.summarize(plural, o) for o in data.get("items") or []])

    @app.get("/api/watch/<plural>")
    def watch(plural: str) -> Response:
        kind = get_kind(plural)
        return event_stream(watch_events(api(), plural, ns_arg() if kind.namespaced else None))

    @app.get("/api/hpa-targets")
    def hpa_targets() -> Any:
        return jsonify(targets=k8s.hpa_targets(api(), ns_arg()))

    def obj_ns(plural: str, ns: str) -> str | None:
        return check_name(ns, "namespace") if get_kind(plural).namespaced else None

    @app.get("/api/r/<plural>/<ns>/<name>")
    def get_resource(plural: str, ns: str, name: str) -> Any:
        obj = k8s.get_object(api(), plural, obj_ns(plural, ns), name)
        reveal = request.args.get("reveal") == "1"
        return jsonify(
            yaml=k8s.to_display_yaml(obj, reveal=reveal),
            row=k8s.summarize(plural, obj),
            annotations=(obj.get("metadata") or {}).get("annotations") or {},
        )

    @app.get("/api/r/<plural>/<ns>/<name>/events")
    def resource_events(plural: str, ns: str, name: str) -> Any:
        return jsonify(events=k8s.events_for(api(), get_kind(plural), obj_ns(plural, ns), name))

    @app.get("/api/r/<plural>/<ns>/<name>/pods")
    def workload_pods(plural: str, ns: str, name: str) -> Any:
        if plural not in k8s.HAS_PODS:
            raise K8sError(400, "This resource has no pods")
        return jsonify(pods=k8s.pods_for_workload(api(), plural, check_name(ns, "namespace"), name))

    @app.get("/api/r/<plural>/<ns>/<name>/graph")
    def resource_graph_route(plural: str, ns: str, name: str) -> Any:
        if plural not in k8s.KINDS:
            raise K8sError(400, "No resource map for this kind")
        return jsonify(k8s.resource_graph(api(), plural, obj_ns(plural, ns), name))

    @app.get("/api/r/secrets/<ns>/<name>/values")
    def secret_values(ns: str, name: str) -> Any:
        return jsonify(values=k8s.secret_values(api(), check_name(ns, "namespace"), name))

    @app.get("/api/r/deployments/<ns>/<name>/history")
    def history(ns: str, name: str) -> Any:
        return jsonify(history=k8s.rollout_history(api(), check_name(ns, "namespace"), name))

    @app.get("/api/permissions/<plural>/<ns>")
    def perms(plural: str, ns: str) -> Any:
        if read_only:
            return jsonify(readOnly=True, allowed={})
        return jsonify(readOnly=False, allowed=k8s.permissions(api(), plural, check_name(ns, "namespace")))

    @app.get("/api/logs")
    def logs() -> Response:
        targets = []
        for raw in request.args.getlist("target")[:MAX_LOG_TARGETS]:
            parts = raw.split("/")
            if len(parts) not in (3, 4) or (len(parts) == 4 and parts[3] != "previous"):
                raise K8sError(400, f"Bad log target: {raw!r}")
            targets.append(
                LogTarget(
                    check_name(parts[0], "namespace"),
                    check_name(parts[1]),
                    check_name(parts[2], "container"),
                    len(parts) == 4,
                )
            )
        if not targets:
            raise K8sError(400, "No log target given")
        since = _int_arg("since", 1, 31 * 24 * 3600)
        tail = _int_arg("tail", 0, 1_000_000)
        return event_stream(log_events(api(), targets, since, tail))

    # ---------------------------------------------------------------------------------------------- actions

    @app.put("/api/r/<plural>/<ns>/<name>")
    @writes
    def apply_yaml(plural: str, ns: str, name: str) -> Any:
        text = body().get("yaml")
        if not isinstance(text, str):
            raise K8sError(400, "Missing yaml")
        k8s.replace_from_yaml(api(), plural, check_name(ns, "namespace"), name, text)
        return jsonify(message="Applied.")

    @app.delete("/api/r/<plural>/<ns>/<name>")
    @writes
    def delete_resource(plural: str, ns: str, name: str) -> Any:
        k8s.delete(
            api(),
            plural,
            check_name(ns, "namespace"),
            name,
            _int_arg("grace", 0, 3600),
            request.args.get("force") == "1",
        )
        return jsonify(message=f"Deleted {name}.")

    @app.post("/api/r/<plural>/<ns>/<name>/<action>")
    @writes
    def action(plural: str, ns: str, name: str, action: str) -> Any:
        ns = check_name(ns, "namespace")
        a = api()
        if action == "restart":
            return jsonify(message=k8s.restart(a, plural, ns, name))
        if action == "scale":
            replicas = body().get("replicas")
            k8s.scale(
                a, plural, ns, name, replicas if isinstance(replicas, int) and not isinstance(replicas, bool) else -1
            )
            return jsonify(message=f"Scaled {name} to {replicas}.")
        if action == "suspend" and plural == "cronjobs":
            suspend = bool(body().get("suspend"))
            k8s.set_suspend(a, ns, name, suspend)
            return jsonify(message="Suspended." if suspend else "Resumed.")
        if action == "trigger" and plural == "cronjobs":
            return jsonify(message=f"Started job {k8s.trigger_cronjob(a, ns, name)}.")
        if action == "rollback" and plural == "deployments":
            rev = body().get("revision")
            done = k8s.rollback(a, ns, name, rev if isinstance(rev, int) else None)
            return jsonify(message=f"Rolled back to revision {done}.")
        raise K8sError(404, f"Unknown action {action!r} for {plural}")

    # ---------------------------------------------------------------------------------------------- port-forward

    @app.get("/api/portforward")
    def list_forwards() -> Any:
        return jsonify(forwards=forwards.list())

    @app.post("/api/portforward")
    @writes
    def start_forward() -> Any:
        data = body()
        ctx = request.args.get("ctx", "")
        ns = check_name(data.get("namespace"), "namespace")
        name = check_name(data.get("name"))
        remote = data.get("port")
        local = data.get("localPort") or None
        if not isinstance(remote, int) or not 1 <= remote <= 65535:
            raise K8sError(400, "Port must be between 1 and 65535")
        if local is not None and (not isinstance(local, int) or not 1024 <= local <= 65535):
            raise K8sError(400, "Local port must be between 1024 and 65535")
        a = manager.api(ctx)
        resolve = None
        if data.get("kind") == "services":
            resolve = lambda: k8s.resolve_service_target(a, ns, name, remote)  # noqa: E731
            pod, remote_port = resolve()
            target = f"service/{name}"
        else:
            pod, remote_port, target = name, remote, f"pod/{name}"
        fwd = forwards.start(a, ctx, ns, target, pod, remote_port, local, resolve)
        return jsonify(forward=fwd.public(), forwards=forwards.list())

    @app.delete("/api/portforward/<fwd_id>")
    def stop_forward(fwd_id: str) -> Any:
        forwards.stop(fwd_id)
        return jsonify(forwards=forwards.list())

    # ---------------------------------------------------------------------------------------------- exec

    @sock.route("/ws/exec")
    def exec_ws(ws: Any) -> None:
        if read_only:
            ws.send("\r\nKubeLite was started with --read-only, so shells are disabled.\r\n")
            return
        try:
            cluster_writes_allowed()
        except K8sError as exc:
            ws.send(f"\r\n{exc.message}\r\n")
            return
        try:
            a = api()
            ns = check_name(request.args.get("ns"), "namespace")
            pod = check_name(request.args.get("pod"))
            container = check_name(request.args.get("container"), "container")
        except K8sError as exc:
            ws.send(f"\r\n{exc.message}\r\n")
            return
        exec_bridge(ws, a, ns, pod, container)

    return app


def _int_arg(name: str, lo: int, hi: int) -> int | None:
    raw = request.args.get(name)
    if raw in (None, ""):
        return None
    try:
        value = int(raw)
    except ValueError:
        raise K8sError(400, f"{name} must be a number") from None
    if not lo <= value <= hi:
        raise K8sError(400, f"{name} must be between {lo} and {hi}")
    return value


