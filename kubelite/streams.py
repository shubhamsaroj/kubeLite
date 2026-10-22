"""Long-lived streams: pod logs and resource watches over Server-Sent Events, exec over WebSocket, port-forwarding."""

from __future__ import annotations

import json
import logging
import re
import secrets
import socket
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import gevent
from gevent.queue import Empty, Queue
from gevent.server import StreamServer
from kubernetes import client
from kubernetes.stream import portforward, stream

from .k8s import COLUMNS, K8sError, get_kind, request, resource_path, summarize

log = logging.getLogger(__name__)

# ponytail: initial history per container is capped so "1h" on a chatty pod can't send millions of lines.
# Raise MAX_INITIAL_LINES (and the browser's MAX_LINES in app.js) together if people need more.
MAX_INITIAL_LINES = 10_000
KEEPALIVE_SECONDS = 15


def sse(event: str | None, data: Any) -> str:
    head = f"event: {event}\n" if event else ""
    return f"{head}data: {json.dumps(data, separators=(',', ':'))}\n\n"


# --------------------------------------------------------------------------------------------------------------------
# Logs
# --------------------------------------------------------------------------------------------------------------------


@dataclass
class LogTarget:
    namespace: str
    pod: str
    container: str
    previous: bool = False


RETRY_SECONDS = 3
TS_RE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z$")
INOTIFY_HINT = (
    "The node ran out of inotify watchers, so it cannot follow logs (kubectl logs -f fails the same way). "
    "Raise fs.inotify.max_user_instances / max_user_watches on the node; see the README troubleshooting section."
)


def _lines(resp: Any) -> Iterator[str]:
    """Like kubernetes' iter_resp_lines, but also yields a final line that has no trailing newline."""
    buffer = bytearray()
    for segment in resp.stream(amt=None, decode_content=False):
        buffer.extend(segment if isinstance(segment, bytes) else segment.encode())
        while (nl := buffer.find(b"\n")) != -1:
            yield buffer[:nl].decode("utf-8", errors="replace")
            del buffer[: nl + 1]
    if buffer:
        yield buffer.decode("utf-8", errors="replace")


def _ts_key(ts: str) -> str:
    """Make RFC3339Nano timestamps sortable as strings (Kubernetes trims trailing zeros from the fraction)."""
    head, _, frac = ts.rstrip("Z").partition(".")
    return f"{head}.{frac.ljust(9, '0')}"


def _read_log(
    api: client.ApiClient, idx: int, target: LogTarget, since: int | None, tail: int | None, out: Queue
) -> None:
    """Stream one container's log. Following streams survive restarts: when the container exits or has not started
    yet, wait and reconnect from the last timestamp seen, skipping lines already sent."""
    path = resource_path(get_kind("pods"), target.namespace, target.pod, "log")
    last_ts: str | None = None
    last_notice: Any = None
    inotify_warned = False

    def notice(msg: dict) -> None:
        nonlocal last_notice
        if msg != last_notice:  # don't repeat the same status every retry
            out.put([idx, None, msg])
            last_notice = msg

    while True:
        query = [("container", target.container), ("timestamps", "true")]
        if target.previous:
            query.append(("previous", "true"))
        else:
            query.append(("follow", "true"))
        if last_ts:
            query.append(("sinceTime", last_ts.split(".")[0].rstrip("Z") + "Z"))
        else:
            if since:
                query.append(("sinceSeconds", str(since)))
            query.append(("tailLines", str(MAX_INITIAL_LINES if tail is None else min(tail, MAX_INITIAL_LINES))))
        resp = None
        got_lines = False
        try:
            resp = request(api, "GET", path, query=query, stream=True, timeout=(10, None))
            for line in _lines(resp):
                ts, _, text = line.partition(" ")
                if not TS_RE.match(ts):  # a message from the API server/kubelet rather than a log line
                    if "fsnotify" not in line:
                        notice({"error": line})
                    elif not inotify_warned:  # reconnecting every few seconds still delivers new lines
                        inotify_warned = True
                        out.put([idx, None, {"warning": INOTIFY_HINT}])
                    continue
                if last_ts and _ts_key(ts) <= _ts_key(last_ts):
                    continue
                last_ts, got_lines, last_notice = ts, True, None
                out.put([idx, ts, text])
            if target.previous:
                out.put([idx, None, {"end": "previous"}])
                return
            if not inotify_warned and (last_notice is None or got_lines):
                notice({"end": "exited" if last_ts else "nologs"})
        except K8sError as exc:
            if exc.status == 404 or target.previous:
                out.put([idx, None, {"error": exc.message, "final": True}])
                return
            notice({"error": exc.message})
        except Exception as exc:  # connection dropped mid-stream; reconnect below
            notice({"error": f"Log stream interrupted ({exc}); reconnecting"})
        finally:
            if resp is not None:
                resp.release_conn()
        gevent.sleep(RETRY_SECONDS)


def log_events(api: client.ApiClient, targets: list[LogTarget], since: int | None, tail: int | None) -> Iterator[str]:
    """Multiplex several container log streams into one SSE response (browsers allow only ~6 connections per host).

    Each event carries a batch of ``[targetIndex, timestamp, line]`` items. A non-string third element is a control
    message: ``{"end": ...}`` when the stream finishes, ``{"error": ...}`` when it fails.
    """
    out: Queue = Queue(maxsize=20_000)  # blocks readers if the browser falls behind
    readers = [gevent.spawn(_read_log, api, i, t, since, tail, out) for i, t in enumerate(targets)]
    try:
        yield "retry: 3000\n\n"
        while True:
            try:
                first = out.get(timeout=KEEPALIVE_SECONDS)
            except Empty:
                yield ": keepalive\n\n"
                continue
            batch = [first]
            while len(batch) < 2000:
                try:
                    batch.append(out.get_nowait())
                except Empty:
                    break
            yield sse(None, batch)
            gevent.sleep(0.05)  # coalesce bursts into fewer, larger events
    finally:
        gevent.killall(readers, block=False)


# --------------------------------------------------------------------------------------------------------------------
# Watches
# --------------------------------------------------------------------------------------------------------------------


def watch_events(api: client.ApiClient, plural: str, namespace: str | None) -> Iterator[str]:
    """Send a full ``snapshot`` and then ``upsert``/``delete`` rows as the cluster changes. Relists on 410 Gone."""
    kind = get_kind(plural)
    path = resource_path(kind, namespace if kind.namespaced else None)
    yield "retry: 3000\n\n"
    while True:
        try:
            data = request(api, "GET", path, timeout=60)
        except K8sError as exc:
            yield sse("failure", {"error": exc.message, "status": exc.status})
            gevent.sleep(5)
            continue
        version = data["metadata"]["resourceVersion"]
        rows = [summarize(plural, o) for o in data.get("items") or []]
        yield sse("snapshot", {"columns": COLUMNS[plural], "rows": rows})
        gone = False
        while not gone:
            query = [
                ("watch", "true"),
                ("resourceVersion", version),
                ("allowWatchBookmarks", "true"),
                ("timeoutSeconds", "240"),
            ]
            resp = None
            try:
                resp = request(api, "GET", path, query=query, stream=True, timeout=(10, 300))
                for line in _lines(resp):
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    obj = event.get("object") or {}
                    if event.get("type") == "ERROR":
                        if obj.get("code") == 410:
                            gone = True
                            break
                        raise K8sError(obj.get("code", 500), obj.get("message", "watch failed"))
                    version = (obj.get("metadata") or {}).get("resourceVersion", version)
                    if event.get("type") == "BOOKMARK":
                        continue
                    yield sse("delete" if event["type"] == "DELETED" else "upsert", summarize(plural, obj))
            except K8sError as exc:
                if exc.status == 410:
                    gone = True
                else:
                    yield sse("failure", {"error": exc.message, "status": exc.status})
                    gevent.sleep(5)
                    gone = True
            except Exception as exc:  # dropped connection: resume from the last version we saw
                log.debug("watch %s interrupted: %s", plural, exc)
                gevent.sleep(1)
            finally:
                if resp is not None:
                    resp.release_conn()


# --------------------------------------------------------------------------------------------------------------------
# Exec
# --------------------------------------------------------------------------------------------------------------------

SHELL_CMD = ["/bin/sh", "-c", "if command -v bash >/dev/null 2>&1; then exec bash; else exec sh; fi"]


def _ws_core(api: client.ApiClient) -> client.CoreV1Api:
    """kubernetes.stream swaps ``call_api`` on the client it is given; under gevent that leaks into concurrent
    requests on a shared client, so every exec/port-forward call gets its own throwaway client."""
    return client.CoreV1Api(client.ApiClient(api.configuration))


def exec_bridge(ws: Any, api: client.ApiClient, namespace: str, pod: str, container: str) -> None:
    """Connect a browser terminal (xterm.js over WebSocket) to an interactive shell in the container."""
    core = _ws_core(api)
    try:
        resp = stream(
            core.connect_get_namespaced_pod_exec,
            pod,
            namespace,
            container=container,
            command=SHELL_CMD,
            stdin=True,
            stdout=True,
            stderr=True,
            tty=True,
            _preload_content=False,
        )
    except Exception as exc:
        ws.send(f"\r\n\x1b[31mCould not open a shell: {_short_error(exc)}\x1b[0m\r\n")
        return

    def pump_output() -> None:
        try:
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    ws.send(resp.read_stdout())
                if resp.peek_stderr():
                    ws.send(resp.read_stderr())
            ws.send("\r\n\x1b[90m[session ended]\x1b[0m\r\n")
        except Exception:  # browser went away
            pass
        finally:
            ws.close()

    pump = gevent.spawn(pump_output)
    try:
        while resp.is_open():
            msg = json.loads(ws.receive())
            if msg.get("type") == "input":
                resp.write_stdin(msg.get("data", ""))
            elif msg.get("type") == "resize":
                cols, rows = int(msg.get("cols", 80)), int(msg.get("rows", 24))
                resp.write_channel(4, json.dumps({"Width": max(cols, 1), "Height": max(rows, 1)}))
    except Exception:  # connection closed by either side
        pass
    finally:
        resp.close()
        pump.kill(block=False)


def _short_error(exc: Exception) -> str:
    body = getattr(exc, "body", None)
    if body:
        try:
            return json.loads(body).get("message", str(exc))
        except (ValueError, AttributeError):
            pass
    return str(exc).splitlines()[0][:300]


# --------------------------------------------------------------------------------------------------------------------
# Port-forwarding
# --------------------------------------------------------------------------------------------------------------------


@dataclass
class Forward:
    id: str
    context: str
    namespace: str
    target: str  # "pod/name" or "service/name"
    pod: str
    remote_port: int
    local_port: int
    server: StreamServer = field(repr=False)

    def public(self) -> dict:
        return {
            "id": self.id,
            "context": self.context,
            "namespace": self.namespace,
            "target": self.target,
            "pod": self.pod,
            "remotePort": self.remote_port,
            "localPort": self.local_port,
            "url": f"http://localhost:{self.local_port}",
        }


class PortForwards:
    """Local listeners on 127.0.0.1 that tunnel each TCP connection to a pod port through the API server."""

    def __init__(self) -> None:
        self.items: dict[str, Forward] = {}

    def start(
        self,
        api: client.ApiClient,
        context: str,
        namespace: str,
        target: str,
        pod: str,
        remote_port: int,
        local_port: int | None,
        resolve: Callable[[], tuple[str, int]] | None = None,
    ) -> Forward:
        """``resolve`` (Service targets) picks a ready pod for each new connection, so forwards survive rollouts."""

        def connect() -> socket.socket:
            pf = portforward(
                _ws_core(api).connect_get_namespaced_pod_portforward, fwd.pod, namespace, ports=str(fwd.remote_port)
            )
            sock = pf.socket(fwd.remote_port)
            sock.setblocking(True)
            return sock

        def handle(conn: socket.socket, _addr: Any) -> None:
            try:
                if resolve is not None:
                    # ponytail: two API reads per TCP connection; cache with a short TTL if that ever matters.
                    try:
                        fwd.pod, fwd.remote_port = resolve()
                    except K8sError as exc:
                        log.debug("re-resolve %s failed, keeping %s: %s", target, fwd.pod, exc.message)
                remote = connect()
            except Exception as exc:
                log.warning(
                    "port-forward to %s/%s:%s failed: %s", namespace, fwd.pod, fwd.remote_port, _short_error(exc)
                )
                conn.close()
                return

            def pipe(src: socket.socket, dst: socket.socket) -> None:
                try:
                    while data := src.recv(65536):
                        dst.sendall(data)
                except OSError:
                    pass
                finally:
                    for s in (src, dst):
                        try:
                            s.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass

            back = gevent.spawn(pipe, remote, conn)
            pipe(conn, remote)
            back.join(timeout=5)
            conn.close()
            remote.close()

        wanted = local_port or (remote_port if remote_port >= 1024 else 0)
        server = None
        for port in dict.fromkeys([wanted, 0]):
            try:
                server = StreamServer(("127.0.0.1", port), handle)
                server.start()
                break
            except OSError:
                if local_port:  # the user asked for this exact port
                    raise K8sError(409, f"Local port {local_port} is already in use") from None
        assert server is not None
        fwd = Forward(secrets.token_hex(4), context, namespace, target, pod, remote_port, server.server_port, server)
        self.items[fwd.id] = fwd
        return fwd

    def stop(self, fwd_id: str) -> None:
        fwd = self.items.pop(fwd_id, None)
        if fwd is None:
            raise K8sError(404, "Port-forward not found")
        fwd.server.stop(timeout=1)

    def list(self) -> list[dict]:
        return [f.public() for f in self.items.values()]
