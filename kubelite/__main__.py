"""Command-line entry point: ``kubelite`` or ``python -m kubelite``."""

from gevent import monkey

monkey.patch_all()  # must run before anything imports socket/ssl/urllib3

import argparse  # noqa: E402
import logging  # noqa: E402
import os  # noqa: E402
import secrets  # noqa: E402
import socket  # noqa: E402
import sys  # noqa: E402
import webbrowser  # noqa: E402
from pathlib import Path  # noqa: E402

import gevent  # noqa: E402
from gevent.pywsgi import WSGIServer  # noqa: E402

from . import __version__  # noqa: E402
from .app import create_app  # noqa: E402
from .k8s import ClusterManager, K8sError  # noqa: E402

DEFAULT_PORT = 7462
STORE_DIR = Path.home() / ".kubelite" / "kubeconfigs"
SESSION_FILE = Path.home() / ".kubelite" / "session"


def _kubelite_home() -> Path:
    base = Path.home() / ".kubelite"
    if base.exists() and not base.is_dir():
        raise SystemExit(
            f"{base} exists but is not a folder. Remove or rename that file, then start KubeLite again."
        )
    base.mkdir(mode=0o700, exist_ok=True)
    return base


def _load_session_token() -> str:
    """Stable localhost session token (reused across restarts so http://localhost:PORT keeps working)."""
    _kubelite_home()
    if SESSION_FILE.is_file():
        token = SESSION_FILE.read_text(encoding="utf-8").strip()
        if token:
            return token
    token = secrets.token_urlsafe(24)
    fd = os.open(SESSION_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(token)
    return token


def _listen(port: int) -> socket.socket:
    """Bind 127.0.0.1:port, falling back to any free port if it is taken."""
    for candidate in dict.fromkeys([port, 0]):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if os.name != "nt":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", candidate))
        except OSError:
            sock.close()
            if candidate:
                print(f"Port {candidate} is in use; picking a free one.", file=sys.stderr)
            continue
        sock.listen(128)
        return sock
    raise SystemExit("Could not find a free port")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="kubelite", description="Lightweight Kubernetes dashboard in your browser.")
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("KUBELITE_PORT", DEFAULT_PORT)),
        help=f"port on localhost (default {DEFAULT_PORT}, or $KUBELITE_PORT)",
    )
    parser.add_argument(
        "--kubeconfig",
        type=Path,
        action="append",
        default=[],
        help="kubeconfig file to load at start (repeatable). Defaults to $KUBECONFIG or ~/.kube/config",
    )
    parser.add_argument("--no-browser", action="store_true", help="don't open the browser automatically")
    parser.add_argument("--read-only", action="store_true", help="disable every action that changes the cluster")
    parser.add_argument("--debug", action="store_true", help="verbose logging")
    parser.add_argument("--version", action="version", version=f"kubelite {__version__}")
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARNING, format="%(levelname)s %(name)s: %(message)s"
    )

    manager = ClusterManager(STORE_DIR)
    paths = (
        args.kubeconfig
        or [Path(p) for p in os.environ.get("KUBECONFIG", "").split(os.pathsep) if p]
        or [Path.home() / ".kube" / "config"]
    )
    for path in paths:
        path = path.expanduser()
        if path.is_file():
            try:
                manager.load_file(path)
            except (K8sError, OSError) as exc:
                print(f"Skipping {path}: {getattr(exc, 'message', exc)}", file=sys.stderr)
        elif args.kubeconfig:
            print(f"Kubeconfig not found: {path}", file=sys.stderr)
    manager.load_remembered()

    listener = _listen(args.port)
    port = listener.getsockname()[1]
    token = _load_session_token()
    app = create_app(manager, token, port, read_only=args.read_only)
    url = f"http://localhost:{port}/"

    server = WSGIServer(listener, app, log=None if not args.debug else "default")
    print(f"\n  KubeLite {__version__} is running.\n\n  Open: {url}\n")
    if args.read_only:
        print("  Read-only mode: no changes can be made to clusters.")
    print("  Press Ctrl+C to stop.\n")
    if not args.no_browser:
        gevent.spawn_later(0.5, webbrowser.open, url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
