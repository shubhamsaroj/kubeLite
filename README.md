# KubeLite

When a workload acts up, you jump between `kubectl get`, log tails, and guessing how Deployment, Service, and config line up.

**KubeLite is what you need for that troubleshooting flow — in one tab.** Lists and warnings, live logs (several pods at once), **Map** to see relationships, plus Events and YAML. Shell and port-forward when you turn **edit mode** on.

Runs on your laptop with the same kubeconfig as `kubectl`. Nothing installed in the cluster.

Python **3.10+** on macOS, Linux, or Windows.

## Quick start

**Python 3.10+** required. Check with `python3 --version` (Windows: `py --version`).

Install from the [v1.0.0 release](https://github.com/shubhamsaroj/kubeLite/releases/tag/v1.0.0) wheel (no repo clone):

```bash
python3 -m pip install https://github.com/shubhamsaroj/kubeLite/releases/download/v1.0.0/kubelite-1.0.0-py3-none-any.whl
kubelite
```

Open **http://localhost:7462** (or the URL printed in the terminal). Press `Ctrl+C` to stop.

If `kubelite` is not found, run `python3 -m kubelite` instead. Other install options (pipx, venv, from source) are in [Install](#install).

### First minute

1. Select **cluster** and **namespace** at the top — or drag a kubeconfig onto the page.
2. Open any resource; for pods, **Logs** opens first. Try **Map** on a Deployment to see the rollout tree.
3. Need more than one log stream? **Log workspace** → **+ Add pods** → pick pods → **Add N panes**.

## Features

### Browse and understand

- **Live resource lists** — Pods, Deployments, StatefulSets, DaemonSets, ReplicaSets, Jobs, CronJobs, Services, Ingresses, NetworkPolicies, ConfigMaps, Secrets, ServiceAccounts, HPAs, PVCs, Events, and cluster-scoped **Nodes**, **Namespaces**, **PVs**, **StorageClasses** (cluster kinds are view-only).
- **Overview** — failing pods, deployments not ready, recent warning events.
- **Map** (detail drawer) — relationship tree for almost every resource type: e.g. Deployment → ReplicaSets → Pods, Service → Pods, Ingress → Services, ConfigMap/Secret → workloads and pods, PVC ↔ PV ↔ StorageClass, HPA → scale target, namespace summary, and more. Click a row in the map to jump to that resource.
- **Deployment history** — rollout revisions on the **History** tab (Deployments only).

### Logs

- **Single pod** — live tail, time presets (5m–7d), last 100 lines, or since a date/time; search with highlight; level filters; wrap; timestamps; previous container; fullscreen; copy/download.
- **Log workspace** — up to four panes side by side, grid layout, or merged timeline; add pods incrementally from the workspace or from workload actions.

### Change the cluster (optional)

**Edit mode is off by default.** Viewing, logs, and Map always work.

Turn on **Edit mode** (with confirmation) to scale, delete, apply YAML, open a **shell**, or **port-forward**. The server rejects mutations without edit mode. RBAC still applies — blocked actions stay disabled with a short reason. Use `--read-only` to forbid all writes at the process level.

- **YAML** tab — view and edit with line numbers; copy/download exports plain YAML (no gutter).
- **Secrets** — values masked until you choose **Reveal**.

### Productivity

- **Kubeconfig** — drag-and-drop or file picker; `~/.kube/config` / `$KUBECONFIG` loads at start. **Remember this file** stores uploads under `~/.kubelite/kubeconfigs`.
- **Export** table data as CSV or JSON; **PDF** via print.
- **Themes** — light, dark, or system. **⌘K** / **Ctrl+K** command palette. Press **?** in the app for shortcuts.

## Install

Pick one path below. You need **Python 3.10+** and a working `pip`.

| Path | Best for |
| --- | --- |
| [Release wheel](#install-from-github-release) | Most users — install and run, no clone |
| [pipx + release wheel](#install-with-pipx) | Isolated app on your PATH |
| [Clone + pip / venv](#install-from-a-cloned-repo) | Hacking on KubeLite locally |
| [Editable dev install](#development-install) | Running tests and contributing |

### Install from GitHub Release

1. Open [Releases](https://github.com/shubhamsaroj/kubeLite/releases/latest) and note the version tag (e.g. `v1.0.0`) and wheel name (e.g. `kubelite-1.0.0-py3-none-any.whl`).
2. Install the wheel (current stable):

```bash
python3 -m pip install https://github.com/shubhamsaroj/kubeLite/releases/download/v1.0.0/kubelite-1.0.0-py3-none-any.whl
```

3. Start the app:

```bash
kubelite
```

Use `python3 -m kubelite` if the `kubelite` command is not on your PATH.

**Upgrade:** repeat step 2 with the wheel URL from the newer release (same tag and filename pattern: `.../download/vX.Y.Z/kubelite-X.Y.Z-py3-none-any.whl`).

### Install with pipx

[pipx](https://pipx.pypa.io/) keeps KubeLite in its own environment and puts `kubelite` on your PATH.

```bash
pipx install https://github.com/shubhamsaroj/kubeLite/releases/download/v1.0.0/kubelite-1.0.0-py3-none-any.whl
kubelite
```

**Upgrade:** `pipx upgrade kubelite` or reinstall with the newer release wheel URL.

### Install from a cloned repo

```bash
git clone https://github.com/shubhamsaroj/kubeLite.git
cd kubeLite
```

**pipx (from the repo directory)**

```bash
pipx install .
kubelite
```

After you `git pull`: `pipx reinstall .`

**Virtual environment — macOS / Linux**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install .
kubelite
```

**Virtual environment — Windows (PowerShell)**

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install .
kubelite
```

After you change the code in a venv: `pip install .`

### Development install

From a clone of the repo:

```bash
pip install -e ".[dev]"
python -m kubelite
```

## Command-line options

| Flag | Default | Purpose |
| --- | --- | --- |
| `--port N` | `7462` or `$KUBELITE_PORT` | Listen port on localhost |
| `--kubeconfig PATH` | `$KUBECONFIG` or `~/.kube/config` | Kubeconfig to load (**repeatable**) |
| `--read-only` | off | Block scale, delete, YAML apply, shell, port-forward |
| `--no-browser` | off | Do not open a browser tab |
| `--debug` | off | Verbose logging |
| `--version` | — | Print version and exit |

```bash
kubelite --kubeconfig ~/.kube/kubelite-dev.yaml
```

If the default port is busy, KubeLite binds another free port and prints it.

## Security

Designed for a developer machine:

- Binds **127.0.0.1 only** — not exposed on your LAN.
- **HttpOnly** session cookie set on first visit to `/` (no token in the URL for normal use).
- Host/Origin checks, CSP, and anti-framing headers to reduce DNS rebinding and cross-site abuse.
- Mutations require edit mode in the UI (`X-KubeLite-Edit-Mode`); namespaced resources only for writes.
- Remembered kubeconfigs live under `~/.kubelite/` with owner-only permissions.

## Cloud clusters (EKS, GKE, AKS)

If `kubectl get pods` works with your kubeconfig, KubeLite works. Keep cloud auth plugins on your PATH (`aws`, `gke-gcloud-auth-plugin`, `kubelogin`, etc.).

## Try it locally (kind)

Sample workloads and a one-node cluster: **[kind-demo/README.md](kind-demo/README.md)**.

```bash
kind create cluster --config kind-demo/kind-config.yaml --kubeconfig ~/.kube/kubelite-dev.yaml
kubectl --kubeconfig ~/.kube/kubelite-dev.yaml apply -f kind-demo/manifests/
kubelite --kubeconfig ~/.kube/kubelite-dev.yaml
```

## Troubleshooting

| Problem | What to try |
| --- | --- |
| Port in use | Another port is chosen automatically, or set `--port`. |
| Cloud cluster won’t connect | `kubectl get ns` with the same kubeconfig; refresh cloud login / auth plugin. |
| Logs stall on kind/minikube (“inotify watchers”) | Watch limits on the node VM; KubeLite falls back to polling. On the node: `sudo sysctl fs.inotify.max_user_instances=512 fs.inotify.max_user_watches=524288` |
| Blank or stale UI | Hard refresh (`Cmd+Shift+R` / `Ctrl+Shift+R`). Stick to one host (`localhost` or `127.0.0.1`). Reinstall from the [release wheel](#install-from-github-release) or `pipx reinstall kubelite`. Check the browser console (F12). |

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check . && ruff format --check .
```

| Path | Role |
| --- | --- |
| `kubelite/__main__.py` | CLI, port, kubeconfig loading |
| `kubelite/app.py` | HTTP routes, security, WebSockets |
| `kubelite/k8s.py` | Kubernetes API, summaries, Map graph |
| `kubelite/streams.py` | Logs (SSE), watches, port-forward, shell |
| `kubelite/templates/`, `kubelite/static/` | UI (Alpine.js, vendored xterm) |
| `tests/` | pytest |
| `kind-demo/` | Optional local demo cluster |

---

## License

[MIT](LICENSE) © [Shubham Saroj](https://www.linkedin.com/in/shubham-saroj-devops-engineer/)
