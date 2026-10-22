import Alpine from "./vendor/alpine.esm.js";

// ponytail: in-browser caps keep memory bounded on chatty pods. Raise together with MAX_INITIAL_LINES in streams.py.
const MAX_LINES = 50000; // per log session, across panes
const RENDER_LINES = 5000; // max DOM lines (single pane); multi-pane uses renderLineCap()
const FLUSH_LINES_PER_FRAME = 100; // cap append work per animation frame while tailing
const MAX_PANES = 4;
const MAX_MERGED = 10;

const NAV = [
  { group: "", items: [["overview", "Overview"], ["workspace", "Log workspace"]] },
  { group: "Workloads", items: [["pods", "Pods"], ["deployments", "Deployments"], ["statefulsets", "StatefulSets"], ["daemonsets", "DaemonSets"], ["replicasets", "ReplicaSets"], ["jobs", "Jobs"], ["cronjobs", "CronJobs"]] },
  { group: "Network", items: [["services", "Services"], ["ingresses", "Ingresses"], ["networkpolicies", "Network policies"]] },
  { group: "Config", items: [["configmaps", "ConfigMaps"], ["secrets", "Secrets"], ["serviceaccounts", "Service accounts"], ["horizontalpodautoscalers", "Autoscalers (HPA)"]] },
  { group: "Storage", items: [["persistentvolumeclaims", "PVCs"]] },
  { group: "Cluster (view only)", items: [["nodes", "Nodes"], ["namespaces", "Namespaces"], ["persistentvolumes", "Persistent volumes"], ["storageclasses", "Storage classes"]] },
  { group: "", items: [["events", "Events"]] },
];
const LABELS = Object.fromEntries(NAV.flatMap((g) => g.items));
const CLUSTER_KINDS = new Set(["nodes", "namespaces", "persistentvolumes", "storageclasses"]);
const NO_STATUS = new Set(["configmaps", "secrets", "serviceaccounts", "networkpolicies", "horizontalpodautoscalers", "storageclasses"]);
const HAS_PODS = new Set(["deployments", "statefulsets", "daemonsets", "replicasets", "jobs"]);
const GRAPH_SKIP = new Set(["overview", "workspace", "connect"]);
const GRAPH_KINDS = new Set(NAV.flatMap((g) => g.items.map((i) => i[0])).filter((p) => !GRAPH_SKIP.has(p)));
const SCALABLE = new Set(["deployments", "statefulsets", "replicasets"]);
const RESTARTABLE = new Set(["deployments", "statefulsets", "daemonsets"]);
const RANGES = [
  ["live", "Live from now"],
  ["5m", "5m"],
  ["15m", "15m"],
  ["1h", "1h"],
  ["6h", "6h"],
  ["24h", "24h"],
  ["48h", "48h"],
  ["7d", "7 days"],
  ["tail", "Last 100"],
  ["since", "Since date/time…"],
];
const SINCE_SECONDS = { "5m": 300, "15m": 900, "1h": 3600, "6h": 21600, "24h": 86400, "48h": 172800, "7d": 604800 };
const LEVELS = ["ERROR", "WARN", "INFO", "DEBUG"];
const LEVEL_RE = /\b(fatal|panic|critical|error|err|warn(?:ing)?|info|debug|trace)\b/i;
const PANE_COLORS = ["var(--c1)", "var(--c2)", "var(--c3)", "var(--c4)"];

function levelOf(text) {
  const m = LEVEL_RE.exec(text.slice(0, 160));
  if (!m) return "INFO";
  const w = m[1].toLowerCase();
  if (["fatal", "panic", "critical", "error", "err"].includes(w)) return "ERROR";
  if (w.startsWith("warn")) return "WARN";
  if (w === "debug" || w === "trace") return "DEBUG";
  return "INFO";
}

let clusterEditMode = false;

function sessionHeaders(headers = {}) {
  const h = { ...headers };
  const session = sessionStorage.getItem("kubelite.session");
  if (session) h["X-KubeLite-Session"] = session;
  if (clusterEditMode) h["X-KubeLite-Edit-Mode"] = "1";
  return h;
}

async function api(path, opts = {}) {
  const headers = sessionHeaders({ "Content-Type": "application/json", ...(opts.headers || {}) });
  const res = await fetch(path, { ...opts, headers, credentials: "same-origin" });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

function age(iso, now) {
  if (!iso) return "-";
  const s = Math.max(0, Math.floor((now - Date.parse(iso)) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h${Math.floor((s % 3600) / 60) ? Math.floor((s % 3600) / 60) + "m" : ""}`;
  return `${Math.floor(s / 86400)}d`;
}

function csvCell(value) {
  let s = String(value ?? "");
  if (/^[=+\-@\t\r]/.test(s)) s = "'" + s; // stop spreadsheet formula injection
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function download(name, text, type = "text/plain") {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = Object.assign(document.createElement("a"), { href: url, download: name });
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const stamp = () => new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const tsKey = (ts) => { const [h, f = ""] = ts.replace("Z", "").split("."); return `${h}.${f.padEnd(9, "0")}`; };

// Log lines live outside Alpine's reactivity: tens of thousands of proxied objects would make every update slow.
const LOG = {
  entries: [],
  es: null,
  targets: [],
  pending: [],
  frame: 0,
  rendered: {},
  hitMarks: [],
  counts: { ERROR: 0, WARN: 0, INFO: 0, DEBUG: 0 },
  total: 0,
  re: null,
  reSig: "",
  statsTimer: 0,
  hitSyncTimer: 0,
};

/** @param {{ re: RegExp, literal?: string, ignoreCase?: boolean } | null} search */
function fillMessageEl(m, text, highlight, search) {
  if (!highlight || !search) {
    m.textContent = text;
    return;
  }
  m.replaceChildren();
  if (search.literal) {
    const q = search.literal;
    const hay = search.ignoreCase ? text.toLowerCase() : text;
    const needle = search.ignoreCase ? q.toLowerCase() : q;
    let i = 0;
    let pos = 0;
    while ((pos = hay.indexOf(needle, i)) !== -1) {
      if (pos > i) m.append(text.slice(i, pos));
      const mk = document.createElement("mark");
      mk.textContent = text.slice(pos, pos + q.length);
      m.append(mk);
      i = pos + q.length;
    }
    if (i < text.length) m.append(text.slice(i));
    return;
  }
  const re = search.re;
  let last = 0;
  re.lastIndex = 0;
  for (const hit of text.matchAll(re)) {
    if (!hit[0]) break;
    if (hit.index > last) m.append(text.slice(last, hit.index));
    const mk = document.createElement("mark");
    mk.textContent = hit[0];
    m.append(mk);
    last = hit.index + hit[0].length;
  }
  if (last < text.length) m.append(text.slice(last));
  re.lastIndex = 0;
}
let term = null; // { term, fit, ws, observer }
let watchSource = null;
const confirmRuns = new Map();
let confirmTokenSeq = 0;

Alpine.data("kubelite", () => ({
  NAV, LABELS, RANGES, LEVELS, PANE_COLORS, MAX_PANES,
  version: "", readOnly: false, editModeRequired: true,
  editMode: sessionStorage.getItem("kubelite.editMode") === "1",
  theme: localStorage.getItem("kubelite.theme") || "system",
  now: Date.now(),
  contexts: [], ctx: "", namespaces: [], canListNs: true, ns: "",
  view: "",
  // table
  columns: [], rows: [], loading: false, watchError: "", lastSync: 0,
  filter: "", statusFilter: "All", sortKey: "name", sortDir: 1, picked: [], hpaTargets: [], workspaceAddMode: false,
  // overview
  ov: null,
  // drawer
  sel: null, tab: "overview", detail: null, detailError: "", events: [], perms: {}, permsLoading: false, editing: false, yamlDraft: "", revealed: null,
  _lastWatchErr: "",
  history: [], graph: null, graphLoading: false, scaleTo: 0, wide: false,
  drawerDock: localStorage.getItem("kubelite.drawerDock") === "bottom" ? "bottom" : "right",
  drawerWidthPx: Number(localStorage.getItem("kubelite.drawerW")) || 0,
  drawerHeightPx: Number(localStorage.getItem("kubelite.drawerH")) || 0,
  // dialogs
  confirm: null, pf: null, forwards: [], palette: { open: false, q: "", i: 0 }, help: false,
  toasts: [], dropOver: false, remember: false, bootError: "", ready: false,
  // logs
  L: {
    panes: [], layout: "cols", range: "15m", sinceWhen: "", activePane: 0, searchScope: "all", query: "", caseS: false, regex: false, onlyMatch: false,
    levels: { ERROR: true, WARN: true, INFO: true, DEBUG: true }, follow: true, wrap: false, showTs: true,
    counts: { ERROR: 0, WARN: 0, INFO: 0, DEBUG: 0 }, hits: 0, hitIdx: -1, reError: false, waiting: 0, full: false, total: 0,
    paneHitLabels: [],
  },

  async init() {
    this.applyTheme();
    matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => this.applyTheme());
    setInterval(() => (this.now = Date.now()), 5000);
    document.addEventListener("keydown", (e) => this.onKey(e));
    document.addEventListener("fullscreenchange", () => (this.L.full = !!document.fullscreenElement));
    this.$watch("view", () => this.$nextTick(() => this.placeViewer()));
    this.$watch("tab", () => this.$nextTick(() => this.placeViewer()));
    this.$watch("sel", () => this.$nextTick(() => this.placeViewer()));
    let st;
    try {
      st = await api("/api/state");
    } catch (e) {
      this.bootError =
        e.message.includes("Not signed in")
          ? "Session missing or expired. Close this tab and open the full link from your terminal (with ?token=…)."
          : e.message;
      this.view = "connect";
      this.toast(this.bootError, "err");
      this.ready = true;
      return;
    }
    this.version = st.version;
    this.readOnly = st.readOnly;
    this.editModeRequired = st.editModeRequired !== false;
    if (this.readOnly) this.editMode = false;
    clusterEditMode = this.editMode && !this.readOnly;
    this.contexts = st.contexts;
    this.forwards = st.forwards;
    const fromHash = this.parseHash();
    const saved = fromHash?.ctx || localStorage.getItem("kubelite.ctx");
    const pick = this.contexts.find((c) => c.id === saved) || this.contexts.find((c) => c.current) || this.contexts[0];
    if (!pick) {
      this.view = "connect";
      this.ready = true;
      return;
    }
    try {
      await this.selectContext(pick.id, fromHash ? "workspace" : localStorage.getItem("kubelite.view") || "overview");
      if (fromHash?.panes?.length) {
        this.L.panes = fromHash.panes;
        this.connectLogs();
      }
    } catch (e) {
      this.view = "connect";
      this.bootError = e.message;
      this.toast(e.message, "err");
    } finally {
      this.ready = true;
    }
  },

  // ------------------------------------------------------------------ theme, toasts, helpers
  applyTheme() {
    localStorage.setItem("kubelite.theme", this.theme);
    const dark = this.theme === "dark" || (this.theme === "system" && matchMedia("(prefers-color-scheme: dark)").matches);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    if (term) term.term.options.theme = this.termTheme();
  },
  dismissToast(id) {
    this.toasts = this.toasts.filter((x) => x.id !== id);
  },
  toast(msg, kind = "info") {
    const id = crypto.randomUUID?.() ?? Math.random();
    this.toasts.push({ id, msg, kind });
    const ms = kind === "err" ? 8000 : kind === "ok" ? 3500 : 4000;
    setTimeout(() => this.dismissToast(id), ms);
  },
  age(iso) { return age(iso, this.now); },
  q(extra = {}) {
    const p = new URLSearchParams({ ctx: this.ctx, ns: this.ns || "_all", ...extra });
    const session = sessionStorage.getItem("kubelite.session");
    if (session) p.set("session", session);
    return p.toString();
  },
  get ctxInfo() { return this.contexts.find((c) => c.id === this.ctx) || null; },
  get isKind() { return !["overview", "workspace", "connect"].includes(this.view); },
  get showNsCol() { return this.isKind && !CLUSTER_KINDS.has(this.view) && this.ns === "_all"; },
  get showStatus() { return !NO_STATUS.has(this.view); },
  get canWrite() { return !this.readOnly && this.editMode; },

  setEditMode(on) {
    this.editMode = !!on;
    clusterEditMode = this.editMode && !this.readOnly;
    try {
      sessionStorage.setItem("kubelite.editMode", this.editMode ? "1" : "0");
    } catch {
      /* ponytail: storage may be blocked in some profiles; in-memory edit mode still works */
    }
    if (!on) {
      this.closeTerminal();
      if (this.tab === "shell") this.tab = "overview";
    }
  },
  promptEnableEdit(continueFn) {
    if (this.readOnly) return;
    this.ask({
      title: "Enable edit mode?",
      message:
        "Viewing is always safe. To change the cluster, turn edit mode on first. You can turn it off again from the top bar.",
      confirmLabel: continueFn ? "Enable and continue" : "Enable edit mode",
      run: async () => {
        this.setEditMode(true);
        if (typeof continueFn === "function") this.$nextTick(continueFn);
        return { message: "Edit mode is on. Cluster changes are allowed until you turn it off." };
      },
    });
  },
  toggleEditMode() {
    if (this.readOnly) return;
    if (this.editMode) {
      this.ask({
        title: "Turn off edit mode?",
        message: "You will not be able to change the cluster until you enable it again. Open shells are closed.",
        confirmLabel: "Turn off",
        run: async () => {
          this.setEditMode(false);
          return { message: "Edit mode is off." };
        },
      });
    } else {
      this.promptEnableEdit();
    }
  },
  guardEdit(continueFn) {
    if (this.readOnly) return false;
    if (this.editMode) return true;
    this.promptEnableEdit(continueFn);
    return false;
  },

  // ------------------------------------------------------------------ contexts & namespaces
  async selectContext(id, view) {
    this.ctx = id;
    localStorage.setItem("kubelite.ctx", id);
    this.closeDrawer();
    this.stopLogs();
    this.L.panes = [];
    const info = this.ctxInfo;
    try {
      const res = await api(`/api/namespaces?ctx=${encodeURIComponent(id)}`);
      this.namespaces = res.namespaces;
      this.canListNs = res.canList;
    } catch (e) {
      this.namespaces = [];
      this.toast(`Could not connect to ${info?.name}: ${e.message}`, "err");
    }
    const savedNs = localStorage.getItem(`kubelite.ns.${id}`);
    this.ns = savedNs || info?.namespace || "default";
    this.go(view || (this.view === "connect" ? "overview" : this.view));
  },
  setNs(ns) {
    this.ns = ns;
    localStorage.setItem(`kubelite.ns.${this.ctx}`, ns);
    this.picked = [];
    this.closeDrawer();
    this.go(this.view);
  },
  async onDrop(ev) {
    this.dropOver = false;
    const file = ev.dataTransfer?.files?.[0] || ev.target?.files?.[0];
    if (!file) return;
    if (file.size > 1_000_000) return this.toast("That file is larger than 1 MB; kubeconfigs are much smaller.", "err");
    try {
      const res = await api("/api/kubeconfigs", { method: "POST", body: JSON.stringify({ content: await file.text(), name: file.name, remember: this.remember }) });
      this.contexts = res.contexts;
      const first = res.contexts.find((c) => c.source === res.source);
      this.toast(`Loaded ${file.name}`, "ok");
      if (first) await this.selectContext(first.id, "overview");
    } catch (e) {
      this.toast(e.message, "err");
    }
    if (ev.target?.value) ev.target.value = "";
  },
  async removeSource(sourceId) {
    try {
      const res = await api(`/api/kubeconfigs/${sourceId}`, { method: "DELETE" });
      this.contexts = res.contexts;
    } catch (e) {
      this.toast(e.message, "err");
    }
  },

  // ------------------------------------------------------------------ navigation
  go(view) {
    if (view !== "connect" && !this.ctx) view = "connect";
    if (this.view !== view) this.closeDrawer();
    if (view !== "pods") this.workspaceAddMode = false;
    if (this.view === "workspace" && view !== "workspace") this.stopLogs();
    this.view = view;
    if (view === "workspace" && this.L.panes.length) {
      this.$nextTick(() => {
        this.placeViewer();
        if (!LOG.es) this.connectLogs();
      });
    }
    if (!["connect", "workspace"].includes(view)) localStorage.setItem("kubelite.view", view);
    this.stopWatch();
    this.filter = "";
    this.statusFilter = "All";
    this.sortKey = view === "events" ? "last" : "name";
    this.sortDir = view === "events" ? -1 : 1;
    if (view === "overview") this.loadOverview();
    else if (this.isKind) this.startWatch(view);
  },
  stopWatch() {
    if (watchSource) watchSource.close();
    watchSource = null;
    clearTimeout(this._ovTimer);
  },
  startWatch(plural) {
    this.rows = [];
    this.columns = [];
    this.loading = true;
    this.watchError = "";
    this._lastWatchErr = "";
    const es = new EventSource(`/api/watch/${plural}?${this.q()}`);
    watchSource = es;
    const touch = () => (this.lastSync = Date.now());
    es.addEventListener("snapshot", (e) => {
      const d = JSON.parse(e.data);
      this.columns = d.columns;
      this.rows = d.rows;
      this.loading = false;
      this.watchError = "";
      touch();
      this.syncSelection();
    });
    es.addEventListener("upsert", (e) => {
      const row = JSON.parse(e.data);
      const i = this.rows.findIndex((r) => r.uid === row.uid);
      if (i >= 0) this.rows[i] = row;
      else this.rows.push(row);
      touch();
      this.syncSelection();
    });
    es.addEventListener("delete", (e) => {
      const row = JSON.parse(e.data);
      this.rows = this.rows.filter((r) => r.uid !== row.uid);
      touch();
    });
    es.addEventListener("failure", (e) => {
      const d = JSON.parse(e.data);
      this.watchError = d.error || "Could not load this resource.";
      this.loading = false;
      if (this.watchError !== this._lastWatchErr) {
        this._lastWatchErr = this.watchError;
        this.toast(this.watchError, "err");
      }
    });
    es.onerror = () => {
      if (!this.loading) return;
      this.loading = false;
      if (!this.watchError) {
        this.watchError = "Could not load this list. Check your connection or permissions for this namespace.";
        this.toast(this.watchError, "err");
      }
    };
    if (SCALABLE.has(plural)) this.loadHpaTargets();
  },
  syncSelection() {
    if (this.sel && this.sel.plural === this.view) {
      const fresh = this.rows.find((r) => r.uid === this.sel.row.uid);
      if (fresh) this.sel.row = fresh;
    }
  },
  async loadHpaTargets() {
    try {
      this.hpaTargets = (await api(`/api/hpa-targets?${this.q()}`)).targets;
    } catch { this.hpaTargets = []; }
  },
  async loadOverview() {
    if (this.view !== "overview") return;
    try {
      this.ov = await api(`/api/overview?${this.q()}`);
      this.lastSync = Date.now();
    } catch (e) {
      this.toast(e.message, "err");
    }
    clearTimeout(this._ovTimer);
    this._ovTimer = setTimeout(() => this.loadOverview(), 10000);
  },

  // ------------------------------------------------------------------ table
  get visibleRows() {
    const f = this.filter.trim().toLowerCase();
    let rows = this.rows;
    if (f) rows = rows.filter((r) => [r.name, r.namespace, r.status, ...r.cells].join(" ").toLowerCase().includes(f));
    if (this.view === "pods" && this.statusFilter !== "All") {
      const want = { Running: (r) => r.status === "Running", Pending: (r) => r.tone === "warn", Failing: (r) => r.tone === "bad" }[this.statusFilter];
      rows = rows.filter(want);
    }
    const key = this.sortKey, dir = this.sortDir;
    const val = (r) => (typeof key === "number" ? r.cells[key] : key === "age" ? -Date.parse(r.created || 0) : key === "last" ? r.last || r.created || "" : r[key] ?? "");
    return [...rows].sort((a, b) => {
      const x = val(a), y = val(b);
      return (typeof x === "number" && typeof y === "number" ? x - y : String(x).localeCompare(String(y), undefined, { numeric: true })) * dir;
    });
  },
  sortBy(key) {
    this.sortDir = this.sortKey === key ? -this.sortDir : 1;
    this.sortKey = key;
  },
  togglePick(row) {
    const i = this.picked.findIndex((p) => p.uid === row.uid);
    if (i >= 0) this.picked.splice(i, 1);
    else this.picked.push(row);
  },
  isPicked(row) { return this.picked.some((p) => p.uid === row.uid); },
  exportRows(format) {
    const rows = this.visibleRows;
    const name = `kubelite-${this.view}-${stamp()}`;
    if (format === "json") return download(`${name}.json`, JSON.stringify(rows, null, 2), "application/json");
    const header = ["Name", ...(this.showNsCol ? ["Namespace"] : []), ...(this.showStatus ? ["Status"] : []), ...this.columns, "Age"];
    const lines = rows.map((r) => [r.name, ...(this.showNsCol ? [r.namespace] : []), ...(this.showStatus ? [r.status] : []), ...r.cells, this.age(this.view === "events" ? r.last : r.created)]);
    download(`${name}.csv`, [header, ...lines].map((l) => l.map(csvCell).join(",")).join("\n"), "text/csv");
  },

  // ------------------------------------------------------------------ drawer
  beginWorkspaceAdd() {
    this.workspaceAddMode = true;
    this.go("pods");
  },
  commitWorkspaceAdds() {
    if (!this.picked.length) return this.toast("Tick one or more pods in the list first", "err");
    this.workspaceAddMode = false;
    this.addToWorkspace([...this.picked], true);
  },
  cancelWorkspaceAdd() {
    this.workspaceAddMode = false;
    this.picked = [];
    if (this.L.panes.length) this.go("workspace");
  },
  async open(plural, row, tab) {
    if (this.workspaceAddMode && plural === "pods") {
      this.togglePick(row);
      return;
    }
    if (this.editing && !confirm("Discard your unsaved YAML edits?")) return;
    this.closeTerminal();
    this.sel = { plural, row };
    this.editing = false;
    this.revealed = null;
    this.detail = null;
    this.events = [];
    this.history = [];
    this.graph = null;
    this.graphLoading = false;
    this.perms = {};
    this.permsLoading = false;
    this.detailError = "";
    this.scaleTo = row.replicas ?? 0;
    this.tab = tab || (plural === "pods" ? "logs" : "overview");
    if (plural === "pods" && this.tab === "logs") this.openPodLogs(row);
    this.loadDetail();
  },
  async loadDetail() {
    const { plural, row } = this.sel;
    const ns = row.namespace || "_";
    const base = `/api/r/${plural}/${ns}/${row.name}`;
    this.detailError = "";
    this.permsLoading = !this.readOnly && !CLUSTER_KINDS.has(plural) && plural !== "events";
    this.graphLoading = GRAPH_KINDS.has(plural);
    try {
      const graphP = GRAPH_KINDS.has(plural)
        ? api(`${base}/graph?${this.q()}`).catch(() => ({ sections: [] }))
        : Promise.resolve(null);
      const [detail, ev, graph] = await Promise.all([
        api(`${base}?${this.q()}`),
        api(`${base}/events?${this.q()}`).catch(() => ({ events: [] })),
        graphP,
      ]);
      if (this.sel?.row.uid !== row.uid) return;
      this.detail = detail;
      this.events = ev.events;
      this.graph = graph;
      if (this.permsLoading) {
        this.perms = (await api(`/api/permissions/${plural}/${ns}?${this.q()}`)).allowed || {};
      }
      if (plural === "deployments") this.history = (await api(`${base}/history?${this.q()}`)).history;
    } catch (e) {
      this.detailError = e.message;
      this.toast(e.message, "err");
    } finally {
      this.permsLoading = false;
      this.graphLoading = false;
    }
  },
  kindLabel(plural) { return LABELS[plural] || plural; },
  graphRows() {
    const rows = [];
    for (const sec of this.graph?.sections || []) {
      rows.push({ type: "section", title: sec.title });
      const walk = (nodes, depth) => {
        for (const n of nodes || []) {
          rows.push({ type: "node", depth, plural: n.plural, row: n.row, hint: n.hint || "" });
          walk(n.children, depth + 1);
        }
      };
      walk(sec.nodes, 0);
    }
    return rows;
  },
  openFromMap(plural, row) {
    if (!row?.uid) return;
    if (this.view !== plural) this.go(plural);
    this.open(plural, row, "map");
  },
  toggleDrawerDock() {
    this.drawerDock = this.drawerDock === "right" ? "bottom" : "right";
    localStorage.setItem("kubelite.drawerDock", this.drawerDock);
  },
  drawerStyle() {
    if (this.drawerDock === "bottom") {
      if (this.drawerHeightPx > 0) return `height:${this.drawerHeightPx}px;width:100%;max-width:none`;
      return "height:min(42vh, 520px);width:100%;max-width:none";
    }
    if (this.drawerWidthPx > 0) return `width:${this.drawerWidthPx}px;max-width:92vw`;
    if (this.wide) return "width:min(72vw, 1100px);max-width:92vw";
    return "";
  },
  beginDrawerResize(e) {
    const drawer = e.target.closest(".drawer");
    const bottom = this.drawerDock === "bottom";
    const startX = e.clientX;
    const startY = e.clientY;
    const startW = drawer?.offsetWidth || 560;
    const startH = drawer?.offsetHeight || 400;
    const move = (ev) => {
      if (bottom) {
        this.drawerHeightPx = Math.min(window.innerHeight * 0.85, Math.max(200, startH + (startY - ev.clientY)));
      } else {
        this.drawerWidthPx = Math.min(window.innerWidth * 0.92, Math.max(300, startW + (startX - ev.clientX)));
      }
    };
    const up = () => {
      if (bottom) localStorage.setItem("kubelite.drawerH", String(this.drawerHeightPx));
      else localStorage.setItem("kubelite.drawerW", String(this.drawerWidthPx));
      document.removeEventListener("mousemove", move);
      document.removeEventListener("mouseup", up);
      document.body.classList.remove("resizing-drawer", "resizing-drawer-y");
    };
    document.body.classList.add("resizing-drawer");
    if (bottom) document.body.classList.add("resizing-drawer-y");
    document.addEventListener("mousemove", move);
    document.addEventListener("mouseup", up);
  },
  closeDrawer() {
    if (this.editing && this.sel && !confirm("Discard your unsaved YAML edits?")) return;
    this.closeTerminal();
    if (this.view !== "workspace") this.stopLogs();
    this.sel = null;
    this.editing = false;
  },
  get tabs() {
    if (!this.sel) return [];
    const p = this.sel.plural;
    const t = ["overview"];
    if (GRAPH_KINDS.has(p)) t.push("map");
    t.push("yaml", "events");
    if (p === "pods") t.splice(1, 0, "logs", "shell");
    if (p === "deployments") t.push("history");
    return t;
  },
  setTab(t) {
    if (t === "shell" && !this.guardEdit(() => this.setTab(t))) return;
    if (this.tab === "shell" && t !== "shell") this.closeTerminal();
    this.tab = t;
    if (t === "logs" && this.sel?.plural === "pods" && !this.isPaneOpen(this.sel.row)) this.openPodLogs(this.sel.row);
    if (t === "shell") this.$nextTick(() => this.openTerminal());
  },
  allowed(action) {
    return !this.rbacBlocked(action) && this.editMode && !this.readOnly;
  },
  rbacBlocked(action) {
    if (this.readOnly) return true;
    if (this.permsLoading) return true;
    return this.perms[action] === false;
  },
  actionNeedsEdit(action) {
    return !this.readOnly && !this.editMode && !this.rbacBlocked(action);
  },
  actionHint(action) {
    if (this.readOnly) return "KubeLite was started with --read-only";
    if (!this.editMode && !this.rbacBlocked(action)) return "Click to enable edit mode, then you can use this";
    if (this.permsLoading) return "Checking your Kubernetes permissions…";
    if (this.perms[action] === false) return "Your Kubernetes role does not allow this action";
    if (this.perms[action] !== true && this.editMode) return "Could not verify permissions for this action";
    return "";
  },
  denyTitle(action) {
    return this.actionHint(action);
  },
  get selCan() {
    if (!this.sel) return {};
    const { plural, row } = this.sel;
    const ns = !CLUSTER_KINDS.has(plural) && plural !== "events";
    const hpa = this.hpaTargets.includes(`${row.namespace}/${{ deployments: "Deployment", statefulsets: "StatefulSet", replicasets: "ReplicaSet" }[plural]}/${row.name}`);
    return {
      edit: ns,
      delete: ns,
      restartPod: plural === "pods" && !!row.controller && row.phase !== "Succeeded",
      restart: RESTARTABLE.has(plural),
      scale: SCALABLE.has(plural),
      hpa,
      logs: HAS_PODS.has(plural),
      pf: (plural === "pods" && row.status === "Running") || (plural === "services" && row.hasSelector),
      cron: plural === "cronjobs",
      secret: plural === "secrets",
    };
  },
  overviewPairs() {
    if (!this.sel) return [];
    const { plural, row } = this.sel;
    const pairs = [["Name", row.name]];
    if (row.namespace) pairs.push(["Namespace", row.namespace]);
    if (this.showStatusFor(plural)) pairs.push(["Status", row.status]);
    const cols = this.columns.length && this.view === plural ? this.columns : [];
    cols.forEach((c, i) => pairs.push([c, String(row.cells[i] ?? "-")]));
    if (row.controller) pairs.push(["Controlled by", row.controller]);
    if (row.containers) pairs.push(["Containers", row.containers.join(", ")]);
    if (row.selector) pairs.push(["Selector", row.selector]);
    pairs.push(["Created", `${row.created || "-"} (${this.age(row.created)} ago)`]);
    return pairs;
  },
  showStatusFor(plural) { return !NO_STATUS.has(plural); },

  // ------------------------------------------------------------------ actions
  ask(opts) {
    const token = ++confirmTokenSeq;
    if (typeof opts.run === "function") confirmRuns.set(token, opts.run);
    const { run, typeName, ...rest } = opts;
    const conf = { token, typed: "", busy: false, ...rest };
    if (typeName != null && typeName !== "") conf.typeName = typeName;
    this.confirm = conf;
  },
  confirmSubmitDisabled() {
    const c = this.confirm;
    if (!c || c.busy === true) return true;
    if (c.typeName) return c.typed !== c.typeName;
    return false;
  },
  cancelConfirm() {
    if (this.confirm?.token) confirmRuns.delete(this.confirm.token);
    this.confirm = null;
  },
  async runConfirm() {
    const c = this.confirm;
    if (!c || this.confirmSubmitDisabled()) return;
    const run = confirmRuns.get(c.token);
    if (!run) {
      this.toast("Could not run this confirmation.", "err");
      return;
    }
    c.busy = true;
    try {
      const res = await run.call(this);
      confirmRuns.delete(c.token);
      if (res?.message) this.toast(res.message, "ok");
      this.confirm = null;
    } catch (e) {
      c.busy = false;
      this.toast(e.message, "err");
    }
  },
  path(plural, row, suffix = "") { return `/api/r/${plural}/${row.namespace}/${row.name}${suffix}?${this.q()}`; },
  doDelete() {
    if (!this.guardEdit(() => this.doDelete())) return;
    const { plural, row } = this.sel;
    const isPod = plural === "pods";
    this.ask({
      title: `Delete ${LABELS[plural].replace(/s$/, "")} ${row.name}?`,
      message: isPod && row.controller ? "Its controller will create a replacement pod." : "This cannot be undone.",
      ...(isPod ? {} : { typeName: row.name }),
      danger: true, confirmLabel: "Delete", force: false,
      run: async () => {
        const res = await api(this.path(plural, row) + (this.confirm.force ? "&force=1" : ""), { method: "DELETE" });
        this.closeDrawer();
        return res;
      },
    });
  },
  doRestart() {
    if (!this.guardEdit(() => this.doRestart())) return;
    const { plural, row } = this.sel;
    this.ask({
      title: plural === "pods" ? `Restart pod ${row.name}?` : `Rolling restart of ${row.name}?`,
      message: plural === "pods" ? "The pod is deleted and its controller starts a fresh one." : "Pods are replaced a few at a time, like kubectl rollout restart.",
      confirmLabel: "Restart",
      run: () => api(this.path(plural, row, "/restart"), { method: "POST", body: "{}" }),
    });
  },
  doScale() {
    if (!this.guardEdit(() => this.doScale())) return;
    const { plural, row } = this.sel;
    const n = Number(this.scaleTo);
    if (!Number.isInteger(n) || n < 0 || n > 1000) return this.toast("Replicas must be a whole number from 0 to 1000", "err");
    this.ask({
      title: `Scale ${row.name} from ${row.replicas} to ${n}?`,
      message: n === 0 ? "Scaling to 0 stops every pod of this workload." : "",
      danger: n === 0, confirmLabel: "Scale",
      run: () => api(this.path(plural, row, "/scale"), { method: "POST", body: JSON.stringify({ replicas: n }) }),
    });
  },
  doSuspend() {
    if (!this.guardEdit(() => this.doSuspend())) return;
    const { row } = this.sel;
    const suspend = !row.suspended;
    this.ask({
      title: `${suspend ? "Suspend" : "Resume"} CronJob ${row.name}?`,
      message: suspend ? "No new jobs will be scheduled until you resume it." : "Jobs will run on schedule again.",
      confirmLabel: suspend ? "Suspend" : "Resume",
      run: () => api(this.path("cronjobs", row, "/suspend"), { method: "POST", body: JSON.stringify({ suspend }) }),
    });
  },
  doTrigger() {
    if (!this.guardEdit(() => this.doTrigger())) return;
    const { row } = this.sel;
    this.ask({
      title: `Run ${row.name} now?`, message: "Creates a Job from the CronJob template, like kubectl create job --from.", confirmLabel: "Run now",
      run: () => api(this.path("cronjobs", row, "/trigger"), { method: "POST", body: "{}" }),
    });
  },
  doRollback(rev) {
    if (!this.guardEdit(() => this.doRollback(rev))) return;
    const { row } = this.sel;
    this.ask({
      title: `Roll ${row.name} back to revision ${rev}?`, message: "Its pod template is replaced with that revision's and a rollout starts.", confirmLabel: "Roll back",
      run: async () => {
        const res = await api(this.path("deployments", row, "/rollback"), { method: "POST", body: JSON.stringify({ revision: rev }) });
        setTimeout(() => this.loadDetail(), 1500);
        return res;
      },
    });
  },
  async revealSecret() {
    const { row } = this.sel;
    try {
      this.revealed = (await api(this.path("secrets", row, "/values"))).values;
    } catch (e) { this.toast(e.message, "err"); }
  },
  async startEdit() {
    if (!this.guardEdit(() => this.startEdit())) return;
    const { plural, row } = this.sel;
    try {
      const d = await api(this.path(plural, row) + (plural === "secrets" ? "&reveal=1" : ""));
      this.yamlDraft = d.yaml;
      this.editing = true;
      this.wide = true;
      this.tab = "yaml";
    } catch (e) { this.toast(e.message, "err"); }
  },
  async applyYaml() {
    if (!this.guardEdit(() => this.applyYaml())) return;
    const { plural, row } = this.sel;
    try {
      const res = await api(this.path(plural, row), { method: "PUT", body: JSON.stringify({ yaml: this.yamlDraft }) });
      this.toast(res.message, "ok");
      this.editing = false;
      this.loadDetail();
    } catch (e) { this.toast(e.message, "err"); }
  },
  yamlBodyText() {
    if (this.editing) return this.yamlDraft ?? "";
    if (!this.detail) return "Loading…";
    return this.detail.yaml ?? "";
  },
  yamlLineGutter(text) {
    const n = Math.max(1, String(text ?? "").split("\n").length);
    return Array.from({ length: n }, (_, i) => i + 1).join("\n");
  },
  dl(name, text) { download(name, text); },
  copy(text) {
    navigator.clipboard.writeText(text).then(() => this.toast("Copied", "ok"), () => this.toast("Copy failed", "err"));
  },
  async logsForWorkload() {
    const { plural, row } = this.sel;
    try {
      const pods = (await api(this.path(plural, row, "/pods"))).pods;
      if (!pods.length) return this.toast("This workload has no pods right now", "err");
      if (pods.length > MAX_MERGED) this.toast(`Adding the first ${MAX_MERGED} of ${pods.length} pods`);
      const merge = this.L.panes.length > 0;
      this.addToWorkspace(pods.slice(0, MAX_MERGED), merge);
    } catch (e) { this.toast(e.message, "err"); }
  },

  // ------------------------------------------------------------------ port-forward
  openPf() {
    if (!this.guardEdit(() => this.openPf())) return;
    const { plural, row } = this.sel;
    const ports = plural === "pods" ? row.ports.map((p) => ({ port: p.port, label: `${p.port}${p.name ? " (" + p.name + ")" : ""} · ${p.container}` })) : row.ports.map((p) => ({ port: p.port, label: `${p.port}${p.name ? " (" + p.name + ")" : ""} → ${p.targetPort}` }));
    this.pf = { plural, row, ports, port: ports[0]?.port || "", localPort: "", busy: false };
  },
  async startPf() {
    const p = this.pf;
    const port = Number(p.port), localPort = p.localPort ? Number(p.localPort) : null;
    if (!Number.isInteger(port) || port < 1 || port > 65535) return this.toast("Pick a port between 1 and 65535", "err");
    p.busy = true;
    try {
      const res = await api(`/api/portforward?${this.q()}`, { method: "POST", body: JSON.stringify({ kind: p.plural, namespace: p.row.namespace, name: p.row.name, port, localPort }) });
      this.forwards = res.forwards;
      this.pf = null;
      this.toast(`Forwarding ${res.forward.url} → ${res.forward.target}:${port}`, "ok");
    } catch (e) {
      p.busy = false;
      this.toast(e.message, "err");
    }
  },
  async stopPf(id) {
    try { this.forwards = (await api(`/api/portforward/${id}`, { method: "DELETE" })).forwards; } catch (e) { this.toast(e.message, "err"); }
  },

  // ------------------------------------------------------------------ terminal
  termTheme() {
    const css = getComputedStyle(document.documentElement);
    const v = (n) => css.getPropertyValue(n).trim();
    return { background: v("--sunken"), foreground: v("--fg"), cursor: v("--accent"), selectionBackground: v("--fill1") };
  },
  openTerminal(container) {
    this.closeTerminal();
    const el = this.$refs.term;
    const row = this.sel?.row;
    if (!el || !row) return;
    container = container || this.shellContainer || row.containers[0];
    this.shellContainer = container;
    const t = new window.Terminal({ cursorBlink: true, fontSize: 12, fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", theme: this.termTheme() });
    const fit = new window.FitAddon.FitAddon();
    t.loadAddon(fit);
    t.open(el);
    fit.fit();
    const qs = new URLSearchParams({ ctx: this.ctx, ns: row.namespace, pod: row.name, container });
    const session = sessionStorage.getItem("kubelite.session");
    if (session) qs.set("session", session);
    if (clusterEditMode) qs.set("edit", "1");
    const ws = new WebSocket(`ws://${location.host}/ws/exec?${qs}`);
    const resize = () => {
      try { fit.fit(); } catch {}
      if (ws.readyState === 1) ws.send(JSON.stringify({ type: "resize", cols: t.cols, rows: t.rows }));
    };
    ws.onopen = () => { resize(); t.focus(); };
    ws.onmessage = (e) => t.write(typeof e.data === "string" ? e.data : new Uint8Array(e.data));
    ws.onclose = () => t.write("\r\n\x1b[90m[disconnected]\x1b[0m\r\n");
    t.onData((d) => ws.readyState === 1 && ws.send(JSON.stringify({ type: "input", data: d })));
    const observer = new ResizeObserver(resize);
    observer.observe(el);
    term = { term: t, fit, ws, observer };
  },
  closeTerminal() {
    if (!term) return;
    term.observer.disconnect();
    term.ws.close();
    term.term.dispose();
    term = null;
  },

  // ------------------------------------------------------------------ logs: session & stream
  paneFor(row) {
    return { ns: row.namespace, pod: row.name, containers: row.containers || [], container: "all", status: row.status, tone: row.tone, searchIn: true };
  },
  isPaneOpen(row) { return this.L.panes.length === 1 && this.L.panes[0].pod === row.name && this.L.panes[0].ns === row.namespace; },
  openPodLogs(row) {
    this.L.panes = [this.paneFor(row)];
    this.L.layout = "cols";
    this.connectLogs();
  },
  addToWorkspace(rows, merge = false) {
    if (!rows?.length) return this.toast("No pods to add", "err");
    this.closeDrawer();
    const panes = merge ? [...this.L.panes] : [];
    let added = 0;
    for (const r of rows) {
      if (!r?.name || !r?.namespace) continue;
      if (panes.some((p) => p.pod === r.name && p.ns === r.namespace)) continue;
      if (panes.length >= MAX_MERGED) {
        this.toast(`Up to ${MAX_MERGED} pods per workspace`, "err");
        break;
      }
      panes.push(this.paneFor(r));
      added++;
    }
    if (!added && !panes.length) return this.toast("No pods were added", "err");
    if (!added && panes.length) {
      this.toast("Those pods are already in the workspace", "info");
      this.go("workspace");
      return;
    }
    this.L.panes = panes;
    if (this.L.panes.length > MAX_PANES) this.L.layout = "merged";
    else if (this.L.panes.length > 1 && this.L.layout === "merged") this.L.layout = "cols";
    this.picked = [];
    this.stopLogs();
    this.go("workspace");
    if (added) this.toast(`Added ${added} pod${added > 1 ? "s" : ""} to log workspace`, "ok");
  },
  removePane(i) {
    this.L.panes.splice(i, 1);
    this.connectLogs();
  },
  stopLogs() {
    if (LOG.es) LOG.es.close();
    LOG.es = null;
    LOG.entries = [];
    LOG.pending = [];
    LOG.rendered = {};
    LOG.hitMarks = [];
    LOG.counts = { ERROR: 0, WARN: 0, INFO: 0, DEBUG: 0 };
    LOG.total = 0;
    if (LOG.statsTimer) clearTimeout(LOG.statsTimer);
    if (LOG.hitSyncTimer) clearTimeout(LOG.hitSyncTimer);
    LOG.statsTimer = 0;
    LOG.hitSyncTimer = 0;
  },
  rangeParams() {
    const r = this.L.range;
    if (r === "live") return { tail: 0 };
    if (r === "tail") return { tail: 100 };
    if (r === "since") {
      if (!this.L.sinceWhen) return { since: 3600 };
      const at = Date.parse(this.L.sinceWhen);
      if (!Number.isFinite(at)) return { since: 3600 };
      return { since: Math.max(1, Math.min(31 * 24 * 3600, Math.round((Date.now() - at) / 1000))) };
    }
    const sec = SINCE_SECONDS[r];
    return sec ? { since: sec } : { since: 900 };
  },
  renderLineCap() {
    const n = Math.max(1, this.L.panes.length);
    if (this.L.layout === "merged" && n > 1) return 3500;
    if (n <= 1) return RENDER_LINES;
    if (n === 2) return 2000;
    if (n === 3) return 1400;
    return 1000;
  },
  compileLogRe() {
    const q = this.L.query;
    if (!q) {
      LOG.re = null;
      LOG.reSig = "";
      return null;
    }
    const sig = `${q}\0${this.L.caseS}\0${this.L.regex}`;
    if (LOG.reSig === sig) return LOG.re;
    LOG.reSig = sig;
    try {
      LOG.re = new RegExp(this.L.regex ? q : q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), this.L.caseS ? "g" : "gi");
    } catch {
      LOG.re = null;
    }
    return LOG.re;
  },
  logSearchOpts() {
    const re = this.compileLogRe();
    if (!re || !this.L.query) return null;
    if (this.L.regex) return { re };
    return { re, literal: this.L.query, ignoreCase: !this.L.caseS };
  },
  scheduleLogStats() {
    if (LOG.statsTimer) return;
    LOG.statsTimer = window.setTimeout(() => {
      LOG.statsTimer = 0;
      this.L.counts = { ...LOG.counts };
      this.L.total = LOG.total;
    }, 350);
  },
  scheduleHitSync() {
    if (LOG.hitSyncTimer) return;
    LOG.hitSyncTimer = window.setTimeout(() => {
      LOG.hitSyncTimer = 0;
      this.syncHitMarks();
      this.refreshPaneHitLabels();
    }, 400);
  },
  refreshPaneHitLabels() {
    if (!this.L.query || !this.compileLogRe()) {
      this.L.paneHitLabels = [];
      return;
    }
    const labels = [];
    for (let i = 0; i < this.L.panes.length; i++) labels[i] = this.paneHits(i);
    this.L.paneHitLabels = labels;
  },
  connectLogs() {
    this.stopLogs();
    LOG.counts = { ERROR: 0, WARN: 0, INFO: 0, DEBUG: 0 };
    LOG.total = 0;
    Object.assign(this.L.counts, LOG.counts);
    this.L.hits = 0;
    this.L.hitIdx = -1;
    this.L.waiting = 0;
    this.L.total = 0;
    this.L.paneHitLabels = [];
    this.L.follow = true;
    LOG.targets = [];
    const params = new URLSearchParams({ ctx: this.ctx });
    this.L.panes.forEach((p, pane) => {
      const names = p.container === "all" ? p.containers : [p.container.replace(/:previous$/, "")];
      const previous = p.container.endsWith(":previous");
      for (const c of names) {
        LOG.targets.push({ pane, container: c, multi: names.length > 1 });
        params.append("target", `${p.ns}/${p.pod}/${c}${previous ? "/previous" : ""}`);
      }
    });
    const rp = this.rangeParams();
    for (const [k, v] of Object.entries(rp)) if (v !== undefined) params.set(k, v);
    this.renderLogs();
    if (!LOG.targets.length) return;
    const es = new EventSource(`/api/logs?${params}`);
    LOG.es = es;
    es.onmessage = (e) => this.onLogBatch(JSON.parse(e.data));
    es.onerror = () => {
      if (es.readyState === EventSource.CLOSED) this.toast("Log stream closed", "err");
    };
  },
  onLogBatch(batch) {
    const fresh = [];
    let prevK = LOG.entries.at(-1)?.k ?? "";
    let inOrder = true;
    let toastedLogErr = false;
    for (const [i, ts, payload] of batch) {
      const t = LOG.targets[i];
      if (!t) continue;
      let entry;
      if (typeof payload === "string") {
        const lvl = levelOf(payload);
        LOG.counts[lvl]++;
        entry = { p: t.pane, c: t.multi ? t.container : "", ts, text: payload, lvl };
      } else {
        const where = t.multi ? ` (${t.container})` : "";
        let text, kind = "";
        if (payload.error) {
          text = `${payload.error}${where}`;
          kind = "err";
          if (payload.final && !toastedLogErr) {
            this.toast(payload.error, "err");
            toastedLogErr = true;
          }
        }
        else if (payload.warning) { text = payload.warning; kind = "warn"; }
        else if (payload.end === "exited") text = `Container${where} stopped; waiting for it to restart…`;
        else if (payload.end === "nologs") text = `No logs yet${where}: the container has not started. Waiting…`;
        else if (payload.end === "previous") text = `End of the previous container's logs${where}.`;
        else continue;
        entry = { p: t.pane, c: "", ts: ts || "", text, sys: kind || "info", lvl: "SYS" };
      }
      entry.k = entry.ts ? tsKey(entry.ts) : prevK;
      if (entry.k < prevK) inOrder = false;
      prevK = entry.k;
      LOG.entries.push(entry);
      fresh.push(entry);
    }
    // Backlogs from several containers arrive one after another; keep one time-ordered list (stable sort,
    // near-linear on mostly sorted input) so panes and the merged timeline read top-to-bottom in time.
    if (!inOrder) LOG.entries.sort((a, b) => (a.k < b.k ? -1 : a.k > b.k ? 1 : 0));
    LOG.total = LOG.entries.length;
    this.scheduleLogStats();
    if (LOG.entries.length > MAX_LINES * 1.1) {
      LOG.entries.splice(0, LOG.entries.length - MAX_LINES);
      return this.renderLogs();
    }
    if (!inOrder && this.L.follow) return this.renderLogs();
    if (!this.L.follow) {
      this.L.waiting += fresh.filter((e) => this.matches(e)).length;
      return;
    }
    LOG.pending.push(...fresh);
    if (!LOG.frame) LOG.frame = requestAnimationFrame(() => { LOG.frame = 0; this.flushLogs(); });
  },

  // ------------------------------------------------------------------ logs: filtering & rendering
  get logRe() {
    return this.compileLogRe();
  },
  matches(e, re = this.compileLogRe()) {
    if (e.sys) return true;
    if (!this.L.levels[e.lvl]) return false;
    // "Only matching" applies per search scope — other panes stay full when Active/Picked.
    if (this.L.onlyMatch && re && this.entryPaneInSearchScope(e.p)) {
      re.lastIndex = 0;
      const ok = re.test(e.text);
      re.lastIndex = 0;
      return ok;
    }
    return true;
  },
  lineEl(e, re, merged) {
    const div = document.createElement("div");
    div.className = `ln lvl-${e.lvl}` + (e.sys ? ` sys ${e.sys}` : "");
    if (e.ts) {
      const t = document.createElement("span");
      t.className = "t";
      t.textContent = e.ts.slice(11, 23);
      t.title = e.ts;
      div.append(t);
    }
    if (merged || e.c) {
      const s = document.createElement("span");
      s.className = "src";
      const p = this.L.panes[e.p];
      s.textContent = merged ? `${p ? p.pod.replace(/-[a-z0-9]{8,10}-[a-z0-9]{5}$|-[a-z0-9]{5}$/, "") : "?"}${e.c ? "/" + e.c : ""}` : e.c;
      s.title = p ? `${p.pod}${e.c ? " / " + e.c : ""}` : "";
      s.style.color = PANE_COLORS[e.p % PANE_COLORS.length];
      div.append(s);
    }
    const m = document.createElement("span");
    m.className = "m";
    const search = this.logSearchOpts();
    fillMessageEl(m, e.text, !!(search && !e.sys && this.entryPaneInSearchScope(e.p)), search);
    div.append(m);
    return div;
  },
  entryPaneInSearchScope(p) {
    if (this.L.searchScope === "all" || this.L.panes.length <= 1) return true;
    if (this.L.searchScope === "active") return String(p) === String(this.L.activePane);
    if (this.L.searchScope === "selected") return !!this.L.panes[p]?.searchIn;
    return true;
  },
  paneSearchHitsVisible(i) {
    if (!this.L.query) return false;
    if (this.L.searchScope === "all" || this.L.panes.length <= 1 || this.L.layout === "merged") return true;
    if (this.L.searchScope === "active") return String(i) === String(this.L.activePane);
    return !!this.L.panes[i]?.searchIn;
  },
  bodies() {
    const v = document.getElementById("logviewer");
    return v ? [...v.querySelectorAll("[data-pane]")] : [];
  },
  renderLogs() {
    LOG.pending = [];
    const re = this.compileLogRe();
    const cap = this.renderLineCap();
    this.L.reError = !!this.L.query && !re;
    this.L.waiting = 0;
    const merged = this.L.layout === "merged" && this.L.panes.length > 1;
    LOG.rendered = {};
    for (const body of this.bodies()) {
      const key = body.dataset.pane;
      let list = merged ? LOG.entries : LOG.entries.filter((e) => String(e.p) === key);
      list = list.filter((e) => this.matches(e, re)).slice(-cap);
      LOG.rendered[key] = list;
      const frag = document.createDocumentFragment();
      for (const e of list) frag.append(this.lineEl(e, re, merged));
      body.replaceChildren(frag);
    }
    this.syncHitMarks();
    this.refreshPaneHitLabels();
    this.scrollToEnd();
  },
  rehighlightLogs() {
    const search = this.logSearchOpts();
    this.L.reError = !!this.L.query && !search?.re;
    if (this.L.onlyMatch) return this.renderLogs();
    for (const body of this.bodies()) {
      const key = body.dataset.pane;
      const list = LOG.rendered[key];
      if (!list?.length) continue;
      const kids = body.children;
      if (kids.length !== list.length) return this.renderLogs();
      for (let i = 0; i < list.length; i++) {
        const e = list[i];
        const m = kids[i]?.querySelector?.(".m");
        if (!m) continue;
        fillMessageEl(m, e.text, !!(search && !e.sys && this.entryPaneInSearchScope(e.p)), search);
      }
    }
    this.syncHitMarks();
    this.refreshPaneHitLabels();
  },
  flushLogs() {
    let fresh = LOG.pending;
    LOG.pending = [];
    if (!fresh.length) return;
    if (fresh.length > FLUSH_LINES_PER_FRAME) {
      LOG.pending = fresh.slice(FLUSH_LINES_PER_FRAME);
      fresh = fresh.slice(0, FLUSH_LINES_PER_FRAME);
    }
    const re = this.compileLogRe();
    const merged = this.L.layout === "merged" && this.L.panes.length > 1;
    const cap = this.renderLineCap();
    for (const body of this.bodies()) {
      const key = body.dataset.pane;
      if (!LOG.rendered[key]) LOG.rendered[key] = [];
      const frag = document.createDocumentFragment();
      const shown = [];
      for (const e of fresh) {
        if ((merged || String(e.p) === key) && this.matches(e, re)) {
          shown.push(e);
          frag.append(this.lineEl(e, re, merged));
        }
      }
      if (!shown.length) continue;
      LOG.rendered[key] = [...LOG.rendered[key], ...shown].slice(-cap);
      body.append(frag);
      while (body.childElementCount > cap) {
        body.firstElementChild.remove();
        if (LOG.rendered[key].length) LOG.rendered[key].shift();
      }
    }
    if (this.L.query && re) {
      if (this.L.follow) this.scheduleHitSync();
      else {
        this.syncHitMarks();
        this.refreshPaneHitLabels();
      }
    }
    this.scrollToEnd();
    if (LOG.pending.length && !LOG.frame) {
      LOG.frame = requestAnimationFrame(() => {
        LOG.frame = 0;
        this.flushLogs();
      });
    }
  },
  scrollToEnd() {
    if (!this.L.follow) return;
    for (const body of this.bodies()) body.scrollTop = body.scrollHeight;
  },
  onLogWheel(e) {
    if (e.deltaY < 0 && this.L.follow) this.L.follow = false;
  },
  setFollow(on) {
    this.L.follow = on;
    if (on) this.renderLogs();
  },
  syncHitMarks() {
    const re = this.compileLogRe();
    if (!this.L.query || !re) {
      LOG.hitMarks = [];
      this.L.hits = 0;
      this.L.hitIdx = -1;
      return;
    }
    const marks = [];
    for (const body of this.bodies()) marks.push(...body.querySelectorAll("mark"));
    LOG.hitMarks = marks;
    this.L.hits = marks.length;
    if (this.L.hitIdx >= marks.length) this.L.hitIdx = marks.length - 1;
  },
  paneHits(i) {
    const re = this.compileLogRe();
    if (!this.L.query || !re) return 0;
    const list = LOG.rendered[String(i)];
    if (!list?.length) return 0;
    let n = 0;
    for (const e of list) {
      if (e.sys || !this.entryPaneInSearchScope(e.p)) continue;
      re.lastIndex = 0;
      if (re.test(e.text)) n++;
      re.lastIndex = 0;
    }
    return n;
  },
  setSearchScope(scope) {
    this.L.searchScope = scope;
    if (scope === "selected" && this.L.panes.length > 1 && !this.L.panes.some((p) => p.searchIn)) {
      const i = Math.max(0, Number(this.L.activePane) || 0);
      if (this.L.panes[i]) this.L.panes[i].searchIn = true;
    }
    this.L.hitIdx = -1;
    for (const m of LOG.hitMarks) m.classList.remove("cur");
    // Scope changes which panes are line-filtered (Only matching) — re-render, not just rehighlight.
    if (this.L.onlyMatch && this.L.query) this.renderLogs();
    else this.rehighlightLogs();
  },
  focusLogPane(key) {
    const prev = this.L.activePane;
    this.L.activePane = key;
    this.L.hitIdx = -1;
    for (const m of LOG.hitMarks) m.classList.remove("cur");
    if (this.L.query && this.L.searchScope === "active" && String(prev) !== String(key)) {
      if (this.L.onlyMatch) this.renderLogs();
      else this.rehighlightLogs();
    } else if (this.L.query) this.syncHitMarks();
  },
  togglePaneSearch(i) {
    const p = this.L.panes[i];
    if (!p) return;
    p.searchIn = !p.searchIn;
    this.L.searchScope = "selected";
    this.L.hitIdx = -1;
    for (const m of LOG.hitMarks) m.classList.remove("cur");
    if (this.L.onlyMatch && this.L.query) this.renderLogs();
    else this.rehighlightLogs();
  },
  nextHit(step) {
    const marks = LOG.hitMarks;
    if (!marks.length) return;
    this.L.follow = false;
    marks[this.L.hitIdx]?.classList.remove("cur");
    this.L.hitIdx = (this.L.hitIdx + step + marks.length) % marks.length;
    const m = marks[this.L.hitIdx];
    m.classList.add("cur");
    m.scrollIntoView({ block: "center" });
  },
  onQuery() {
    clearTimeout(this._qTimer);
    this._qTimer = setTimeout(() => {
      this.L.hitIdx = -1;
      for (const m of LOG.hitMarks) m.classList.remove("cur");
      if (this.L.onlyMatch) this.renderLogs();
      else this.rehighlightLogs();
    }, 220);
  },
  setRange(r) {
    this.L.range = r;
    if (r === "since" && !this.L.sinceWhen) {
      const d = new Date(Date.now() - 3600 * 1000);
      this.L.sinceWhen = new Date(d.getTime() - d.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
    }
    this.connectLogs();
  },
  setLayout(l) {
    this.L.layout = l;
    this.$nextTick(() => this.renderLogs());
  },
  get gridCols() {
    const n = this.L.panes.length;
    if (this.L.layout === "merged" || n <= 1) return "1fr";
    return this.L.layout === "grid" ? "repeat(2, minmax(0, 1fr))" : `repeat(${Math.min(n, MAX_PANES)}, minmax(0, 1fr))`;
  },
  get logPanes() {
    if (this.L.layout === "merged" && this.L.panes.length > 1) return [{ key: "m" }];
    return this.L.panes.slice(0, MAX_PANES).map((p, i) => ({ key: String(i), i, p }));
  },
  logText() {
    const merged = this.L.panes.length > 1;
    return LOG.entries.filter((e) => !e.sys && this.matches(e)).map((e) => {
      const p = this.L.panes[e.p];
      const src = merged ? `[${p?.pod}${e.c ? "/" + e.c : ""}] ` : e.c ? `[${e.c}] ` : "";
      return `${this.L.showTs ? e.ts + " " : ""}${src}${e.text}`;
    }).join("\n");
  },
  downloadLogs() {
    const name = this.L.panes.length === 1 ? this.L.panes[0].pod : "workspace";
    download(`${name}-${stamp()}.log`, this.logText() + "\n");
  },
  toggleFullscreen() {
    const v = document.getElementById("logviewer");
    if (document.fullscreenElement) return document.exitFullscreen();
    if (this.L.full) return (this.L.full = false);
    // Native full screen can be refused (no user gesture, iframes, policies); fill the window instead.
    Promise.resolve(v?.requestFullscreen?.()).then(() => (this.L.full = true), () => (this.L.full = true));
  },
  parseHash() {
    const m = location.hash.match(/^#logs=(.+)$/);
    if (!m) return null;
    history.replaceState(null, "", "/");
    try {
      const d = JSON.parse(decodeURIComponent(m[1]));
      const panes = (d.panes || []).slice(0, MAX_MERGED).filter((p) => Array.isArray(p) && p.length === 3).map(([ns, pod, containers]) => ({ ns: String(ns), pod: String(pod), containers: (containers || []).map(String), container: "all", searchIn: true }));
      return { ctx: String(d.ctx || ""), panes };
    } catch { return null; }
  },
  placeViewer() {
    const viewer = document.getElementById("logviewer");
    const home = document.getElementById("logviewer-home");
    if (!viewer || !home) return;
    let target = home;
    if (this.view === "workspace") target = document.getElementById("workspace-slot");
    else if (this.sel?.plural === "pods" && this.tab === "logs") target = document.getElementById("drawer-logs-slot");
    if (target && viewer.parentElement !== target) {
      target.append(viewer);
      this.$nextTick(() => this.renderLogs());
    }
  },

  // ------------------------------------------------------------------ palette & keyboard
  get paletteItems() {
    const q = this.palette.q.toLowerCase();
    const items = [
      ...Object.entries(LABELS).map(([id, label]) => ({ label, hint: "Go to", run: () => this.go(id) })),
      ...this.namespaces.map((n) => ({ label: n, hint: "Namespace", run: () => this.setNs(n) })),
      ...this.contexts.map((c) => ({ label: c.name, hint: "Cluster", run: () => this.selectContext(c.id) })),
      ...(this.isKind ? this.rows.map((r) => ({ label: r.name, hint: LABELS[this.view], run: () => this.open(this.view, r) })) : []),
      { label: "Toggle light / dark theme", hint: "Theme", run: () => { this.theme = document.documentElement.dataset.theme === "dark" ? "light" : "dark"; this.applyTheme(); } },
      { label: "Add a kubeconfig", hint: "Clusters", run: () => this.go("connect") },
    ];
    return (q ? items.filter((i) => i.label.toLowerCase().includes(q) || i.hint.toLowerCase().includes(q)) : items).slice(0, 50);
  },
  openPalette() {
    this.palette = { open: true, q: "", i: 0 };
    this.$nextTick(() => this.$refs.paletteInput?.focus());
  },
  runPalette(item) {
    item = item || this.paletteItems[this.palette.i];
    this.palette.open = false;
    item?.run();
  },
  onKey(e) {
    const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName) || e.target.closest?.(".xterm");
    const mod = e.metaKey || e.ctrlKey;
    if (mod && e.key.toLowerCase() === "k") { e.preventDefault(); return this.openPalette(); }
    const logsVisible = this.view === "workspace" || (this.sel?.plural === "pods" && this.tab === "logs");
    if (mod && e.key.toLowerCase() === "f" && logsVisible) { e.preventDefault(); return this.$refs.logSearch?.focus(); }
    if (e.key === "Escape") {
      if (this.palette.open) return (this.palette.open = false);
      if (this.confirm) return this.cancelConfirm();
      if (this.pf) return (this.pf = null);
      if (this.help) return (this.help = false);
      if (document.fullscreenElement) return;
      if (this.L.full) return (this.L.full = false);
      if (typing) return e.target.blur();
      if (this.sel) return this.closeDrawer();
      return;
    }
    if (typing || mod || e.altKey) return;
    if (this._g) {
      this._g = false;
      const to = { o: "overview", p: "pods", d: "deployments", s: "services", e: "events", w: "workspace", n: "nodes" }[e.key];
      if (to) return this.go(to);
    }
    if (e.key === "g") return (this._g = true);
    if (e.key === "/") { e.preventDefault(); return (logsVisible ? this.$refs.logSearch : this.$refs.filter)?.focus(); }
    if (e.key === "?") return (this.help = true);
    if (e.key === "F" && logsVisible) return this.toggleFullscreen();
    if (e.key === " " && logsVisible) { e.preventDefault(); return this.setFollow(!this.L.follow); }
    if (this.sel && this.tabs.includes({ l: "logs", y: "yaml", s: "shell", e: "events", o: "overview" }[e.key])) return this.setTab({ l: "logs", y: "yaml", s: "shell", e: "events", o: "overview" }[e.key]);
  },
}));

Alpine.start();
