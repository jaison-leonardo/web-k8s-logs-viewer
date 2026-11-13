const LEVELS = ["FATAL", "CRITICAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "UNKNOWN"];
const MAX_DOM_LINES = 5000;
const PAGE_SIZE = 500;

const state = {
  environment: null,
  namespace: null,
  pods: {},             // nombre -> resumen del pod (incluye workload y contenedores)
  checked: new Set(),   // "pod/contenedor" marcados en el árbol
  enabledLevels: new Set(LEVELS),
  offset: 0,
  total: 0,
  ws: null,
};

const el = (id) => document.getElementById(id);
const logView = el("logView");

// ------------------------------------------------------------ utilidades
async function api(path, options = {}) {
  const res = await fetch(path, options);
  let body = null;
  try { body = await res.json(); } catch (_) { /* sin cuerpo JSON */ }
  if (!res.ok) {
    const detail = body && body.detail ? body.detail : `HTTP ${res.status}`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return body;
}

function postJSON(path, data) {
  return api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) });
}

function showBanner(message) {
  const banner = el("banner");
  banner.textContent = message || "";
  banner.hidden = !message;
}

function setConn(text, cls) {
  const c = el("connStatus");
  c.textContent = text;
  c.className = `conn ${cls || ""}`;
}

function useUtc() { return el("tzSelect").value === "utc"; }

function fmtTs(ts) {
  if (!ts) return "--:--:--.---";
  const d = new Date(ts.slice(0, 23) + "Z");
  if (isNaN(d.getTime())) return ts;
  const pad = (n, w = 2) => String(n).padStart(w, "0");
  const utc = useUtc();
  const parts = utc
    ? [d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds(), d.getUTCMilliseconds()]
    : [d.getFullYear(), d.getMonth() + 1, d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds(), d.getMilliseconds()];
  return `${parts[0]}-${pad(parts[1])}-${pad(parts[2])} ${pad(parts[3])}:${pad(parts[4])}:${pad(parts[5])}.${pad(parts[6], 3)}`;
}

// datetime-local -> ISO UTC, según la zona elegida en la UI.
function inputToIso(value) {
  if (!value) return "";
  const d = new Date(useUtc() ? `${value}Z` : value);
  return isNaN(d.getTime()) ? "" : d.toISOString();
}

// -------------------------------------------------------- entorno / namespace
async function loadEnvironments() {
  try {
    const data = await api("/api/environments");
    const select = el("envSelect");
    select.innerHTML = "";
    data.environments.forEach((env) => {
      const opt = document.createElement("option");
      opt.value = env.name;
      opt.textContent = env.context && env.context !== env.name ? `${env.name} (${env.context})` : env.name;
      select.appendChild(opt);
    });
    select.value = data.active || data.default;
    await connect(select.value);
  } catch (err) {
    setConn("Error", "err");
    showBanner(err.message);
  }
}

async function connect(environment) {
  setConn("Conectando...", "");
  el("nsSelect").disabled = true;
  try {
    const data = await postJSON("/api/connect", { environment });
    state.environment = data.environment;
    const ns = el("nsSelect");
    ns.innerHTML = "";
    data.namespaces.forEach((name) => {
      const opt = document.createElement("option");
      opt.value = name;
      opt.textContent = name;
      ns.appendChild(opt);
    });
    ns.disabled = data.namespaces.length === 0;
    const remembered = localGet(`ns:${environment}`);
    ns.value = data.namespaces.includes(remembered) ? remembered : (data.default_namespace || data.namespaces[0] || "");
    setConn(`Conectado a ${environment}`, "ok");
    showBanner("");
    await selectNamespace(ns.value);
  } catch (err) {
    state.environment = null;
    setConn("Sin conexión", "err");
    showBanner(`No fue posible conectar con "${environment}": ${err.message}`);
    el("podTree").textContent = "Sin conexión al clúster.";
  }
}

async function selectNamespace(namespace) {
  state.namespace = namespace || null;
  state.checked.clear();
  if (namespace) localSet(`ns:${state.environment}`, namespace);
  await fetchPods();
}

function localGet(key) { try { return localStorage.getItem(key); } catch (_) { return null; } }
function localSet(key, value) { try { localStorage.setItem(key, value); } catch (_) { /* sin storage */ } }

el("envSelect").addEventListener("change", (e) => connect(e.target.value));
el("nsSelect").addEventListener("change", (e) => selectNamespace(e.target.value));

// ------------------------------------------------------------- árbol de pods
let lastWorkloads = null;

async function fetchPods() {
  const tree = el("podTree");
  if (!state.namespace) { tree.textContent = "Selecciona un namespace."; return; }
  tree.textContent = "Cargando...";
  try {
    const data = await api(`/api/pods?namespace=${encodeURIComponent(state.namespace)}`);
    lastWorkloads = data.workloads;
    renderPods(lastWorkloads);
  } catch (err) {
    tree.textContent = "";
    showBanner(`Error consultando pods: ${err.message}`);
  }
}

function renderPods(workloads) {
  const tree = el("podTree");
  tree.innerHTML = "";
  state.pods = {};
  const filter = el("podFilter").value.trim().toLowerCase();
  if (workloads.length === 0) { tree.textContent = "No hay pods en este namespace."; return; }

  workloads.forEach((group) => {
    const visiblePods = group.pods.filter((p) => !filter || group.workload.toLowerCase().includes(filter) || p.name.toLowerCase().includes(filter));
    group.pods.forEach((p) => { state.pods[p.name] = p; });
    if (visiblePods.length === 0) return;

    const wrap = document.createElement("div");
    wrap.className = "workload";
    const title = document.createElement("label");
    title.className = "workload-title";
    const wcb = document.createElement("input");
    wcb.type = "checkbox";
    wcb.title = "Marcar los contenedores por defecto de todos los pods";
    const keys = visiblePods.flatMap((p) => p.default_containers.map((c) => `${p.name}/${c}`));
    wcb.checked = keys.length > 0 && keys.every((k) => state.checked.has(k));
    wcb.addEventListener("change", () => {
      keys.forEach((k) => (wcb.checked ? state.checked.add(k) : state.checked.delete(k)));
      renderPods(workloads);
    });
    const name = document.createElement("span");
    name.textContent = group.workload;
    const kind = document.createElement("span");
    kind.className = "kind";
    kind.textContent = `${group.kind} · ${group.pods.length}`;
    title.append(wcb, name, kind);
    wrap.appendChild(title);

    visiblePods.forEach((pod) => {
      const row = document.createElement("div");
      row.className = "pod-row";
      const dot = document.createElement("span");
      const healthy = pod.status === "Running" || pod.status === "Succeeded";
      dot.className = `status-dot ${healthy ? pod.status : "bad"}`;
      dot.title = pod.status;
      const pname = document.createElement("span");
      pname.className = "pod-name";
      pname.textContent = pod.name;
      const info = document.createElement("span");
      info.textContent = `${pod.ready}${pod.restarts ? ` · ↻${pod.restarts}` : ""}`;
      info.title = `READY ${pod.ready} · reinicios ${pod.restarts} · ${pod.status}`;
      row.append(dot, pname, info);
      wrap.appendChild(row);

      pod.containers.forEach((c) => {
        const key = `${pod.name}/${c.name}`;
        const crow = document.createElement("label");
        crow.className = "container-row";
        const cb = document.createElement("input");
        cb.type = "checkbox";
        cb.checked = state.checked.has(key);
        cb.addEventListener("change", () => (cb.checked ? state.checked.add(key) : state.checked.delete(key)));
        const cname = document.createElement("span");
        cname.className = "cname";
        cname.textContent = c.name;
        cname.title = c.image || "";
        crow.append(cb, cname);
        if (c.tail_state) {
          const pill = document.createElement("span");
          pill.className = `pill ${c.tail_state}`;
          pill.textContent = c.tail_state;
          crow.appendChild(pill);
        }
        if (c.has_previous) {
          const prev = document.createElement("button");
          prev.className = "link-btn";
          prev.textContent = "anterior";
          prev.title = "Descargar los logs de la instancia anterior (antes del último reinicio)";
          prev.addEventListener("click", (e) => { e.preventDefault(); fetchPrevious(pod.name, c.name); });
          crow.appendChild(prev);
        }
        wrap.appendChild(crow);
      });
    });
    tree.appendChild(wrap);
  });
}

el("podFilter").addEventListener("input", () => { if (lastWorkloads) renderPods(lastWorkloads); });

function checkedTargets() {
  const byPod = {};
  state.checked.forEach((key) => {
    const [pod, container] = key.split("/");
    (byPod[pod] = byPod[pod] || []).push(container);
  });
  return Object.entries(byPod).map(([pod, containers]) => ({ pod, containers }));
}

el("refreshPods").addEventListener("click", async () => { await fetchPods(); await refreshTails(); });

el("startTail").addEventListener("click", async () => {
  const targets = checkedTargets();
  if (!state.namespace || targets.length === 0) { showBanner("Marca al menos un contenedor para seguir."); return; }
  try {
    await postJSON("/api/tail/start", { namespace: state.namespace, targets });
    showBanner("");
  } catch (err) { showBanner(`No se pudo iniciar: ${err.message}`); }
  await refreshTails();
  await fetchPods();
});

el("stopTail").addEventListener("click", async () => {
  const ids = [...state.checked].map((key) => `${state.environment}/${state.namespace}/${key}`);
  if (ids.length === 0) return;
  await postJSON("/api/tail/stop", { ids });
  await refreshTails();
  await fetchPods();
});

async function fetchPrevious(pod, container) {
  try {
    const data = await postJSON("/api/tail/previous", { namespace: state.namespace, pod, container });
    showBanner("");
    setTimeout(() => loadHistory(true), 800);
    el("countLabel").textContent = `Instancia anterior de ${pod}/${container}: ${data.lines} registros importados`;
  } catch (err) { showBanner(`Logs anteriores no disponibles: ${err.message}`); }
}

// ----------------------------------------------------------- streams activos
async function refreshTails() {
  let data;
  try { data = await api("/api/tail/status"); } catch (_) { return; }
  const list = el("tailList");
  list.innerHTML = "";
  const tails = data.tails.filter((t) => t.state !== "stopped");
  // Un reemplazo seguido automáticamente (rollout) aún no está en el árbol.
  if (tails.some((t) => t.environment === state.environment && t.namespace === state.namespace && !state.pods[t.pod])) {
    fetchPods();
  }
  if (tails.length === 0) { list.innerHTML = '<span class="dim">Ninguno</span>'; return; }
  tails.forEach((t) => {
    const item = document.createElement("div");
    item.className = "tail-item";
    const name = document.createElement("span");
    name.className = "tname";
    name.textContent = `${t.pod} / ${t.container}`;
    const right = document.createElement("span");
    const pill = document.createElement("span");
    pill.className = `pill ${t.state}`;
    pill.textContent = t.state;
    const stop = document.createElement("button");
    stop.className = "small";
    stop.textContent = "×";
    stop.title = "Detener";
    stop.addEventListener("click", async () => { await postJSON("/api/tail/stop", { ids: [t.id] }); refreshTails(); });
    right.append(pill, " ", stop);
    const meta = document.createElement("span");
    meta.className = "tmeta";
    meta.textContent = `${t.environment} · ${t.namespace} · formato: ${t.format || "?"} · ${t.lines} registros`;
    item.append(name, right, meta);
    if (t.message) {
      const msg = document.createElement("span");
      msg.className = "tmsg";
      msg.textContent = t.message;
      item.appendChild(msg);
    }
    list.appendChild(item);
  });
}

// ------------------------------------------------------------------ filtros
function renderLevelToggles() {
  const wrap = el("levelToggles");
  wrap.innerHTML = "";
  LEVELS.forEach((level) => {
    const label = document.createElement("label");
    label.className = `lvl-${level}`;
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = state.enabledLevels.has(level);
    label.classList.toggle("off", !cb.checked);
    cb.addEventListener("change", () => {
      if (cb.checked) state.enabledLevels.add(level); else state.enabledLevels.delete(level);
      label.classList.toggle("off", !cb.checked);
      onFilterChange();
    });
    label.append(cb, document.createTextNode(level));
    wrap.appendChild(label);
  });
}

async function loadMeta() {
  try {
    const meta = await api("/api/meta");
    fillSelect(el("workloadFilter"), meta.workloads, "Todos los workloads");
    fillSelect(el("formatFilter"), meta.formats, "Todos los formatos");
  } catch (_) { /* no crítico */ }
}

function fillSelect(select, values, emptyLabel) {
  const current = select.value;
  select.innerHTML = "";
  const empty = document.createElement("option");
  empty.value = "";
  empty.textContent = emptyLabel;
  select.appendChild(empty);
  values.forEach((v) => {
    const opt = document.createElement("option");
    opt.value = v;
    opt.textContent = v;
    select.appendChild(opt);
  });
  select.value = values.includes(current) ? current : "";
}

function buildParams() {
  const p = new URLSearchParams();
  if (el("onlySelected").checked) {
    const pods = [...new Set([...state.checked].map((k) => k.split("/")[0]))];
    if (pods.length) p.set("pods", pods.join(","));
  }
  if (state.enabledLevels.size < LEVELS.length) p.set("levels", [...state.enabledLevels].join(","));
  const q = el("searchBox").value.trim();
  if (q) { p.set("q", q); if (el("regexToggle").checked) p.set("regex", "1"); }
  const exclude = el("excludeBox").value.trim();
  if (exclude) p.set("exclude", exclude);
  const trace = el("traceBox").value.trim();
  if (trace) p.set("trace", trace);
  if (el("workloadFilter").value) p.set("workloads", el("workloadFilter").value);
  if (el("formatFilter").value) p.set("formats", el("formatFilter").value);
  const since = inputToIso(el("sinceInput").value);
  if (since) p.set("since", since);
  const until = inputToIso(el("untilInput").value);
  if (until) p.set("until", until);
  return p;
}

let filterDebounce = null;
function onFilterChange() {
  clearTimeout(filterDebounce);
  filterDebounce = setTimeout(() => {
    loadHistory(true);
    if (el("liveToggle").checked) connectWS();
  }, 350);
}

["searchBox", "excludeBox", "traceBox"].forEach((id) => el(id).addEventListener("input", onFilterChange));
["regexToggle", "workloadFilter", "formatFilter", "onlySelected", "sinceInput", "untilInput"]
  .forEach((id) => el(id).addEventListener("change", onFilterChange));
el("tzSelect").addEventListener("change", () => loadHistory(true));

// ----------------------------------------------------------------- render
function isAtBottom() { return logView.scrollHeight - logView.scrollTop - logView.clientHeight < 40; }
function scrollToBottom() { logView.scrollTop = logView.scrollHeight; }

function makeLineEl(row) {
  const div = document.createElement("div");
  div.className = `log-line ${row.level || "UNKNOWN"}`;
  div.dataset.ts = row.ts || "";

  const ts = document.createElement("span");
  ts.className = "ts";
  ts.textContent = fmtTs(row.ts);

  const lvl = document.createElement("span");
  lvl.className = `lvl lvl-${row.level}`;
  lvl.textContent = row.level;

  const src = document.createElement("span");
  src.className = "src";
  // direction:rtl en CSS recorta por la izquierda: se ve el final (hash del pod y contenedor).
  src.textContent = `‎${row.pod}/${row.container}‎`;
  src.title = `${row.namespace} · ${row.workload} · ${row.pod} · ${row.container}${row.logger ? ` · ${row.logger}` : ""}`;

  const msg = document.createElement("span");
  msg.className = "msg";
  const lines = (row.message || "").split("\n");
  msg.textContent = lines[0];
  if (lines.length > 1) {
    const more = document.createElement("span");
    more.className = "more";
    more.textContent = `▸ +${lines.length - 1} líneas`;
    let open = false;
    more.addEventListener("click", () => {
      open = !open;
      msg.firstChild.textContent = open ? row.message : lines[0];
      more.textContent = open ? "▾ contraer" : `▸ +${lines.length - 1} líneas`;
    });
    msg.appendChild(more);
  }

  div.append(ts, lvl, src, msg);
  if (row.trace_id) {
    const trace = document.createElement("span");
    trace.className = "trace";
    trace.textContent = `⛓ ${row.trace_id.slice(0, 12)}`;
    trace.title = `Filtrar por trace/correlation id ${row.trace_id}`;
    trace.addEventListener("click", () => { el("traceBox").value = row.trace_id; onFilterChange(); });
    div.appendChild(trace);
  }
  const fmt = document.createElement("span");
  fmt.className = "fmt";
  fmt.textContent = row.format;
  div.appendChild(fmt);
  return div;
}

// Los registros multilínea se emiten ~0,5 s después (esperan continuaciones), así
// que en vivo pueden llegar después de otros más recientes: se ubican por ts.
function insertChronologically(lineEl) {
  const ts = lineEl.dataset.ts;
  let ref = null;
  let node = logView.lastElementChild;
  for (let steps = 0; ts && node && steps < 300; steps++) {
    if (!node.dataset.ts || node.dataset.ts <= ts) break;
    ref = node;
    node = node.previousElementSibling;
  }
  logView.insertBefore(lineEl, ref);
}

function trimDom(fromTop) {
  while (logView.childElementCount > MAX_DOM_LINES) {
    (fromTop ? logView.firstElementChild : logView.lastElementChild).remove();
  }
}

function updateCount() {
  el("countLabel").textContent = `${state.total.toLocaleString()} registros coinciden · ${logView.childElementCount.toLocaleString()} en pantalla`;
  el("loadMore").hidden = state.offset >= state.total;
}

async function loadHistory(reset) {
  const params = buildParams();
  params.set("limit", String(PAGE_SIZE));
  params.set("offset", String(reset ? 0 : state.offset));
  params.set("order", "desc");
  let data;
  try {
    data = await api(`/api/logs?${params}`);
    el("searchBox").classList.remove("invalid");
  } catch (err) {
    if (err.message.includes("regular")) el("searchBox").classList.add("invalid");
    el("countLabel").textContent = `Error al consultar: ${err.message}`;
    return;
  }
  const rows = data.rows.slice().reverse();
  if (reset) {
    logView.innerHTML = "";
    state.offset = 0;
    rows.forEach((row) => logView.appendChild(makeLineEl(row)));
    if (el("autoscrollToggle").checked) scrollToBottom();
  } else {
    const frag = document.createDocumentFragment();
    rows.forEach((row) => frag.appendChild(makeLineEl(row)));
    const prevHeight = logView.scrollHeight;
    logView.insertBefore(frag, logView.firstChild);
    logView.scrollTop += logView.scrollHeight - prevHeight;
    trimDom(false);
  }
  state.offset += data.rows.length;
  state.total = data.total;
  updateCount();
}

el("loadHistory").addEventListener("click", () => loadHistory(true));
el("loadMore").addEventListener("click", () => loadHistory(false));
el("clearView").addEventListener("click", () => {
  logView.innerHTML = "";
  el("countLabel").textContent = "Vista limpia (los datos siguen guardados; usa Consultar para recargarlos)";
});
el("exportBtn").addEventListener("click", () => {
  const params = buildParams();
  params.set("fmt", el("exportFormat").value);
  window.open(`/api/logs/export?${params}`, "_blank");
});

// --------------------------------------------------------------- websocket
function connectWS() {
  disconnectWS();
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${proto}://${location.host}/ws/logs?${buildParams()}`);
  const status = el("wsStatus");
  ws.onopen = () => { status.textContent = "WS: en vivo"; status.className = "conn ok"; };
  ws.onclose = () => {
    status.textContent = "WS: desconectado";
    status.className = "conn";
    if (state.ws === ws && el("liveToggle").checked) setTimeout(() => { if (state.ws === ws) connectWS(); }, 3000);
  };
  ws.onerror = () => { status.textContent = "WS: error"; status.className = "conn err"; };
  ws.onmessage = (ev) => {
    const row = JSON.parse(ev.data);
    const stick = el("autoscrollToggle").checked && isAtBottom();
    insertChronologically(makeLineEl(row));
    trimDom(true);
    state.total += 1;
    state.offset += 1;
    updateCount();
    if (stick || el("autoscrollToggle").checked && logView.childElementCount < 50) scrollToBottom();
  };
  state.ws = ws;
}

function disconnectWS() {
  if (state.ws) {
    const ws = state.ws;
    state.ws = null;
    ws.close();
  }
  el("wsStatus").textContent = "WS: desconectado";
  el("wsStatus").className = "conn";
}

el("liveToggle").addEventListener("change", () => (el("liveToggle").checked ? connectWS() : disconnectWS()));

// -------------------------------------------------------------------- init
renderLevelToggles();
loadEnvironments();
loadMeta();
loadHistory(true);
if (el("liveToggle").checked) connectWS();
setInterval(refreshTails, 3000);
setInterval(loadMeta, 15000);
refreshTails();
