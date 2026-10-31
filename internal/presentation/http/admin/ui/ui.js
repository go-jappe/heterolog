// 管理用APIのブラウザ画面です。tokenはsessionStorageに置き、fetchのAuthorization headerで
// 送ります。画面の文字はdataを値として入れ（textContent）、HTMLとして解釈しません。
"use strict";

const STRINGS = {
  en: {
    token: "Admin token", connect: "Connect", disconnect: "Forget token",
    tab_status: "Status", tab_settings: "Settings", tab_config: "Config",
    refresh: "Refresh", auto_refresh: "Refresh every 5 s", save: "Save changes", config_path: "Path",
    need_token: "Enter the admin token and press Connect.", unauthorized: "Unauthorized: check the token.",
    failed: "Request failed", saved: "Saved.", no_changes: "No changes to save.", updated: "Updated",
    process: "Process", warnings: "Warnings", no_warnings: "No warnings", members: "Members", directories: "Directories",
    uptime: "Uptime", cpu: "CPU", memory: "Memory (RSS / heap)", host_memory: "Host memory used / limit", goroutines: "Goroutines",
    backend: "Destination", records: "Records ok / failed", requests: "Requests (avg / max)", retries: "Retries",
    breaker: "Circuit breaker", open: "open", closed: "closed", spool: "Spool active / dead / quarantine", replay: "Resend",
    last_purge: "Last purge", path: "Path", role: "Role", usage: "Files / size", limit: "Limit", disk_free: "Disk free / total",
    not_scanned: "not scanned yet", logger: "Logger", output: "Output", level: "Level", sampling: "Sampling",
    by_request_id: "By request ID", request_id_rate: "Request ID rate", debug_rate: "DEBUG rate", info_rate: "INFO rate",
    warn_rate: "WARN rate", error_rate: "ERROR rate", kind: "Kind", member: "Member", never: "never",
  },
  ja: {
    token: "管理用token", connect: "接続", disconnect: "tokenを消す",
    tab_status: "状態", tab_settings: "設定の変更", tab_config: "設定の一覧",
    refresh: "更新", auto_refresh: "5秒ごとに更新", save: "変更を保存", config_path: "path",
    need_token: "管理用tokenを入れて「接続」を押してください。", unauthorized: "認証に失敗しました。tokenを確かめてください。",
    failed: "要求に失敗しました", saved: "保存しました。", no_changes: "変更はありません。", updated: "更新",
    process: "process", warnings: "注意", no_warnings: "注意すべき状態はありません", members: "member", directories: "directory",
    uptime: "起動からの時間", cpu: "CPU", memory: "memory（RSS / heap）", host_memory: "hostのmemory 使用 / 上限", goroutines: "goroutine",
    backend: "出力先", records: "record 成功 / 失敗", requests: "送信（平均 / 最大）", retries: "再試行",
    breaker: "circuit breaker", open: "開", closed: "閉", spool: "spool active / dead / quarantine", replay: "再送",
    last_purge: "前回のpurge", path: "path", role: "role", usage: "file数 / 大きさ", limit: "上限", disk_free: "diskの空き / 総量",
    not_scanned: "未scan", logger: "logger", output: "出力先", level: "level", sampling: "sampling",
    by_request_id: "request IDごと", request_id_rate: "request IDの割合", debug_rate: "DEBUGの割合", info_rate: "INFOの割合",
    warn_rate: "WARNの割合", error_rate: "ERRORの割合", kind: "種類", member: "member", never: "なし",
  },
};

let lang = "en";
try { lang = localStorage.getItem("heterolog.lang") || "en"; } catch (e) { /* 保存できない環境では既定のままです。 */ }
if (!STRINGS[lang]) lang = "en";

function t(key) { return (STRINGS[lang] && STRINGS[lang][key]) || STRINGS.en[key] || key; }

function el(tag, attrs, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs || {})) {
    if (key === "class") node.className = value;
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

function applyLanguage() {
  document.documentElement.lang = lang;
  for (const node of document.querySelectorAll("[data-i18n]")) node.textContent = t(node.dataset.i18n);
  for (const button of document.querySelectorAll("[data-lang]")) button.setAttribute("aria-pressed", String(button.dataset.lang === lang));
}

// ---- token and API ----

function token() { try { return sessionStorage.getItem("heterolog.token") || ""; } catch (e) { return ""; } }

async function api(method, path, body) {
  const headers = { Authorization: "Bearer " + token() };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body), cache: "no-store" });
  let document_ = null;
  try { document_ = await response.json(); } catch (e) { /* 本文のない応答です。 */ }
  if (response.status === 401) throw new Error(t("unauthorized"));
  if (!response.ok) throw new Error(t("failed") + ": " + response.status + (document_ && document_.error ? " " + document_.error : ""));
  return document_;
}

function showMessage(text, kind) {
  const node = document.getElementById("message");
  node.textContent = text;
  node.className = "message" + (kind ? " " + kind : "");
  node.hidden = !text;
}

// ---- formatting ----

function fmtBytes(n) {
  if (n === null || n === undefined) return "-";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let value = Number(n), unit = 0;
  while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit++; }
  return (unit === 0 ? value : value.toFixed(1)) + " " + units[unit];
}
function fmtSeconds(s) {
  if (s === null || s === undefined) return "-";
  if (s < 1) return (s * 1000).toFixed(0) + " ms";
  if (s < 90) return s.toFixed(1) + " s";
  const minutes = Math.floor(s / 60), hours = Math.floor(minutes / 60), days = Math.floor(hours / 24);
  if (days > 0) return days + "d " + (hours % 24) + "h";
  if (hours > 0) return hours + "h " + (minutes % 60) + "m";
  return minutes + "m " + Math.floor(s % 60) + "s";
}
function fmtTime(iso) {
  if (!iso || iso.startsWith("0001-")) return t("never");
  const date = new Date(iso);
  return isNaN(date) ? iso : date.toLocaleString();
}
function fmtPercent(ratio) { return (ratio * 100).toFixed(1) + "%"; }

// ---- status ----

function warningClassFor(warnings, target) {
  let result = "";
  for (const warning of warnings) {
    if (warning.target !== target) continue;
    if (warning.severity === "critical") return "critical";
    result = "warning";
  }
  return result;
}

function renderStatus(report) {
  const body = document.getElementById("status-body");
  body.replaceChildren();
  const warnings = report.warnings || [];

  body.append(el("h2", {}, t("warnings")));
  if (warnings.length === 0) body.append(el("span", { class: "none" }, t("no_warnings")));
  else body.append(el("ul", { class: "warnings" }, warnings.map((w) =>
    el("li", { class: w.severity }, el("code", {}, w.severity.toUpperCase() + " " + w.code), w.target + ": " + w.message))));

  const p = report.process || {};
  body.append(el("h2", {}, t("process")));
  const processRows = [
    [t("uptime"), fmtSeconds(p.uptime_seconds)],
    [t("cpu"), p.cpu ? p.cpu.percent.toFixed(1) + "% (" + fmtSeconds(p.cpu.window_seconds) + ")" : "-"],
    [t("memory"), p.memory ? fmtBytes(p.memory.rss_bytes) + " / " + fmtBytes(p.memory.heap_bytes) : "-"],
    [t("host_memory"), p.memory && p.memory.limit_bytes ? fmtBytes(p.memory.used_bytes) + " / " + fmtBytes(p.memory.limit_bytes) + " (" + fmtPercent(p.memory.ratio) + ", " + p.memory.source + ")" : "-"],
    [t("goroutines"), p.goroutines],
  ];
  body.append(el("table", {}, processRows.map(([k, v]) => el("tr", {}, el("th", {}, k), el("td", {}, v)))));

  body.append(el("h2", {}, t("members")));
  for (const [name, member] of Object.entries(report.members || {})) {
    body.append(el("h3", {}, name + " (" + member.kind + ")"));
    const head = el("tr", {}, [t("backend"), t("records"), t("requests"), t("retries"), t("breaker"), t("spool"), t("replay"), t("last_purge")].map((h) => el("th", {}, h)));
    const rows = (member.backends || []).map((b) => {
      const r = b.records || {};
      const failed = (r.retryable || 0) + (r.dead || 0) + (r.quarantine || 0) + (r.failure || 0);
      const req = b.requests ? fmtSeconds(b.requests.average_seconds) + " / " + fmtSeconds(b.requests.max_seconds) + " (" + b.requests.count + ")" : "-";
      const breaker = b.circuit_breaker ? (b.circuit_breaker.open ? t("open") : t("closed")) : "-";
      const s = b.spool;
      const spool = s ? [s.states.active, s.states.dead, s.states.quarantine].map((u) => (u && u.scanned_at && !u.scanned_at.startsWith("0001-") ? u.files : "?")).join(" / ") : "-";
      const replay = s ? s.replay.state + (s.replay.last_error ? " (" + s.replay.last_error + ")" : "") : "-";
      return el("tr", { class: warningClassFor(warnings, name + "/" + b.name) },
        el("td", {}, b.name), el("td", { class: "num" }, (r.success || 0) + " / " + failed), el("td", {}, req),
        el("td", { class: "num" }, b.retries || 0), el("td", {}, breaker), el("td", {}, spool), el("td", {}, replay),
        el("td", {}, s ? fmtTime(s.replay.last_purge_at) : "-"));
    });
    body.append(el("table", {}, head, rows));
  }

  body.append(el("h2", {}, t("directories")));
  const dirHead = el("tr", {}, [t("path"), t("role"), t("member"), t("backend"), t("usage"), t("limit"), t("disk_free")].map((h) => el("th", {}, h)));
  const dirRows = (report.directories || []).map((d) => {
    const u = d.usage || {};
    const scanned = u.scanned_at && !u.scanned_at.startsWith("0001-");
    const usage = scanned ? u.files + " / " + fmtBytes(u.bytes) + (u.error ? " (" + u.error + ")" : "") : t("not_scanned");
    const disk = d.disk ? fmtBytes(d.disk.free_bytes) + " / " + fmtBytes(d.disk.total_bytes) + " (" + fmtPercent(d.disk.free_bytes / d.disk.total_bytes) + ")" : "-";
    return el("tr", { class: warningClassFor(warnings, d.path) },
      el("td", {}, d.path), el("td", {}, d.role), el("td", {}, d.member), el("td", {}, d.backend || "-"),
      el("td", {}, usage), el("td", {}, d.limit_bytes ? fmtBytes(d.limit_bytes) : "-"), el("td", {}, disk));
  });
  body.append(el("table", {}, dirHead, dirRows));
  document.getElementById("status-updated").textContent = t("updated") + ": " + new Date().toLocaleTimeString();
}

async function loadStatus() {
  try {
    renderStatus(await api("GET", "status"));
    showMessage("");
  } catch (e) {
    showMessage(e.message, "error");
  }
}

// ---- settings ----

let loadedSettings = null;
const samplingFields = ["enabled", "by_request_id", "request_id_rate", "debug_rate", "info_rate", "warn_rate", "error_rate"];

function renderSettings(document_) {
  loadedSettings = document_;
  const body = document.getElementById("settings-body");
  body.replaceChildren();
  const head = el("tr", {}, [t("logger"), t("output"), t("level"), t("sampling"), t("by_request_id"), t("request_id_rate"), t("debug_rate"), t("info_rate"), t("warn_rate"), t("error_rate")].map((h) => el("th", {}, h)));
  const rows = [];
  for (const [name, logger] of Object.entries(document_.loggers || {}).sort()) {
    for (const [output, settings] of Object.entries(logger.outputs || {}).sort()) {
      const s = settings.sampling || {};
      const row = el("tr", { "data-logger": name, "data-output": output });
      const level = el("select", { name: "level" }, ["DEBUG", "INFO", "WARN", "ERROR"].map((l) => el("option", l === settings.level ? { selected: "" } : {}, l)));
      const inputs = {
        enabled: el("input", { type: "checkbox", name: "enabled" }),
        by_request_id: el("input", { type: "checkbox", name: "by_request_id" }),
      };
      inputs.enabled.checked = Boolean(s.enabled);
      inputs.by_request_id.checked = Boolean(s.by_request_id);
      for (const field of ["request_id_rate", "debug_rate", "info_rate", "warn_rate", "error_rate"]) {
        inputs[field] = el("input", { type: "number", name: field, min: "1", value: String(s[field] ?? 1) });
      }
      row.append(el("td", {}, name), el("td", {}, output), el("td", {}, level));
      for (const field of samplingFields) row.append(el("td", {}, inputs[field]));
      row.addEventListener("input", () => row.classList.toggle("changed", changesOf(row) !== null));
      rows.push(row);
    }
  }
  body.append(el("table", {}, head, rows));
}

// changesOfは、行の入力のうち読み込んだ値と違うものだけをPATCHの形で返します。違いがなければnullです。
function changesOf(row) {
  const current = loadedSettings.loggers[row.dataset.logger].outputs[row.dataset.output];
  const change = {};
  const level = row.querySelector("[name=level]").value;
  if (level !== current.level) change.level = level;
  const sampling = {};
  for (const field of samplingFields) {
    const input = row.querySelector("[name=" + field + "]");
    const value = input.type === "checkbox" ? input.checked : Number(input.value);
    if (value !== (current.sampling || {})[field]) sampling[field] = value;
  }
  if (Object.keys(sampling).length > 0) change.sampling = sampling;
  return Object.keys(change).length > 0 ? change : null;
}

async function loadSettings() {
  try {
    renderSettings(await api("GET", "settings"));
    showMessage("");
  } catch (e) {
    showMessage(e.message, "error");
  }
}

async function saveSettings() {
  const patch = { loggers: {} };
  for (const row of document.querySelectorAll("#settings-body tr[data-logger]")) {
    const change = changesOf(row);
    if (!change) continue;
    const logger = (patch.loggers[row.dataset.logger] ||= { outputs: {} });
    logger.outputs[row.dataset.output] = change;
  }
  const result = document.getElementById("settings-result");
  if (Object.keys(patch.loggers).length === 0) { result.textContent = t("no_changes"); return; }
  try {
    const response = await api("PATCH", "settings", patch);
    result.textContent = t("saved") + (response.warnings ? " " + response.warnings.join(" ") : "");
    renderSettings(response);
    showMessage("");
  } catch (e) {
    result.textContent = "";
    showMessage(e.message, "error");
  }
}

// ---- config ----

let loadedConfig = null;

function renderConfig() {
  const path = document.getElementById("config-path").value.trim().replace(/^\/+|\/+$/g, "");
  let value = loadedConfig;
  if (path) {
    for (const segment of path.split("/")) {
      if (value !== null && typeof value === "object" && segment in value) value = value[segment];
      else { value = undefined; break; }
    }
  }
  document.getElementById("config-body").textContent = value === undefined ? "(not found)" : JSON.stringify(value, null, 2);
}

async function loadConfig() {
  try {
    loadedConfig = await api("GET", "config");
    renderConfig();
    showMessage("");
  } catch (e) {
    showMessage(e.message, "error");
  }
}

// ---- tabs, refresh, wiring ----

let currentTab = "status";
let autoRefreshTimer = null;

function showTab(name) {
  currentTab = name;
  for (const panel of document.querySelectorAll(".panel")) panel.hidden = panel.id !== name;
  for (const tab of document.querySelectorAll(".tab")) tab.setAttribute("aria-selected", String(tab.dataset.tab === name));
  try { localStorage.setItem("heterolog.tab", name); } catch (e) { /* 保存できなくても構いません。 */ }
  if (!token()) { showMessage(t("need_token")); return; }
  if (name === "status") loadStatus();
  else if (name === "settings") loadSettings();
  else loadConfig();
}

function updateAutoRefresh() {
  if (autoRefreshTimer) { clearInterval(autoRefreshTimer); autoRefreshTimer = null; }
  if (document.getElementById("status-auto").checked) {
    autoRefreshTimer = setInterval(() => { if (currentTab === "status" && token() && !document.hidden) loadStatus(); }, 5000);
  }
}

function connect() {
  const value = document.getElementById("token").value.trim();
  if (!value) { showMessage(t("need_token")); return; }
  try { sessionStorage.setItem("heterolog.token", value); } catch (e) { showMessage(t("failed"), "error"); return; }
  document.getElementById("token").value = "";
  showTab(currentTab);
}

function disconnect() {
  try { sessionStorage.removeItem("heterolog.token"); } catch (e) { /* 消せなければそのままです。 */ }
  for (const id of ["status-body", "settings-body"]) document.getElementById(id).replaceChildren();
  document.getElementById("config-body").textContent = "";
  showMessage(t("need_token"));
}

document.getElementById("connect").addEventListener("click", connect);
document.getElementById("token").addEventListener("keydown", (event) => { if (event.key === "Enter") connect(); });
document.getElementById("disconnect").addEventListener("click", disconnect);
for (const button of document.querySelectorAll("[data-lang]")) {
  button.addEventListener("click", () => {
    lang = button.dataset.lang;
    try { localStorage.setItem("heterolog.lang", lang); } catch (e) { /* 保存できなくても構いません。 */ }
    applyLanguage();
    showTab(currentTab);
  });
}
for (const tab of document.querySelectorAll(".tab")) tab.addEventListener("click", () => showTab(tab.dataset.tab));
document.getElementById("status-refresh").addEventListener("click", loadStatus);
document.getElementById("status-auto").addEventListener("change", updateAutoRefresh);
document.getElementById("settings-refresh").addEventListener("click", loadSettings);
document.getElementById("settings-save").addEventListener("click", saveSettings);
document.getElementById("config-refresh").addEventListener("click", loadConfig);
document.getElementById("config-path").addEventListener("input", renderConfig);

applyLanguage();
updateAutoRefresh();
let savedTab = "status";
try { savedTab = localStorage.getItem("heterolog.tab") || "status"; } catch (e) { /* 既定のtabです。 */ }
showTab(["status", "settings", "config"].includes(savedTab) ? savedTab : "status");
