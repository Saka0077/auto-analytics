const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { spawn } = require("child_process");
const { URL } = require("url");
const cheerio = require("cheerio");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const CATALOG_DIR = path.join(ROOT, "catalog");
const DATA_FILE = path.join(DATA_DIR, "listings.json");
const ARCHIVE_FILE = path.join(DATA_DIR, "listings-archive.json");
const SNAPSHOTS_FILE = path.join(DATA_DIR, "listing-snapshots.json");
const AUTH_FILE = path.join(DATA_DIR, "auth-store.json");
const AUTOPARTS_FILE = path.join(CATALOG_DIR, "autoparts-catalog.json");
const AUTOPARTS_ALIASES_FILE = path.join(CATALOG_DIR, "autoparts-aliases.json");
const SAMPLE_FILE = path.join(ROOT, "sample_listings.json");
const TRANSLATOR_PYTHON = path.join(ROOT, ".venv-translate", "Scripts", "python.exe");
const TRANSLATOR_SCRIPT = path.join(ROOT, "scripts", "offline_translate_worker.py");
const STALE_AFTER_HOURS = Number(process.env.LISTING_STALE_AFTER_HOURS) > 0
  ? Number(process.env.LISTING_STALE_AFTER_HOURS)
  : 72;
const STALE_AFTER_MS = STALE_AFTER_HOURS * 60 * 60 * 1000;
const ARCHIVE_AFTER_DAYS = Number(process.env.LISTING_ARCHIVE_AFTER_DAYS) > 0
  ? Number(process.env.LISTING_ARCHIVE_AFTER_DAYS)
  : 3;
const ARCHIVE_AFTER_MS = ARCHIVE_AFTER_DAYS * 24 * 60 * 60 * 1000;
const LOW_LOAD_SETTINGS = {
  pageDelayMinMs: 1800,
  pageDelayMaxMs: 3600,
  previewDelayMinMs: 200,
  previewDelayMaxMs: 450,
  detailDelayMinMs: 3200,
  detailDelayMaxMs: 5600,
  detailCacheTtlMs: 20 * 60 * 1000,
  pageCacheTtlMs: 10 * 60 * 1000,
  previewCacheTtlMs: 5 * 60 * 1000,
  previewProbeCap: 120,
  requestWindowMs: 60 * 1000,
  maxRequestsPerWindow: 18,
  protectionPauseMs: 20 * 60 * 1000,
  captchaPauseMs: 45 * 60 * 1000,
  guardPollIntervalMs: 1500,
  enrichConcurrency: 1,
  collectConcurrency: 1,
  collectMaxConcurrency: 2,
  queuePollIntervalMs: 1500
};
const pageCache = new Map();
const listingSnapshotCache = new Map();
const previewCache = new Map();
const importModelsCache = new Map();
const jobStore = new Map();
const jobQueue = [];
let activeJobId = "";
let jobWorkerRunning = false;
let translatorWorker = null;
let translatorWorkerBuffer = "";
let translatorRequestId = 0;
const translatorPending = new Map();
const remoteRequestTimestamps = [];
const remoteGuardState = {
  scopes: {
    search: {
      pausedUntil: 0,
      lastReason: "",
      lastEventAt: "",
      totalProtectionEvents: 0,
      recentEvents: []
    },
    detail: {
      pausedUntil: 0,
      lastReason: "",
      lastEventAt: "",
      totalProtectionEvents: 0,
      recentEvents: []
    }
  }
};

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon"
};

function sendJson(response, statusCode, payload) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization"
  });
  response.end(JSON.stringify(payload, null, 2));
}

function sendText(response, statusCode, text) {
  response.writeHead(statusCode, { "Content-Type": "text/plain; charset=utf-8" });
  response.end(text);
}

function runOfflineTranslator(command, payload) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(TRANSLATOR_PYTHON)) {
      reject(new Error("Python environment for translator is missing."));
      return;
    }

    if (!fs.existsSync(TRANSLATOR_SCRIPT)) {
      reject(new Error("Translator script is missing."));
      return;
    }

    ensureTranslatorWorker();
    const requestId = String(++translatorRequestId);
    translatorPending.set(requestId, { resolve, reject });
    translatorWorker.stdin.write(`${JSON.stringify({ id: requestId, command, ...(payload || {}) })}\n`);
  });
}

function ensureTranslatorWorker() {
  if (translatorWorker && !translatorWorker.killed) {
    return translatorWorker;
  }

  translatorWorkerBuffer = "";
  translatorWorker = spawn(TRANSLATOR_PYTHON, [TRANSLATOR_SCRIPT], {
    cwd: ROOT,
    env: {
      ...process.env,
      PYTHONIOENCODING: "utf-8",
      ARGOS_PACKAGES_DIR: path.join(ROOT, "offline-translator-data", "packages")
    },
    stdio: ["pipe", "pipe", "pipe"]
  });

  translatorWorker.stdout.on("data", chunk => {
    translatorWorkerBuffer += chunk.toString("utf-8");
    let newlineIndex = translatorWorkerBuffer.indexOf("\n");
    while (newlineIndex >= 0) {
      const line = translatorWorkerBuffer.slice(0, newlineIndex).trim();
      translatorWorkerBuffer = translatorWorkerBuffer.slice(newlineIndex + 1);
      if (line) {
        handleTranslatorWorkerLine(line);
      }
      newlineIndex = translatorWorkerBuffer.indexOf("\n");
    }
  });

  translatorWorker.stderr.on("data", chunk => {
    const message = chunk.toString("utf-8").trim();
    if (message) {
      console.error("[translator]", message);
    }
  });

  translatorWorker.on("close", () => {
    const error = new Error("Offline translator worker stopped.");
    for (const { reject } of translatorPending.values()) {
      reject(error);
    }
    translatorPending.clear();
    translatorWorker = null;
    translatorWorkerBuffer = "";
  });

  translatorWorker.on("error", error => {
    console.error("[translator]", error);
  });

  return translatorWorker;
}

function handleTranslatorWorkerLine(line) {
  let message = null;
  try {
    message = JSON.parse(line);
  } catch (error) {
    console.error("[translator] Invalid JSON:", line);
    return;
  }

  const entry = translatorPending.get(String(message.id || ""));
  if (!entry) {
    return;
  }

  translatorPending.delete(String(message.id || ""));
  if (!message.ok) {
    entry.reject(new Error(message.error || "Offline translator failed."));
    return;
  }

  delete message.id;
  delete message.ok;
  entry.resolve(message);
}

function ensureDataDir() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }
}

function parseRequestBody(request) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    request.on("data", chunk => chunks.push(chunk));
    request.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf-8");
        resolve(raw ? JSON.parse(raw) : {});
      } catch (error) {
        reject(error);
      }
    });
    request.on("error", reject);
  });
}

function cloneJson(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function nowMs() {
  return Date.now();
}

function waitRandom(minMs, maxMs) {
  const min = Math.max(0, Number(minMs) || 0);
  const max = Math.max(min, Number(maxMs) || min);
  const duration = Math.round(min + Math.random() * (max - min));
  return new Promise(resolve => setTimeout(resolve, duration));
}

function cleanupExpiredCache(map) {
  const currentTime = nowMs();
  for (const [key, entry] of map.entries()) {
    if (!entry || entry.expiresAt <= currentTime) {
      map.delete(key);
    }
  }
}

function readCache(map, key) {
  cleanupExpiredCache(map);
  const entry = map.get(key);
  if (!entry || entry.expiresAt <= nowMs()) {
    map.delete(key);
    return null;
  }
  return cloneJson(entry.value);
}

function writeCache(map, key, value, ttlMs) {
  map.set(key, {
    value: cloneJson(value),
    expiresAt: nowMs() + Math.max(1000, Number(ttlMs) || 1000)
  });
}

function pruneRemoteRequestTimestamps() {
  const threshold = nowMs() - LOW_LOAD_SETTINGS.requestWindowMs;
  while (remoteRequestTimestamps.length && remoteRequestTimestamps[0] <= threshold) {
    remoteRequestTimestamps.shift();
  }
}

function pushRemoteGuardEvent(type, detail) {
  pushRemoteGuardScopedEvent("search", type, detail);
}

function pushRemoteGuardScopedEvent(scope, type, detail) {
  const targetScope = remoteGuardState.scopes[scope] ? scope : "search";
  const bucket = remoteGuardState.scopes[targetScope];
  const entry = {
    type: String(type || "info"),
    detail: String(detail || "").trim(),
    at: new Date().toISOString()
  };
  bucket.lastEventAt = entry.at;
  bucket.recentEvents.unshift(entry);
  bucket.recentEvents = bucket.recentEvents.slice(0, 8);
}

function getRemoteGuardStatus(scope = "all") {
  pruneRemoteRequestTimestamps();
  if (scope !== "all") {
    const bucket = remoteGuardState.scopes[scope] || remoteGuardState.scopes.search;
    const waitMs = Math.max(0, bucket.pausedUntil - nowMs());
    return {
      scope,
      paused: waitMs > 0,
      pausedUntil: waitMs > 0 ? new Date(bucket.pausedUntil).toISOString() : "",
      waitMs,
      lastReason: bucket.lastReason,
      requestsInWindow: remoteRequestTimestamps.length,
      maxRequestsPerWindow: LOW_LOAD_SETTINGS.maxRequestsPerWindow,
      requestWindowMs: LOW_LOAD_SETTINGS.requestWindowMs,
      totalProtectionEvents: bucket.totalProtectionEvents,
      recentEvents: cloneJson(bucket.recentEvents)
    };
  }

  const search = getRemoteGuardStatus("search");
  const detail = getRemoteGuardStatus("detail");
  const waitMs = Math.max(search.waitMs, detail.waitMs);
  return {
    paused: waitMs > 0,
    pausedUntil: waitMs > 0 ? new Date(nowMs() + waitMs).toISOString() : "",
    waitMs,
    lastReason: detail.waitMs >= search.waitMs ? detail.lastReason : search.lastReason,
    requestsInWindow: remoteRequestTimestamps.length,
    maxRequestsPerWindow: LOW_LOAD_SETTINGS.maxRequestsPerWindow,
    requestWindowMs: LOW_LOAD_SETTINGS.requestWindowMs,
    totalProtectionEvents: search.totalProtectionEvents + detail.totalProtectionEvents,
    recentEvents: [...detail.recentEvents, ...search.recentEvents]
      .sort((left, right) => String(right.at || "").localeCompare(String(left.at || "")))
      .slice(0, 8),
    scopes: {
      search,
      detail
    }
  };
}

function noteRemoteProtection(reason, { status, url, scope = "search" } = {}) {
  const targetScope = remoteGuardState.scopes[scope] ? scope : "search";
  const bucket = remoteGuardState.scopes[targetScope];
  const normalizedReason = String(reason || "remote-protection").trim();
  const pauseMs = /captcha|robot|challenge/i.test(normalizedReason)
    ? LOW_LOAD_SETTINGS.captchaPauseMs
    : LOW_LOAD_SETTINGS.protectionPauseMs;
  bucket.pausedUntil = Math.max(bucket.pausedUntil, nowMs() + pauseMs);
  bucket.lastReason = normalizedReason;
  bucket.totalProtectionEvents += 1;
  pushRemoteGuardScopedEvent(
    targetScope,
    "protection",
    `${normalizedReason}${status ? ` [${status}]` : ""}${url ? ` ${url}` : ""}`
  );
}

function looksLikeProtectionPage(text) {
  const normalized = String(text || "").toLowerCase();
  return [
    "captcha",
    "cloudflare",
    "access denied",
    "too many requests",
    "too many attempts",
    "подтвердите, что вы не робот",
    "подтвердите что вы не робот",
    "слишком много запросов",
    "временно ограничен",
    "security check",
    "ddos protection"
  ].some(fragment => normalized.includes(fragment));
}

function describeFetchBlock(reason, status) {
  if (/captcha|robot|challenge/i.test(reason)) {
    return "Kolesa показала защиту или капчу. Импорт поставлен на паузу.";
  }
  if (Number(status) === 429) {
    return "Kolesa ответила Too Many Requests. Импорт поставлен на паузу.";
  }
  if (Number(status) === 403) {
    return "Kolesa временно режет доступ. Импорт поставлен на паузу.";
  }
  return "Kolesa ограничила ответы. Импорт поставлен на паузу.";
}

function createKolesaPausedError(waitMs, scope = "search") {
  const roundedWaitSec = Math.max(1, Math.ceil((Number(waitMs) || 0) / 1000));
  const error = new Error(`Kolesa временно поставила ${scope === "detail" ? "проверку карточек" : "поиск"} на паузу. Подожди ${roundedWaitSec} сек и попробуй снова.`);
  error.code = "kolesa-paused";
  error.waitMs = Math.max(0, Number(waitMs) || 0);
  error.scope = scope;
  return error;
}

async function waitForKolesaRateLimit({ jobId = "", context = "запрос", failFastOnPause = true, guardScope = "search" } = {}) {
  while (true) {
    const guard = getRemoteGuardStatus(guardScope);
    if (guard.paused) {
      if (failFastOnPause || jobId) {
        throw createKolesaPausedError(guard.waitMs, guardScope);
      }
      if (jobId) {
        updateJob(jobId, {
          message: `Пауза защиты Kolesa: ждём ${Math.ceil(guard.waitMs / 1000)} сек`
        });
      }
      await wait(Math.min(guard.waitMs, LOW_LOAD_SETTINGS.guardPollIntervalMs));
      continue;
    }

    if (guard.requestsInWindow < LOW_LOAD_SETTINGS.maxRequestsPerWindow) {
      remoteRequestTimestamps.push(nowMs());
      return;
    }

    const nextFreeAt = remoteRequestTimestamps[0] + LOW_LOAD_SETTINGS.requestWindowMs;
    const waitMs = Math.max(500, nextFreeAt - nowMs() + Math.round(120 + Math.random() * 280));
    if (jobId) {
      updateJob(jobId, {
        message: `Мягкая пауза ${Math.ceil(waitMs / 1000)} сек: ${context}`
      });
    }
    await wait(waitMs);
  }
}

async function fetchKolesaHtml(url, { headers = {}, jobId = "", context = "запрос", failFastOnPause = true, guardScope = "search" } = {}) {
  await waitForKolesaRateLimit({ jobId, context, failFastOnPause, guardScope });

  const response = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 AutoAnalytics/1.0",
      "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
      ...headers
    }
  });

  if (response.status === 404) {
    return {
      ok: false,
      status: 404,
      text: "",
      response
    };
  }

  const text = await response.text();
  if ([403, 429, 503].includes(response.status) || looksLikeProtectionPage(text)) {
    const reason = looksLikeProtectionPage(text)
      ? "captcha-or-challenge"
      : `http-${response.status}`;
    noteRemoteProtection(reason, { status: response.status, url, scope: guardScope });
    throw new Error(describeFetchBlock(reason, response.status));
  }

  if (!response.ok) {
    throw new Error(`fetch-failed-${response.status}`);
  }

  return {
    ok: true,
    status: response.status,
    text,
    response
  };
}

function normalizeKolesaCacheKey(value) {
  try {
    const parsed = new URL(String(value || "").trim());
    return `${parsed.origin}${parsed.pathname}`;
  } catch (error) {
    return String(value || "").trim();
  }
}

function createJob(type, payload = {}) {
  const id = crypto.randomBytes(8).toString("hex");
  const job = {
    id,
    type,
    status: "queued",
    created_at: new Date().toISOString(),
    started_at: "",
    finished_at: "",
    progress: 0,
    total: 0,
    message: "В очереди",
    error: "",
    result: null,
    payload: cloneJson(payload)
  };
  jobStore.set(id, job);
  jobQueue.push(id);
  void ensureJobWorker();
  return cloneJson(job);
}

function getJobSnapshot(jobId) {
  const job = jobStore.get(jobId);
  return job ? cloneJson(job) : null;
}

function updateJob(jobId, patch) {
  const job = jobStore.get(jobId);
  if (!job) {
    return null;
  }
  Object.assign(job, patch);
  return job;
}

async function ensureJobWorker() {
  if (jobWorkerRunning) {
    return;
  }

  jobWorkerRunning = true;
  while (jobQueue.length) {
    const jobId = jobQueue.shift();
    const job = jobStore.get(jobId);
    if (!job) {
      continue;
    }

    activeJobId = jobId;
    job.status = "running";
    job.started_at = new Date().toISOString();
    job.message = "Запущено";

    try {
      if (job.type === "kolesa-import") {
        await runKolesaImportJob(job);
      } else if (job.type === "collect-snapshots") {
        await runCollectSnapshotsJob(job);
      } else {
        throw new Error("unsupported-job-type");
      }

      job.status = "completed";
      job.finished_at = new Date().toISOString();
      job.progress = job.total || job.progress;
      job.message = "Готово";
    } catch (error) {
      const detail = String(error.message || error);
      const pausedByRemote = error && error.code === "kolesa-paused";
      job.status = pausedByRemote ? "paused" : "failed";
      job.finished_at = new Date().toISOString();
      job.error = detail;
      job.message = detail;
      pushRemoteGuardEvent(pausedByRemote ? "job-paused" : "job-failed", `${job.type}: ${detail}`);
    } finally {
      activeJobId = "";
    }
  }

  jobWorkerRunning = false;
}

function readListings() {
  const filePath = fs.existsSync(DATA_FILE) ? DATA_FILE : SAMPLE_FILE;
  const raw = fs.readFileSync(filePath, "utf-8");
  const parsed = JSON.parse(raw);
  const listings = normalizeListings(Array.isArray(parsed) ? parsed : parsed.items || []);
  if (filePath !== DATA_FILE) {
    return listings;
  }
  return syncListingArchiveStorage(listings).activeItems;
}

function writeListings(listings) {
  ensureDataDir();
  fs.writeFileSync(DATA_FILE, JSON.stringify(normalizeListings(listings), null, 2), "utf-8");
}

function readArchivedListings() {
  if (!fs.existsSync(ARCHIVE_FILE)) {
    return [];
  }

  try {
    const raw = fs.readFileSync(ARCHIVE_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    return normalizeListings(Array.isArray(parsed) ? parsed : parsed.items || []);
  } catch (error) {
    return [];
  }
}

function writeArchivedListings(listings) {
  ensureDataDir();
  fs.writeFileSync(ARCHIVE_FILE, JSON.stringify(normalizeListings(listings), null, 2), "utf-8");
}

function readListingSnapshots() {
  if (!fs.existsSync(SNAPSHOTS_FILE)) {
    return [];
  }

  try {
    const raw = fs.readFileSync(SNAPSHOTS_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    const rows = Array.isArray(parsed) ? parsed : parsed.items || [];
    return normalizeListingSnapshots(rows);
  } catch (error) {
    return [];
  }
}

function writeListingSnapshots(rows) {
  ensureDataDir();
  fs.writeFileSync(SNAPSHOTS_FILE, JSON.stringify(normalizeListingSnapshots(rows), null, 2), "utf-8");
}

function createEmptyAutopartsCatalog() {
  return {
    generated_at: "",
    source_workbook: "",
    snapshot_title: "",
    snapshot_note: "",
    snapshot_items: [],
    snapshot_conclusions: [],
    market_context: [],
    observations: []
  };
}

function createEmptyAutopartsAliases() {
  return { items: [] };
}

function readAutopartsCatalog() {
  if (!fs.existsSync(AUTOPARTS_FILE)) {
    return createEmptyAutopartsCatalog();
  }

  try {
    const raw = fs.readFileSync(AUTOPARTS_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    const snapshotItems = Array.isArray(parsed?.snapshot_items) ? parsed.snapshot_items : [];
    const marketContext = Array.isArray(parsed?.market_context) ? parsed.market_context : [];
    const observations = Array.isArray(parsed?.observations) ? parsed.observations : [];

    return {
      generated_at: String(parsed?.generated_at || ""),
      source_workbook: String(parsed?.source_workbook || ""),
      snapshot_title: String(parsed?.snapshot_title || ""),
      snapshot_note: String(parsed?.snapshot_note || ""),
      snapshot_items: snapshotItems,
      snapshot_conclusions: Array.isArray(parsed?.snapshot_conclusions)
        ? parsed.snapshot_conclusions.map(item => String(item || "").trim()).filter(Boolean)
        : [],
      market_context: marketContext,
      observations
    };
  } catch (error) {
    return createEmptyAutopartsCatalog();
  }
}

function readAutopartsAliases() {
  if (!fs.existsSync(AUTOPARTS_ALIASES_FILE)) {
    return createEmptyAutopartsAliases();
  }

  try {
    const raw = fs.readFileSync(AUTOPARTS_ALIASES_FILE, "utf-8");
    const parsed = JSON.parse(raw);
    const items = Array.isArray(parsed?.items) ? parsed.items : [];
    return {
      items: items
        .filter(item => item && typeof item === "object")
        .map(item => ({
          catalog_id: String(item.catalog_id || "").trim(),
          brand: String(item.brand || "").trim(),
          aliases: Array.isArray(item.aliases)
            ? item.aliases.map(value => String(value || "").trim()).filter(Boolean)
            : []
        }))
        .filter(item => item.catalog_id && item.aliases.length)
    };
  } catch (error) {
    return createEmptyAutopartsAliases();
  }
}

function normalizeActualityStatus(value) {
  const status = String(value || "").trim().toLowerCase();
  return ["active", "stale", "archived", "unavailable"].includes(status) ? status : "active";
}

function normalizeIsoDate(value) {
  if (!value) {
    return "";
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}

function normalizeBoolean(value) {
  return value === true || value === "true" || value === 1 || value === "1";
}

function normalizeTextList(value) {
  return Array.isArray(value)
    ? value.map(item => String(item || "").trim()).filter(Boolean).slice(0, 12)
    : [];
}

function normalizeLookupText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ");
}

function splitLookupWords(value) {
  return normalizeLookupText(value).split(" ").filter(Boolean);
}

function parseYearRange(value) {
  const normalized = String(value || "").trim();
  const match = normalized.match(/(\d{4})\s*-\s*(\d{4})/);
  if (!match) {
    return null;
  }

  const min = Number(match[1]);
  const max = Number(match[2]);
  return Number.isFinite(min) && Number.isFinite(max) ? { min, max } : null;
}

function isTrimToken(word) {
  return /^(comfort|comfort\+|prestige|style|premium|sport|sportline|elegance|lux|luxe|limited|exclusive|active|drive|trend|status|deluxe|base|luxury|flagship|premium\+|elite|plus|pro|max|awd|4wd|2wd|at|mt|cvt|dsg|tsi|tdi|mpi|gdi|hybrid|ev|электро|автомат|механика|робот|вариатор)$/i.test(word)
    || /^\d+(\.\d+)?[ldt]?$/.test(word)
    || /^\d{2,3}h?$/.test(word);
}

function buildAutopartsModelCandidates(item) {
  const values = [item.model, item.title]
    .map(value => normalizeLookupText(value))
    .filter(Boolean);
  const candidates = new Set();

  values.forEach(value => {
    const tokens = splitLookupWords(value)
      .filter(token => !/^(\d{4}|г|года)$/.test(token))
      .filter(token => !/^(toyota|hyundai|kia|chevrolet|daewoo|ravon|lexus|nissan|tesla|byd|tank|dongfeng|mercedes|benz|volkswagen|lada|ваз)$/i.test(token));

    if (!tokens.length) {
      return;
    }

    candidates.add(tokens.join(" "));

    while (tokens.length > 1 && isTrimToken(tokens[tokens.length - 1])) {
      tokens.pop();
      if (tokens.length) {
        candidates.add(tokens.join(" "));
      }
    }

    for (let length = tokens.length - 1; length >= 1; length -= 1) {
      candidates.add(tokens.slice(0, length).join(" "));
    }
  });

  return [...candidates].filter(Boolean);
}

function normalizeRemoteUrl(value, base = "https://kolesa.kz") {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  if (raw.startsWith("//")) {
    return `https:${raw}`;
  }

  try {
    return new URL(raw, base).toString();
  } catch (error) {
    return "";
  }
}

function getAutopartsMatchText(item) {
  return normalizeLookupText([item.brand, item.model, item.title].filter(Boolean).join(" "));
}

function getAutopartsMatchConfidenceLabel(confidence) {
  switch (String(confidence || "").trim()) {
    case "exact":
      return "Точный матч";
    case "generation_match":
      return "Матч по поколению";
    case "model_only":
      return "Матч по модели";
    case "weak":
      return "Слабый матч";
    default:
      return "Нет матча";
  }
}

function buildAutopartsLookupMap(catalog = readAutopartsCatalog(), aliasesStore = readAutopartsAliases()) {
  const snapshotItems = Array.isArray(catalog?.snapshot_items) ? catalog.snapshot_items : [];
  const observations = Array.isArray(catalog?.observations) ? catalog.observations : [];
  const aliases = Array.isArray(aliasesStore?.items) ? aliasesStore.items : [];
  const aliasMap = new Map();
  const profiles = [...snapshotItems, ...observations]
    .filter(item => item && typeof item === "object" && String(item.id || "").trim());

  aliases.forEach(item => {
    const key = String(item.catalog_id || "").trim();
    if (!key) {
      return;
    }
    aliasMap.set(key, item);
  });

  return profiles.map(profile => {
    const aliasEntry = aliasMap.get(String(profile.id || "").trim());
    const brandKey = normalizeLookupText(profile.brand_key || profile.brand);
    const directKeys = Array.isArray(profile.lookup_keys)
      ? profile.lookup_keys.map(normalizeLookupText).filter(Boolean)
      : [];
    const aliasKeys = Array.isArray(aliasEntry?.aliases)
      ? aliasEntry.aliases.map(normalizeLookupText).filter(Boolean)
      : [];
    const primaryModelKey = normalizeLookupText(profile.primary_model);
    const yearRange = parseYearRange(profile.years);
    return {
      profile,
      brandKey,
      primaryModelKey,
      yearRange,
      lookupKeys: [...new Set([...directKeys, ...aliasKeys])]
    };
  });
}

function findAutopartsProfile(item, catalog = readAutopartsCatalog(), aliasesStore = readAutopartsAliases()) {
  const lookup = buildAutopartsLookupMap(catalog, aliasesStore);
  if (!lookup.length) {
    return null;
  }

  const listingText = getAutopartsMatchText(item);
  const listingBrand = normalizeLookupText(item.brand);
  const listingCandidates = buildAutopartsModelCandidates(item);
  let bestMatch = null;

  lookup.forEach(entry => {
    if (!entry.lookupKeys.length) {
      return;
    }

    let score = 0;
    const reasons = [];
    let bestModelSignal = null;

    if (entry.brandKey && listingBrand) {
      if (entry.brandKey !== listingBrand) {
        return;
      }
      score += 35;
      reasons.push("brand");
    }

    entry.lookupKeys.forEach(key => {
      const normalizedKey = normalizeLookupText(key);
      if (!normalizedKey) {
        return;
      }

      const keyTokens = splitLookupWords(normalizedKey);
      const primaryTokens = splitLookupWords(entry.primaryModelKey);
      const hasGenerationHint = primaryTokens.length > 0
        && keyTokens.length > primaryTokens.length
        && primaryTokens.every((token, index) => keyTokens[index] === token);

      let candidateScore = 0;
      let matchType = "";

      if (listingCandidates.some(candidate => candidate === normalizedKey)) {
        candidateScore = hasGenerationHint ? 72 : 64;
        matchType = "exact";
      } else if (listingCandidates.some(candidate => candidate.startsWith(`${normalizedKey} `) || normalizedKey.startsWith(`${candidate} `))) {
        candidateScore = hasGenerationHint ? 58 : 50;
        matchType = "prefix";
      } else if (listingText.includes(normalizedKey)) {
        candidateScore = hasGenerationHint ? 46 : 38;
        matchType = "text";
      }

      if (!candidateScore) {
        return;
      }

      if (
        !bestModelSignal ||
        candidateScore > bestModelSignal.score ||
        (candidateScore === bestModelSignal.score && normalizedKey.length > bestModelSignal.key.length)
      ) {
        bestModelSignal = {
          key: normalizedKey,
          score: candidateScore,
          matchType,
          hasGenerationHint,
          reason: `${matchType}:${normalizedKey}`
        };
      }
    });

    if (!bestModelSignal) {
      return;
    }

    score += bestModelSignal.score;
    reasons.push(bestModelSignal.reason);

    let yearMatched = null;
    if (entry.yearRange && Number.isFinite(Number(item.year))) {
      const year = Number(item.year);
      if (year >= entry.yearRange.min && year <= entry.yearRange.max) {
        score += 12;
        reasons.push("year");
        yearMatched = true;
      } else {
        return;
      }
    }

    const matchConfidence = bestModelSignal.hasGenerationHint
      ? yearMatched === true
        ? "generation_match"
        : "model_only"
      : bestModelSignal.matchType === "exact"
        ? "exact"
        : "model_only";

    if (
      bestModelSignal.matchType === "text" &&
      !bestModelSignal.hasGenerationHint &&
      score < 70
    ) {
      return;
    }

    if (
      !bestMatch ||
      score > bestMatch.score ||
      (score === bestMatch.score && Number(entry.profile.cheapness_score || 0) > Number(bestMatch.profile.cheapness_score || 0))
    ) {
      bestMatch = {
        profile: entry.profile,
        score,
        reasons,
        confidence: matchConfidence
      };
    }
  });

  if (!bestMatch || bestMatch.score < 55) {
    return null;
  }

  const match = bestMatch.profile;
  return {
    id: String(match.id || ""),
    model_label: String(match.model_label || ""),
    brand: String(match.brand || ""),
    primary_model: String(match.primary_model || ""),
    model_aliases: Array.isArray(match.model_aliases) ? match.model_aliases.map(value => String(value || "").trim()).filter(Boolean) : [],
    segment: String(match.segment || ""),
    years: String(match.years || ""),
    front_pads_price_kzt: Number(match.front_pads_price_kzt) || null,
    front_pads_stock: Number(match.front_pads_stock) || null,
    front_disc_price_kzt: Number(match.front_disc_price_kzt) || null,
    front_disc_stock: Number(match.front_disc_stock) || null,
    service_basket_kzt: Number(match.service_basket_kzt) || null,
    avg_stock: Number(match.avg_stock) || null,
    price_score: Number(match.price_score) || null,
    cheapness_score: Number(match.cheapness_score) || null,
    priority: Number(match.priority) || null,
    maintenance_band: String(match.maintenance_band || "unknown"),
    maintenance_label: String(match.maintenance_label || "Неизвестно"),
    comment: String(match.comment || ""),
    market_source_url: String(match.market_source_url || ""),
    pads_source_url: String(match.pads_source_url || ""),
    disc_source_url: String(match.disc_source_url || ""),
    match_score: Number(bestMatch.score || 0),
    match_reason: bestMatch.reasons.join(", "),
    match_confidence: String(bestMatch.confidence || "none"),
    match_confidence_label: getAutopartsMatchConfidenceLabel(bestMatch.confidence || "none"),
    model_token_match: true
  };
}

function enrichListingsWithAutoparts(listings, catalog = readAutopartsCatalog(), aliasesStore = readAutopartsAliases()) {
  return listings.map(item => {
    const profile = findAutopartsProfile(item, catalog, aliasesStore);
    return profile ? { ...item, autoparts_profile: profile } : item;
  });
}

function getAutopartsCoverage(listings, catalog = readAutopartsCatalog(), aliasesStore = readAutopartsAliases()) {
  const enriched = enrichListingsWithAutoparts(listings, catalog, aliasesStore);
  const matched = [];
  const unmatchedBuckets = new Map();
  const matchedBuckets = new Map();

  enriched.forEach(item => {
    const brand = String(item.brand || "").trim() || "Без марки";
    const model = String(item.model || "").trim() || "Без модели";
    const key = `${brand} | ${model}`;

    if (item.autoparts_profile) {
      matched.push(item);
      const profileKey = item.autoparts_profile.model_label || key;
      matchedBuckets.set(profileKey, (matchedBuckets.get(profileKey) || 0) + 1);
      return;
    }

    const bucket = unmatchedBuckets.get(key) || {
      brand,
      model,
      count: 0,
      examples: []
    };
    bucket.count += 1;
    if (bucket.examples.length < 3) {
      bucket.examples.push(String(item.title || "").trim());
    }
    unmatchedBuckets.set(key, bucket);
  });

  return {
    total: enriched.length,
    matched: matched.length,
    unmatched: enriched.length - matched.length,
    coverage_percent: enriched.length ? Number(((matched.length / enriched.length) * 100).toFixed(1)) : 0,
    matched_profiles: [...matchedBuckets.entries()]
      .map(([model_label, count]) => ({ model_label, count }))
      .sort((a, b) => b.count - a.count || a.model_label.localeCompare(b.model_label, "ru"))
      .slice(0, 20),
    missing_models: [...unmatchedBuckets.values()]
      .sort((a, b) => b.count - a.count || `${a.brand} ${a.model}`.localeCompare(`${b.brand} ${b.model}`, "ru"))
      .slice(0, 20)
  };
}

function getPhotoIdentityKey(urlValue) {
  const normalized = normalizeRemoteUrl(urlValue);
  if (!normalized) {
    return "";
  }

  try {
    const parsed = new URL(normalized);
    const pathname = parsed.pathname.replace(/\/+/g, "/");
    const identityPath = pathname.replace(/\/([^/]+?)(?:-(?:full|\d+x\d+))(\.[a-z0-9]+)$/i, "/$1");
    return `${parsed.hostname}${identityPath}`;
  } catch (error) {
    return normalized;
  }
}

function getPhotoQualityScore(urlValue) {
  const normalized = String(urlValue || "").trim().toLowerCase();
  if (/-full\./.test(normalized)) {
    return Number.MAX_SAFE_INTEGER;
  }
  if (/-\d+x\d+\./.test(normalized)) {
    const match = normalized.match(/-(\d+)x(\d+)\./);
    if (match) {
      return Number(match[1]) * Number(match[2]);
    }
  }
  return 2;
}

function normalizePhotoGallery(value) {
  const items = Array.isArray(value) ? value : [value];
  const unique = [];
  const indexByIdentity = new Map();

  items.forEach(item => {
    const normalized = normalizeRemoteUrl(item);
    if (!normalized) {
      return;
    }

    const identity = getPhotoIdentityKey(normalized) || normalized;
    const existingIndex = indexByIdentity.get(identity);
    if (existingIndex === undefined) {
      indexByIdentity.set(identity, unique.length);
      unique.push(normalized);
      return;
    }

    if (getPhotoQualityScore(normalized) > getPhotoQualityScore(unique[existingIndex])) {
      unique[existingIndex] = normalized;
    }
  });

  return unique.slice(0, 30);
}

function pickDefined(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return null;
}

function normalizeRepairState(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "1" || normalized === "yes" || normalized === "true") {
    return "yes";
  }
  if (normalized === "2" || normalized === "no" || normalized === "false") {
    return "no";
  }
  return "unknown";
}

function normalizeTextValue(value) {
  return String(value || "").trim().toLowerCase();
}

function textField(item, snakeCaseKey, camelCaseKey = "") {
  return String(
    item?.[snakeCaseKey]
    ?? (camelCaseKey ? item?.[camelCaseKey] : undefined)
    ?? ""
  ).trim();
}

function numberField(item, snakeCaseKey, camelCaseKey = "") {
  const value = item?.[snakeCaseKey] ?? (camelCaseKey ? item?.[camelCaseKey] : undefined);
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function booleanField(item, snakeCaseKey, camelCaseKey = "") {
  return normalizeBoolean(item?.[snakeCaseKey] ?? (camelCaseKey ? item?.[camelCaseKey] : undefined));
}

function listField(item, snakeCaseKey, camelCaseKey = "") {
  return normalizeTextList(item?.[snakeCaseKey] ?? (camelCaseKey ? item?.[camelCaseKey] : undefined));
}

function detectFuelType(text) {
  const normalized = normalizeTextValue(text);
  if (/электрич|электромоб|электрокар/i.test(normalized)) {
    return "Электро";
  }
  if (/гибрид/i.test(normalized)) {
    return "Гибрид";
  }
  if (/дизел/i.test(normalized)) {
    return "Дизель";
  }
  if (/(^|[\s,.;:()/-])газ(?=$|[\s,.;:()/-])|гбо/i.test(normalized)) {
    return "Газ";
  }
  if (/бензин/i.test(normalized)) {
    return "Бензин";
  }
  return "";
}

function detectTransmission(text) {
  const normalized = normalizeTextValue(text);
  if (/кпп\s+автомат|автоматическая кпп|\bакпп\b/i.test(normalized)) {
    return "Автомат";
  }
  if (/кпп\s+механика|механическая кпп|\bмкпп\b/i.test(normalized)) {
    return "Механика";
  }
  if (/кпп\s+вариатор|вариатор/i.test(normalized)) {
    return "Вариатор";
  }
  if (/кпп\s+робот|робот/i.test(normalized)) {
    return "Робот";
  }
  if (/кпп\s+типтроник|типтроник/i.test(normalized)) {
    return "Типтроник";
  }
  return "";
}

function detectDriveType(text) {
  const normalized = normalizeTextValue(text);
  if (/4wd|awd|полный привод|привод[:\s-]*полный/i.test(normalized)) {
    return "Полный";
  }
  if (/передний привод|привод[:\s-]*передний/i.test(normalized)) {
    return "Передний";
  }
  if (/задний привод|привод[:\s-]*задний/i.test(normalized)) {
    return "Задний";
  }
  return "";
}

function detectSteeringSide(text) {
  const normalized = normalizeTextValue(text);
  if (/левый руль|слева/i.test(normalized)) {
    return "Левый";
  }
  if (/правый руль|справа/i.test(normalized)) {
    return "Правый";
  }
  return "";
}

function detectColor(text) {
  const normalized = normalizeTextValue(text);
  const colors = [
    ["Серебристый", /серебрист|серебр/i],
    ["Черный", /черн|чёрн/i],
    ["Белый", /бел/i],
    ["Серый", /(^|[\s,])сер(ый|ая|ого|ом)?([\s,.]|$)/i],
    ["Синий", /син/i],
    ["Голубой", /голуб/i],
    ["Красный", /красн/i],
    ["Бордовый", /бордов|вишн/i],
    ["Зеленый", /зелен|зелён/i],
    ["Коричневый", /корич/i],
    ["Бежевый", /беж/i],
    ["Желтый", /желт|жёлт/i],
    ["Оранжевый", /оранж/i],
    ["Фиолетовый", /фиолет|сиренев/i],
    ["Золотистый", /золот/i]
  ];

  for (const [label, pattern] of colors) {
    if (pattern.test(normalized)) {
      return label;
    }
  }

  return "";
}

function detectOptions(text) {
  const normalized = normalizeTextValue(text);
  const optionMap = [
    ["Кожа", /кожа|кожан/i],
    ["Люк", /люк/i],
    ["Камера", /камера/i],
    ["Парктроник", /парктрон/i],
    ["Подогрев", /подогрев/i],
    ["Bluetooth", /bluetooth|блютуз/i],
    ["Климат", /климат/i],
    ["Круиз", /круиз/i],
    ["ABS", /\babs\b/i],
    ["ESP", /\besp\b/i],
    ["Мультируль", /мультирул/i],
    ["Сигнализация", /сигнализац/i]
  ];

  return optionMap
    .filter(([, pattern]) => pattern.test(normalized))
    .map(([label]) => label)
    .slice(0, 12);
}

function extractTitleBrandModel(title, brand = "", model = "") {
  const normalizedTitle = normalizeWhitespace(title).replace(/\s+\d{4}\s*г\.?$/i, "");
  const normalizedBrand = normalizeWhitespace(brand);
  const normalizedModel = normalizeWhitespace(model);
  if (normalizedBrand && normalizedModel) {
    return { brand: normalizedBrand, model: normalizedModel };
  }

  if (normalizedBrand && !normalizedModel) {
    const nextModel = normalizeWhitespace(normalizedTitle.replace(new RegExp(`^${normalizedBrand}\\s*`, "i"), ""));
    return { brand: normalizedBrand, model: nextModel };
  }

  const multiWordBrands = [
    "Mercedes-Benz",
    "Land Rover",
    "Alfa Romeo",
    "Aston Martin",
    "Great Wall",
    "Hongqi",
    "Rolls-Royce",
    "ВАЗ (Lada)"
  ];
  const matchedBrand = multiWordBrands.find(item => normalizedTitle.toLowerCase().startsWith(item.toLowerCase()));
  if (matchedBrand) {
    return {
      brand: matchedBrand,
      model: normalizeWhitespace(normalizedTitle.slice(matchedBrand.length))
    };
  }

  const [firstWord, ...rest] = normalizedTitle.split(" ");
  return {
    brand: firstWord || "",
    model: rest.join(" ").trim()
  };
}

function normalizeVin(value) {
  const vin = String(value || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
  return vin;
}

function isValidVin(value) {
  const vin = normalizeVin(value);
  return vin.length === 17 && !/[IOQ]/.test(vin);
}

function resolveActualityStatus(item) {
  const status = normalizeActualityStatus(item.actuality_status);
  if (status === "archived" || status === "unavailable") {
    return status;
  }

  if (String(item.source || "").trim() !== "kolesa.kz") {
    return status;
  }

  const checkedAt = normalizeIsoDate(item.last_checked_at || item.last_seen_at);
  if (!checkedAt) {
    return status;
  }

  return Date.now() - new Date(checkedAt).getTime() > STALE_AFTER_MS ? "stale" : "active";
}

function getListingStorageKey(item) {
  return String(item.advert_id || extractAdvertIdFromUrl(item.url) || item.url || `${item.title}|${item.price}|${item.city}`);
}

function createListingSnapshotId(listingId, capturedAt) {
  return `snapshot:${listingId}:${capturedAt}`;
}

function buildListingSnapshotHash(item) {
  const payload = {
    price: numberField(item, "price"),
    publication_date: textField(item, "publication_date", "publicationDate"),
    last_update: textField(item, "last_update", "lastUpdate"),
    actuality_status: normalizeActualityStatus(item.actuality_status ?? item.actualityStatus),
    photo_count: numberField(item, "photo_count", "photoCount"),
    credit_available: booleanField(item, "credit_available", "creditAvailable"),
    credit_monthly_payment: numberField(item, "credit_monthly_payment", "creditMonthlyPayment"),
    city: textField(item, "city"),
    avg_price: numberField(item, "avg_price", "avgPrice"),
    market_difference: numberField(item, "market_difference", "marketDifference"),
    market_difference_percent: numberField(item, "market_difference_percent", "marketDifferencePercent")
  };

  return crypto.createHash("sha1").update(JSON.stringify(payload)).digest("hex").slice(0, 16);
}

function buildListingSnapshot(item, { capturedAt } = {}) {
  const listingId = getListingStorageKey(item);
  const snapshotTime = normalizeIsoDate(
    capturedAt
      || item.last_checked_at
      || item.lastCheckedAt
      || item.last_seen_at
      || item.lastSeenAt
      || item.publication_date
      || item.publicationDate
      || new Date().toISOString()
  );

  if (!listingId || !snapshotTime) {
    return null;
  }

  return {
    id: createListingSnapshotId(listingId, snapshotTime),
    listing_id: listingId,
    captured_at: snapshotTime,
    price: numberField(item, "price"),
    publication_date: normalizeIsoDate(textField(item, "publication_date", "publicationDate")),
    last_update: normalizeIsoDate(textField(item, "last_update", "lastUpdate")),
    actuality_status: normalizeActualityStatus(item.actuality_status ?? item.actualityStatus),
    views_count: numberField(item, "views_count", "viewsCount"),
    promotion_type: textField(item, "promotion_type", "promotionType"),
    photo_count: numberField(item, "photo_count", "photoCount"),
    credit_available: booleanField(item, "credit_available", "creditAvailable"),
    credit_monthly_payment: numberField(item, "credit_monthly_payment", "creditMonthlyPayment"),
    city: textField(item, "city"),
    raw_hash: buildListingSnapshotHash(item)
  };
}

function normalizeListingSnapshots(rows) {
  return (Array.isArray(rows) ? rows : [])
    .map(item => {
      const listingId = String(item?.listing_id ?? item?.listingId ?? "").trim();
      const capturedAt = normalizeIsoDate(item?.captured_at ?? item?.capturedAt);
      if (!listingId || !capturedAt) {
        return null;
      }

      return {
        id: String(item?.id || createListingSnapshotId(listingId, capturedAt)).trim(),
        listing_id: listingId,
        captured_at: capturedAt,
        price: numberField(item, "price"),
        publication_date: normalizeIsoDate(item?.publication_date ?? item?.publicationDate),
        last_update: normalizeIsoDate(item?.last_update ?? item?.lastUpdate),
        actuality_status: normalizeActualityStatus(item?.actuality_status ?? item?.actualityStatus),
        views_count: numberField(item, "views_count", "viewsCount"),
        promotion_type: textField(item, "promotion_type", "promotionType"),
        photo_count: numberField(item, "photo_count", "photoCount"),
        credit_available: booleanField(item, "credit_available", "creditAvailable"),
        credit_monthly_payment: numberField(item, "credit_monthly_payment", "creditMonthlyPayment"),
        city: textField(item, "city"),
        raw_hash: String(item?.raw_hash ?? item?.rawHash ?? "").trim() || buildListingSnapshotHash(item)
      };
    })
    .filter(Boolean)
    .sort((left, right) => {
      const timeDiff = new Date(left.captured_at).getTime() - new Date(right.captured_at).getTime();
      if (timeDiff !== 0) {
        return timeDiff;
      }
      return left.listing_id.localeCompare(right.listing_id, "ru");
    });
}

function mergeListingSnapshots(existingRows, listings, { capturedAt } = {}) {
  const existing = normalizeListingSnapshots(existingRows);
  const lastSnapshotByListingId = new Map();
  existing.forEach(item => {
    lastSnapshotByListingId.set(item.listing_id, item);
  });
  const next = [...existing];

  listings.forEach(item => {
    const snapshot = buildListingSnapshot(item, { capturedAt });
    if (!snapshot) {
      return;
    }

    const previous = lastSnapshotByListingId.get(snapshot.listing_id);
    if (previous?.raw_hash === snapshot.raw_hash) {
      return;
    }

    lastSnapshotByListingId.set(snapshot.listing_id, snapshot);
    next.push(snapshot);
  });

  return normalizeListingSnapshots(next);
}

function ensureListingSnapshotsForListings(listings, { capturedAt } = {}) {
  const existing = readListingSnapshots();
  const merged = mergeListingSnapshots(existing, listings, { capturedAt });
  if (merged.length !== existing.length) {
    writeListingSnapshots(merged);
  }
  return merged;
}

function buildListingHistoryMap(listings, snapshotRows) {
  const snapshots = normalizeListingSnapshots(snapshotRows);
  const snapshotsByListingId = new Map();
  snapshots.forEach(snapshot => {
    const bucket = snapshotsByListingId.get(snapshot.listing_id) || [];
    bucket.push(snapshot);
    snapshotsByListingId.set(snapshot.listing_id, bucket);
  });

  const historyMap = new Map();
  const oneDayMs = 24 * 60 * 60 * 1000;

  listings.forEach(item => {
    const listingId = getListingStorageKey(item);
    const history = (snapshotsByListingId.get(listingId) || []).slice().sort((left, right) => (
      new Date(left.captured_at).getTime() - new Date(right.captured_at).getTime()
    ));

    const prices = history
      .map(snapshot => snapshot.price)
      .filter(price => Number.isFinite(price) && price > 0);
    const firstSnapshot = history[0] || null;
    const lastSnapshot = history[history.length - 1] || null;
    const firstPrice = prices[0] ?? (Number.isFinite(item.price) ? item.price : null);
    const lastPrice = prices[prices.length - 1] ?? (Number.isFinite(item.price) ? item.price : null);

    let priceChangeCount = 0;
    let lastPriceChangeAt = "";
    history.forEach((snapshot, index) => {
      if (index === 0) {
        return;
      }
      const previous = history[index - 1];
      if (Number.isFinite(snapshot.price) && Number.isFinite(previous.price) && snapshot.price !== previous.price) {
        priceChangeCount += 1;
        lastPriceChangeAt = snapshot.captured_at;
      }
    });

    let statusChangeCount = 0;
    let wasRelisted = false;
    let sawInactive = false;
    history.forEach((snapshot, index) => {
      const status = normalizeActualityStatus(snapshot.actuality_status);
      if (["archived", "unavailable"].includes(status)) {
        sawInactive = true;
      } else if (sawInactive && status === "active") {
        wasRelisted = true;
      }

      if (index === 0) {
        return;
      }
      if (normalizeActualityStatus(snapshot.actuality_status) !== normalizeActualityStatus(history[index - 1].actuality_status)) {
        statusChangeCount += 1;
      }
    });

    const candidateStartDates = [
      item.publication_date,
      item.first_seen_at,
      firstSnapshot?.publication_date,
      firstSnapshot?.captured_at
    ]
      .map(value => normalizeIsoDate(value))
      .filter(Boolean)
      .map(value => new Date(value).getTime())
      .filter(value => Number.isFinite(value));
    const candidateEndDates = [
      ["archived", "unavailable"].includes(normalizeActualityStatus(item.actuality_status))
        ? (item.last_seen_at || lastSnapshot?.captured_at)
        : new Date().toISOString(),
      item.last_seen_at,
      lastSnapshot?.captured_at
    ]
      .map(value => normalizeIsoDate(value))
      .filter(Boolean)
      .map(value => new Date(value).getTime())
      .filter(value => Number.isFinite(value));

    const startedAtMs = candidateStartDates.length ? Math.min(...candidateStartDates) : null;
    const endedAtMs = candidateEndDates.length ? Math.max(...candidateEndDates) : null;
    const daysOnMarket = startedAtMs !== null && endedAtMs !== null
      ? Math.max(1, Math.floor((endedAtMs - startedAtMs) / oneDayMs) + 1)
      : null;

    historyMap.set(listingId, {
      snapshot_count: history.length,
      first_price: firstPrice,
      last_price: lastPrice,
      price_change_count: priceChangeCount,
      price_drop_total: Number.isFinite(firstPrice) && Number.isFinite(lastPrice) ? Math.max(firstPrice - lastPrice, 0) : null,
      last_price_change_at: lastPriceChangeAt,
      status_change_count: statusChangeCount,
      was_relisted: wasRelisted,
      days_on_market: daysOnMarket,
      history_first_seen_at: firstSnapshot?.captured_at || normalizeIsoDate(item.first_seen_at),
      history_last_seen_at: lastSnapshot?.captured_at || normalizeIsoDate(item.last_seen_at),
      history_available: history.length > 0
    });
  });

  return historyMap;
}

function enrichListingsWithHistory(listings, snapshotRows = readListingSnapshots()) {
  const historyMap = buildListingHistoryMap(listings, snapshotRows);
  return listings.map(item => {
    const history = historyMap.get(getListingStorageKey(item));
    return history ? { ...item, ...history } : item;
  });
}

function buildSellerAnalysisMap(listings) {
  const groups = new Map();

  listings.forEach(item => {
    const sellerUserId = String(item.seller_user_id || "").trim();
    if (!sellerUserId) {
      return;
    }

    const bucket = groups.get(sellerUserId) || [];
    bucket.push(item);
    groups.set(sellerUserId, bucket);
  });

  const analysisMap = new Map();

  groups.forEach((items, sellerUserId) => {
    const totalListingCount = items.length;
    const activeItems = items.filter(item => normalizeActualityStatus(item.actuality_status) === "active");
    const activeListingCount = activeItems.length;
    const brandCounts = new Map();
    const modelCounts = new Map();
    const cityCounts = new Map();
    let belowMarketCount = 0;
    let staleCount = 0;
    let promotedCount = 0;
    let relistedCount = 0;
    let dealerSignals = 0;
    let riskSum = 0;
    let riskCount = 0;

    items.forEach(item => {
      const brandKey = normalizeLookupText(item.brand);
      const modelKey = normalizeLookupText([item.brand, item.model].filter(Boolean).join(" "));
      const cityKey = normalizeLookupText(item.city);

      if (brandKey) {
        brandCounts.set(brandKey, (brandCounts.get(brandKey) || 0) + 1);
      }
      if (modelKey) {
        modelCounts.set(modelKey, (modelCounts.get(modelKey) || 0) + 1);
      }
      if (cityKey) {
        cityCounts.set(cityKey, (cityCounts.get(cityKey) || 0) + 1);
      }
      if (Number(item.market_difference_percent) >= 12) {
        belowMarketCount += 1;
      }
      if (
        normalizeActualityStatus(item.actuality_status) === "stale" ||
        Number(item.days_on_market) >= 30
      ) {
        staleCount += 1;
      }
      if (Array.isArray(item.paid_services) && item.paid_services.length) {
        promotedCount += 1;
      }
      if (item.was_relisted) {
        relistedCount += 1;
      }
      if (item.is_verified_dealer || item.is_used_car_dealer || [2, 4, 5].includes(Number(item.seller_type_id))) {
        dealerSignals += 1;
      }
      if (Number.isFinite(Number(item.risk_score))) {
        riskSum += Number(item.risk_score);
        riskCount += 1;
      }
    });

    const brandEntries = [...brandCounts.entries()].sort((left, right) => right[1] - left[1]);
    const modelEntries = [...modelCounts.entries()].sort((left, right) => right[1] - left[1]);
    const cityEntries = [...cityCounts.entries()].sort((left, right) => right[1] - left[1]);
    const brandCount = brandEntries.length;
    const modelCount = modelEntries.length;
    const cityCount = cityEntries.length;
    const dominantBrand = brandEntries[0]?.[0] || "";
    const dominantBrandCount = brandEntries[0]?.[1] || 0;
    const dominantModel = modelEntries[0]?.[0] || "";
    const dominantModelCount = modelEntries[0]?.[1] || 0;
    const averageRisk = riskCount ? Number((riskSum / riskCount).toFixed(1)) : null;
    const belowMarketShare = totalListingCount ? belowMarketCount / totalListingCount : 0;
    const staleShare = totalListingCount ? staleCount / totalListingCount : 0;
    const promotedShare = totalListingCount ? promotedCount / totalListingCount : 0;
    const relistedShare = totalListingCount ? relistedCount / totalListingCount : 0;

    let traderScore = 12;
    traderScore += Math.min(totalListingCount - 1, 8) * 6;
    traderScore += Math.max(0, brandCount - 1) * 7;
    traderScore += Math.max(0, cityCount - 1) * 5;
    traderScore += dealerSignals > 0 ? 26 : 0;
    traderScore += promotedShare >= 0.6 ? 8 : 0;
    traderScore += dominantModelCount >= 2 ? 6 : 0;
    traderScore += relistedShare >= 0.4 ? 4 : 0;
    traderScore -= totalListingCount === 1 && dealerSignals === 0 ? 8 : 0;
    traderScore = Math.max(0, Math.min(100, Math.round(traderScore)));

    let profileType = "private";
    let profileLabel = "Частник";
    if (dealerSignals > 0) {
      profileType = "dealer";
      profileLabel = "Дилер";
    } else if (totalListingCount >= 7 || (totalListingCount >= 4 && brandCount >= 2)) {
      profileType = "reseller";
      profileLabel = "Похоже на поток";
    } else if (totalListingCount >= 3) {
      profileType = "multi_listing";
      profileLabel = "Несколько авто";
    }

    const notes = [
      `${activeListingCount} акт. из ${totalListingCount} в базе`,
      brandCount > 0 ? `${brandCount} марок` : "",
      cityCount > 1 ? `${cityCount} городов` : "",
      belowMarketShare >= 0.35 ? "часто ниже рынка" : "",
      staleShare >= 0.5 ? "много долгих объявлений" : "",
      dealerSignals > 0 ? "есть дилерские сигналы" : ""
    ].filter(Boolean);

    analysisMap.set(sellerUserId, {
      seller_user_id: sellerUserId,
      profile_type: profileType,
      profile_label: profileLabel,
      trader_score: traderScore,
      total_listing_count: totalListingCount,
      active_listing_count: activeListingCount,
      brand_count: brandCount,
      model_count: modelCount,
      city_count: cityCount,
      dominant_brand: dominantBrand,
      dominant_brand_count: dominantBrandCount,
      dominant_model: dominantModel,
      dominant_model_count: dominantModelCount,
      below_market_count: belowMarketCount,
      stale_count: staleCount,
      promoted_count: promotedCount,
      relisted_count: relistedCount,
      average_risk: averageRisk,
      note: notes.join(" · ")
    });
  });

  return analysisMap;
}

function enrichListingsWithSellerAnalysis(listings) {
  const analysisMap = buildSellerAnalysisMap(listings);
  return listings.map(item => {
    const sellerUserId = String(item.seller_user_id || "").trim();
    const sellerAnalysis = sellerUserId ? analysisMap.get(sellerUserId) : null;
    return sellerAnalysis
      ? { ...item, seller_analysis: sellerAnalysis }
      : item;
  });
}

function enrichListingsForClient(listings, snapshotRows = readListingSnapshots(), catalog = readAutopartsCatalog(), aliasesStore = readAutopartsAliases()) {
  const withHistory = enrichListingsWithHistory(listings, snapshotRows);
  const withAutoparts = enrichListingsWithAutoparts(withHistory, catalog, aliasesStore);
  return enrichListingsWithSellerAnalysis(withAutoparts);
}

function buildRiskSummary(item) {
  let score = 0;
  const flags = [];

  if (item.actuality_status === "archived" || item.actuality_status === "unavailable") {
    score += 50;
    flags.push("Объявление недоступно или в архиве");
  } else if (item.actuality_status === "stale") {
    score += 18;
    flags.push("Давно не подтверждалось");
  }

  if (Number(item.photo_count) > 0 && Number(item.photo_count) < 5) {
    score += 10;
    flags.push("Мало фотографий");
  }

  if (Number(item.owners) >= 4) {
    score += 8;
    flags.push("Много владельцев");
  }

  if (item.description && item.description.length < 80) {
    score += 7;
    flags.push("Короткое описание");
  }

  if (Number.isFinite(Number(item.market_difference_percent)) && Number(item.market_difference_percent) > 15) {
    score += 14;
    flags.push("Сильно ниже среднего рынка");
  }

  if (normalizeBoolean(item.public_history_available)) {
    score = Math.max(score - 5, 0);
    flags.push("Есть публичная история на странице");
  }

  return {
    risk_score: Math.min(score, 100),
    risk_flags: flags.slice(0, 6)
  };
}

function normalizeListings(rows) {
  return rows
    .map(item => {
      const title = String(item.title || item.name || "Без названия").trim();
      const description = textField(item, "description");
      const titleMeta = extractTitleBrandModel(title, item.brand, item.model);
      const descriptionMeta = extractDescriptionMeta(description, { title, alt: "" });
      const photoGallery = normalizePhotoGallery(item.photo_gallery || item.photoGallery || []);
      const image = normalizeRemoteUrl(item.image) || photoGallery[0] || "";
      const resolvedPhotoGallery = photoGallery.length ? photoGallery : (image ? [image] : []);
      const mileage = numberField(item, "mileage") > 0 ? numberField(item, "mileage") : (descriptionMeta.mileage || null);
      const engineVolume = numberField(item, "engine_volume", "engineVolume") > 0
        ? numberField(item, "engine_volume", "engineVolume")
        : (descriptionMeta.engineVolume || null);
      const photoCount = numberField(item, "photo_count", "photoCount") > 0
        ? numberField(item, "photo_count", "photoCount")
        : (resolvedPhotoGallery.length || null);
      const marketDifference = numberField(item, "market_difference", "marketDifference");
      const marketDifferencePercent = numberField(item, "market_difference_percent", "marketDifferencePercent");

      return {
        title,
        price: numberField(item, "price") || 0,
        year: numberField(item, "year") > 0 ? numberField(item, "year") : null,
        mileage,
        owners: numberField(item, "owners") > 0 ? numberField(item, "owners") : null,
        city: textField(item, "city"),
        url: textField(item, "url"),
        image,
        photo_gallery: resolvedPhotoGallery,
        description,
        source: textField(item, "source"),
        brand: textField(item, "brand") || titleMeta.brand,
        model: textField(item, "model") || titleMeta.model,
        fuel_type: textField(item, "fuel_type", "fuelType") || descriptionMeta.fuelType,
        transmission: textField(item, "transmission") || descriptionMeta.transmission,
        body_type: textField(item, "body_type", "bodyType") || descriptionMeta.bodyType,
        drive_type: textField(item, "drive_type", "driveType") || descriptionMeta.driveType,
        steering_side: textField(item, "steering_side", "steeringSide") || descriptionMeta.steeringSide,
        color: textField(item, "color") || descriptionMeta.color,
        options: listField(item, "options"),
        vin: normalizeVin(textField(item, "vin")),
        vin_note: textField(item, "vin_note", "vinNote"),
        repair_state: normalizeRepairState(item.repair_state ?? item.repairState ?? descriptionMeta.repairState),
        advert_id: textField(item, "advert_id", "advertId") || extractAdvertIdFromUrl(item.url),
        engine_volume: engineVolume,
        publication_date: textField(item, "publication_date", "publicationDate"),
        last_update: textField(item, "last_update", "lastUpdate"),
        first_seen_at: normalizeIsoDate(item.first_seen_at ?? item.firstSeenAt),
        last_seen_at: normalizeIsoDate(item.last_seen_at ?? item.lastSeenAt),
        last_checked_at: normalizeIsoDate(item.last_checked_at ?? item.lastCheckedAt),
        last_status_change_at: normalizeIsoDate(item.last_status_change_at ?? item.lastStatusChangeAt),
        actuality_status: normalizeActualityStatus(item.actuality_status ?? item.actualityStatus),
        hidden_after_import: booleanField(item, "hidden_after_import", "hiddenAfterImport"),
        hidden_reason: textField(item, "hidden_reason", "hiddenReason"),
        hidden_at: normalizeIsoDate(item.hidden_at ?? item.hiddenAt),
        import_batch_id: textField(item, "import_batch_id", "importBatchId"),
        import_scope_label: textField(item, "import_scope_label", "importScopeLabel"),
        archived_at: normalizeIsoDate(item.archived_at ?? item.archivedAt),
        archive_reason: textField(item, "archive_reason", "archiveReason"),
        avg_price: numberField(item, "avg_price", "avgPrice") > 0 ? numberField(item, "avg_price", "avgPrice") : null,
        market_difference: marketDifference,
        market_difference_percent: marketDifferencePercent,
        photo_count: photoCount,
        phone_count: numberField(item, "phone_count", "phoneCount") > 0 ? numberField(item, "phone_count", "phoneCount") : null,
        phone_prefix: textField(item, "phone_prefix", "phonePrefix"),
        credit_available: booleanField(item, "credit_available", "creditAvailable"),
        paid_services: listField(item, "paid_services", "paidServices"),
        credit_monthly_payment: numberField(item, "credit_monthly_payment", "creditMonthlyPayment") > 0
          ? numberField(item, "credit_monthly_payment", "creditMonthlyPayment")
          : null,
        credit_down_payment: numberField(item, "credit_down_payment", "creditDownPayment") > 0
          ? numberField(item, "credit_down_payment", "creditDownPayment")
          : null,
        seller_user_id: textField(item, "seller_user_id", "sellerUserId"),
        seller_type_id: numberField(item, "seller_type_id", "sellerTypeId") > 0
          ? numberField(item, "seller_type_id", "sellerTypeId")
          : null,
        is_verified_dealer: booleanField(item, "is_verified_dealer", "isVerifiedDealer"),
        is_used_car_dealer: booleanField(item, "is_used_car_dealer", "isUsedCarDealer"),
        public_history_available: booleanField(item, "public_history_available", "publicHistoryAvailable"),
        history_summary: textField(item, "history_summary", "historySummary"),
        risk_score: numberField(item, "risk_score", "riskScore"),
        risk_flags: listField(item, "risk_flags", "riskFlags")
      };
    })
    .map(item => ({
      ...item,
      actuality_status: resolveActualityStatus(item)
    }))
    .map(item => ({
      ...item,
      ...buildRiskSummary(item)
    }))
    .filter(item => item.title && item.price > 0);
}

function shouldArchiveListing(item, currentTime = Date.now()) {
  const hiddenAt = normalizeIsoDate(item.hidden_at ?? item.hiddenAt);
  if (hiddenAt) {
    const hiddenMs = new Date(hiddenAt).getTime();
    if (Number.isFinite(hiddenMs) && currentTime - hiddenMs >= ARCHIVE_AFTER_MS) {
      return "hidden_timeout";
    }
  }

  const lastSeenAt = normalizeIsoDate(item.last_seen_at ?? item.lastSeenAt ?? item.last_checked_at ?? item.lastCheckedAt);
  if (lastSeenAt) {
    const lastSeenMs = new Date(lastSeenAt).getTime();
    if (
      Number.isFinite(lastSeenMs) &&
      currentTime - lastSeenMs >= ARCHIVE_AFTER_MS &&
      ["stale", "archived", "unavailable"].includes(normalizeActualityStatus(item.actuality_status))
    ) {
      return "stale_timeout";
    }
  }

  return "";
}

function syncListingArchiveStorage(listings, archivedRows = readArchivedListings()) {
  const currentTime = Date.now();
  const archiveByKey = new Map(
    normalizeListings(archivedRows).map(item => [getListingStorageKey(item), item])
  );
  const activeItems = [];
  let changed = false;

  normalizeListings(listings).forEach(item => {
    const archiveReason = shouldArchiveListing(item, currentTime);
    if (!archiveReason) {
      activeItems.push(item);
      return;
    }

    const key = getListingStorageKey(item);
    const previous = archiveByKey.get(key);
    archiveByKey.set(key, {
      ...previous,
      ...item,
      archived_at: previous?.archived_at || new Date(currentTime).toISOString(),
      archive_reason: archiveReason
    });
    changed = true;
  });

  if (changed) {
    writeListings(activeItems);
    writeArchivedListings([...archiveByKey.values()]);
  }

  return {
    activeItems,
    archivedItems: [...archiveByKey.values()],
    changed
  };
}

function createDefaultProfile(id = "default", name = "Основной") {
  return {
    id,
    name,
    favorites: [],
    comparisonHistory: []
  };
}

function normalizeProfile(profile, fallbackId = "default", fallbackName = "Основной") {
  return {
    id: String(profile?.id || fallbackId),
    name: String(profile?.name || fallbackName),
    favorites: Array.isArray(profile?.favorites) ? profile.favorites.map(String).slice(0, 200) : [],
    comparisonHistory: Array.isArray(profile?.comparisonHistory)
      ? profile.comparisonHistory
          .filter(item => item && typeof item === "object")
          .map(item => ({
            createdAt: String(item.createdAt || ""),
            winnerId: String(item.winnerId || ""),
            compareIds: Array.isArray(item.compareIds) ? item.compareIds.map(String).slice(0, 3) : [],
            winnerTitle: String(item.winnerTitle || ""),
            titles: Array.isArray(item.titles) ? item.titles.map(String).slice(0, 3) : []
          }))
          .slice(0, 50)
      : []
  };
}

function createDefaultAuthStore() {
  return {
    users: [],
    sessions: []
  };
}

function normalizeUser(user, index) {
  const username = String(user?.username || `user${index + 1}`).trim();
  const profiles = Array.isArray(user?.profiles) && user.profiles.length
    ? user.profiles.map((profile, profileIndex) =>
        normalizeProfile(profile, profileIndex === 0 ? "default" : `profile-${profileIndex + 1}`, `Профиль ${profileIndex + 1}`)
      )
    : [createDefaultProfile()];
  const activeProfileId = profiles.some(profile => profile.id === user?.activeProfileId)
    ? String(user.activeProfileId)
    : profiles[0].id;

  return {
    id: String(user?.id || `user-${index + 1}`),
    username,
    usernameKey: String(user?.usernameKey || username.toLowerCase()),
    passwordHash: String(user?.passwordHash || ""),
    passwordSalt: String(user?.passwordSalt || ""),
    activeProfileId,
    profiles
  };
}

function normalizeAuthStore(payload) {
  const store = createDefaultAuthStore();
  store.users = Array.isArray(payload?.users)
    ? payload.users.map((user, index) => normalizeUser(user, index))
    : [];
  store.sessions = Array.isArray(payload?.sessions)
    ? payload.sessions
        .filter(session => session && typeof session === "object")
        .map(session => ({
          token: String(session.token || ""),
          userId: String(session.userId || ""),
          createdAt: String(session.createdAt || "")
        }))
        .filter(session => session.token && store.users.some(user => user.id === session.userId))
        .slice(0, 500)
    : [];
  return store;
}

function readAuthStore() {
  ensureDataDir();
  if (!fs.existsSync(AUTH_FILE)) {
    return createDefaultAuthStore();
  }

  const raw = fs.readFileSync(AUTH_FILE, "utf-8");
  return normalizeAuthStore(JSON.parse(raw));
}

function writeAuthStore(store) {
  ensureDataDir();
  fs.writeFileSync(AUTH_FILE, JSON.stringify(normalizeAuthStore(store), null, 2), "utf-8");
}

function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const passwordHash = crypto.scryptSync(password, salt, 64).toString("hex");
  return { passwordHash, passwordSalt: salt };
}

function verifyPassword(password, user) {
  if (!user.passwordSalt || !user.passwordHash) {
    return false;
  }
  const hashed = crypto.scryptSync(password, user.passwordSalt, 64);
  const existing = Buffer.from(user.passwordHash, "hex");
  return existing.length === hashed.length && crypto.timingSafeEqual(existing, hashed);
}

function sanitizeUser(user) {
  return {
    id: user.id,
    username: user.username
  };
}

function buildClientStateForUser(user) {
  const active = user.profiles.find(profile => profile.id === user.activeProfileId) || user.profiles[0];
  return {
    activeProfileId: active.id,
    profiles: user.profiles.map(profile => ({
      id: profile.id,
      name: profile.name
    })),
    favorites: active.favorites,
    comparisonHistory: active.comparisonHistory
  };
}

function createUserId(username) {
  const slug = String(username || "")
    .toLowerCase()
    .replace(/[^a-z0-9а-яё]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
  return `${slug || "user"}-${Date.now()}`;
}

function createProfileId(name) {
  const slug = String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9а-яё]+/gi, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
  return `${slug || "profile"}-${Date.now()}`;
}

function createSession(store, userId) {
  const token = crypto.randomBytes(24).toString("hex");
  store.sessions = [
    {
      token,
      userId,
      createdAt: new Date().toISOString()
    },
    ...store.sessions.filter(session => session.userId !== userId)
  ].slice(0, 500);
  return token;
}

function extractAuthToken(request) {
  const header = String(request.headers.authorization || "").trim();
  if (!header.toLowerCase().startsWith("bearer ")) {
    return "";
  }
  return header.slice(7).trim();
}

function getAuthContext(request) {
  const token = extractAuthToken(request);
  const store = readAuthStore();
  if (!token) {
    return { store, token: "", user: null, session: null };
  }

  const session = store.sessions.find(item => item.token === token) || null;
  const user = session ? store.users.find(item => item.id === session.userId) || null : null;
  return { store, token, session, user };
}

function requireAuth(request, response) {
  const context = getAuthContext(request);
  if (!context.user) {
    sendJson(response, 401, { error: "Требуется вход в аккаунт." });
    return null;
  }
  return context;
}

function updateActiveUserState(user, payload) {
  const activeProfile = user.profiles.find(profile => profile.id === user.activeProfileId) || user.profiles[0];
  activeProfile.favorites = Array.isArray(payload.favorites) ? payload.favorites.map(String).slice(0, 200) : [];
  activeProfile.comparisonHistory = Array.isArray(payload.comparisonHistory)
    ? normalizeProfile({ comparisonHistory: payload.comparisonHistory }).comparisonHistory
    : [];
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function parseNumber(value) {
  const digits = String(value || "").replace(/[^\d]/g, "");
  return digits ? Number(digits) : 0;
}

const KOLESA_MONTHS = {
  "января": 0,
  "февраля": 1,
  "марта": 2,
  "апреля": 3,
  "мая": 4,
  "июня": 5,
  "июля": 6,
  "августа": 7,
  "сентября": 8,
  "октября": 9,
  "ноября": 10,
  "декабря": 11
};

function parseKolesaCardDate(text, now = new Date()) {
  const normalized = normalizeWhitespace(text).toLowerCase();
  if (!normalized) {
    return "";
  }

  const baseDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  if (normalized === "сегодня") {
    return baseDate.toISOString();
  }

  if (normalized === "вчера") {
    baseDate.setUTCDate(baseDate.getUTCDate() - 1);
    return baseDate.toISOString();
  }

  const match = normalized.match(/^(\d{1,2})\s+([а-яё]+)$/i);
  if (!match) {
    return "";
  }

  const day = Number(match[1]);
  const monthIndex = KOLESA_MONTHS[match[2]];
  if (!Number.isFinite(day) || monthIndex === undefined) {
    return "";
  }

  let year = now.getUTCFullYear();
  if (monthIndex > now.getUTCMonth()) {
    year -= 1;
  }

  const date = new Date(Date.UTC(year, monthIndex, day));
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}

function extractCityFromAlt(alt) {
  const match = normalizeWhitespace(alt).match(/в\s+([^–-]+?)(?:\s+[–-]|$)/i);
  return match ? match[1].trim() : "";
}

function extractYear(description, alt) {
  const sources = [description, alt];
  for (const source of sources) {
    const match = normalizeWhitespace(source).match(/\b(19\d{2}|20\d{2})\b/);
    if (match) {
      return Number(match[1]);
    }
  }
  return 0;
}

function extractMileage(description) {
  const match = normalizeWhitespace(description).match(/с\s+пробегом\s+([\d\s]+)\s*км/i);
  return match ? parseNumber(match[1]) : 0;
}

function extractEngineVolume(description) {
  const match = normalizeWhitespace(description).match(/(\d(?:[.,]\d)?)\s*л(?:[\s,.;:/-]|$)/i);
  return match ? Number(match[1].replace(",", ".")) : 0;
}

function extractDescriptionMeta(description, { title = "", alt = "" } = {}) {
  const normalizedDescription = normalizeWhitespace(description);
  const sourceText = [title, normalizedDescription, alt].filter(Boolean).join(", ");
  const transmission = detectTransmission(sourceText);
  const fuelType = detectFuelType(sourceText);
  const driveType = detectDriveType(sourceText);
  const steeringSide = detectSteeringSide(sourceText);
  const color = detectColor(sourceText);
  const options = detectOptions(sourceText);
  const repairState = /не на ходу|после дтп|аварийн|требует ремонта|на разбор/i.test(sourceText)
    ? "yes"
    : /не аварийн|в хорошем состоянии|в отличном состоянии/i.test(sourceText)
      ? "no"
      : "unknown";
  const bodyTypeMatch = normalizedDescription.match(
    /(?:^|[\s,.;:()/-])(седан|универсал|хэтчбек|купе|кабриолет|микровэн|фургон|внедорожник|пикап|кроссовер|минивэн|микроавтобус|лифтбек)(?=$|[\s,.;:()/-])/i
  );

  return {
    year: extractYear(normalizedDescription, alt),
    mileage: extractMileage(normalizedDescription),
    engineVolume: extractEngineVolume(normalizedDescription),
    fuelType,
    driveType,
    steeringSide,
    color,
    options,
    repairState,
    bodyType: bodyTypeMatch ? bodyTypeMatch[1] : "",
    transmission
  };
}

function extractCardRegion(card) {
  return normalizeWhitespace(
    card.find('[data-test="region"]').first().text() ||
    card.find(".a-card__data .a-card__param").first().text()
  );
}

function extractCardPublicationDate(card) {
  const rawDate = normalizeWhitespace(
    card.find(".a-card__param--date").first().text()
  );
  return parseKolesaCardDate(rawDate);
}

function extractImage(card) {
  const src =
    card.find(".thumb-gallery__pic img").first().attr("src") ||
    card.find("img").first().attr("src") ||
    "";
  return normalizeRemoteUrl(src);
}

function extractOfferParameters($) {
  const parameters = {
    city: "",
    generation: "",
    bodyType: "",
    engineVolume: null,
    fuelType: "",
    mileage: null,
    transmission: "",
    driveType: "",
    steeringSide: "",
    color: ""
  };

  $(".offer__parameters dl").each((_, element) => {
    const key = normalizeWhitespace($(element).find("dt").first().text()).toLowerCase();
    const value = normalizeWhitespace($(element).find("dd").first().text());
    if (!key || !value) {
      return;
    }

    if (/^город/.test(key)) {
      parameters.city = value.split(",")[0].trim();
      return;
    }

    if (/^поколение/.test(key)) {
      parameters.generation = value;
      return;
    }

    if (/^кузов/.test(key)) {
      parameters.bodyType = value.toLowerCase();
      return;
    }

    if (/объем двигателя/.test(key)) {
      parameters.engineVolume = extractEngineVolume(value);
      parameters.fuelType = detectFuelType(value) || parameters.fuelType;
      return;
    }

    if (/^пробег/.test(key)) {
      parameters.mileage = parseNumber(value);
      return;
    }

    if (/коробка передач/.test(key)) {
      parameters.transmission = detectTransmission(value) || value;
      return;
    }

    if (/^привод/.test(key)) {
      parameters.driveType = detectDriveType(value) || value;
      return;
    }

    if (/^руль/.test(key)) {
      parameters.steeringSide = detectSteeringSide(value) || value;
      return;
    }

    if (/^цвет/.test(key)) {
      parameters.color = detectColor(value) || value;
    }
  });

  return parameters;
}

function extractGalleryImages($, advertData = {}) {
  const candidates = [];
  const pushCandidate = value => {
    const normalized = normalizeRemoteUrl(value);
    if (!normalized) {
      return;
    }

    candidates.push(normalized);

    const fullVariant = normalized.replace(/-\d+x\d+\.(?:jpg|jpeg|png|webp)$/i, "-full.jpg");
    if (fullVariant !== normalized) {
      candidates.push(fullVariant);
    }
  };

  $(".gallery__main [data-href], .gallery__thumb-image[data-href], .js__gallery-thumb[data-href]").each((_, element) => {
    pushCandidate($(element).attr("data-href"));
  });

  $(".gallery__main img, .gallery__thumb-image img").each((_, element) => {
    pushCandidate($(element).attr("src"));
    pushCandidate($(element).attr("data-src"));
  });

  pushCandidate(advertData?.photo?.path);
  pushCandidate(advertData?.photo?.src);
  return normalizePhotoGallery(candidates);
}

function extractAssignedJsonObject(html, marker) {
  const startIndex = html.indexOf(marker);
  if (startIndex === -1) {
    return null;
  }

  const jsonStart = html.indexOf("{", startIndex);
  if (jsonStart === -1) {
    return null;
  }

  let depth = 0;
  let inString = false;
  let isEscaped = false;

  for (let index = jsonStart; index < html.length; index += 1) {
    const char = html[index];

    if (inString) {
      if (isEscaped) {
        isEscaped = false;
      } else if (char === "\\") {
        isEscaped = true;
      } else if (char === "\"") {
        inString = false;
      }
      continue;
    }

    if (char === "\"") {
      inString = true;
      continue;
    }

    if (char === "{") {
      depth += 1;
      continue;
    }

    if (char === "}") {
      depth -= 1;
      if (depth === 0) {
        return html.slice(jsonStart, index + 1);
      }
    }
  }

  return null;
}

function extractAssignedJsonObjects(html, marker) {
  const objects = [];
  let searchFrom = 0;

  while (searchFrom < html.length) {
    const startIndex = html.indexOf(marker, searchFrom);
    if (startIndex === -1) {
      break;
    }

    const jsonStart = html.indexOf("{", startIndex);
    if (jsonStart === -1) {
      break;
    }

    let depth = 0;
    let inString = false;
    let isEscaped = false;

    for (let index = jsonStart; index < html.length; index += 1) {
      const char = html[index];

      if (inString) {
        if (isEscaped) {
          isEscaped = false;
        } else if (char === "\\") {
          isEscaped = true;
        } else if (char === "\"") {
          inString = false;
        }
        continue;
      }

      if (char === "\"") {
        inString = true;
        continue;
      }

      if (char === "{") {
        depth += 1;
        continue;
      }

      if (char === "}") {
        depth -= 1;
        if (depth === 0) {
          objects.push(html.slice(jsonStart, index + 1));
          searchFrom = index + 1;
          break;
        }
      }
    }

    if (searchFrom <= startIndex) {
      break;
    }
  }

  return objects;
}

function extractKolesaSearchResultCount(html) {
  const dataText = extractAssignedJsonObject(html, "var data =");
  if (!dataText) {
    return null;
  }

  try {
    const data = JSON.parse(dataText);
    const resultCount = Number(data?.searchAnalyticsData?.result_count);
    return Number.isFinite(resultCount) && resultCount >= 0 ? resultCount : null;
  } catch (error) {
    return null;
  }
}

function formatModelLabelFromSlug(slug) {
  return String(slug || "")
    .split("-")
    .filter(Boolean)
    .map(part => {
      if (/^\d+$/.test(part)) {
        return part;
      }
      if (/^[a-z]$/.test(part)) {
        return part.toUpperCase();
      }
      return part.charAt(0).toUpperCase() + part.slice(1);
    })
    .join(" ");
}

function extractKolesaModelOptions(html, mark, city = "") {
  const normalizedMark = String(mark || "").trim().toLowerCase();
  const normalizedCity = String(city || "").trim().toLowerCase();
  if (!normalizedMark) {
    return [];
  }

  const escapedMark = normalizedMark.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const cityPart = normalizedCity ? `${normalizedCity.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/` : "(?:[^/]+/)?";
  const pattern = new RegExp(`/cars/${cityPart}${escapedMark}/([a-z0-9-]+)`, "gi");
  const ignored = new Set(["new", "avtokredit", "price", normalizedCity].filter(Boolean));
  const found = new Map();
  let match = pattern.exec(html);

  while (match) {
    const slug = String(match[1] || "").trim().toLowerCase();
    if (slug && !ignored.has(slug)) {
      found.set(slug, {
        value: slug,
        label: formatModelLabelFromSlug(slug)
      });
    }
    match = pattern.exec(html);
  }

  return [...found.values()].sort((left, right) => left.label.localeCompare(right.label, "ru"));
}

function extractAdvertIdFromUrl(url) {
  const match = String(url || "").match(/\/a\/show\/(\d+)/);
  return match ? String(match[1]) : "";
}

function listingMatchesTarget(item, { targetUrl = "", advertId = "" } = {}) {
  const normalizedTargetUrl = String(targetUrl || "").trim();
  const normalizedAdvertId = String(advertId || extractAdvertIdFromUrl(normalizedTargetUrl) || "").trim();
  const itemUrl = String(item?.url || "").trim();
  const itemAdvertId = String(item?.advert_id || extractAdvertIdFromUrl(itemUrl) || "").trim();

  if (normalizedAdvertId && itemAdvertId && normalizedAdvertId === itemAdvertId) {
    return true;
  }

  if (!normalizedTargetUrl) {
    return false;
  }

  return itemUrl === normalizedTargetUrl;
}

function extractCreditTerms($) {
  const amounts = [];
  $('[data-test="credit-wrap"] .a-credit__amount').each((_, element) => {
    const amount = parseNumber($(element).text());
    if (amount > 0) {
      amounts.push(amount);
    }
  });

  return {
    credit_monthly_payment: amounts[0] || null,
    credit_down_payment: amounts[1] || null
  };
}

function extractPublicHistorySummary(html) {
  const match = String(html || "").match(/История авто[\s\S]{0,280}/i);
  if (!match) {
    return {
      public_history_available: false,
      history_summary: ""
    };
  }

  const summary = normalizeWhitespace(
    match[0]
      .replace(/<[^>]+>/g, " ")
      .replace(/&nbsp;/g, " ")
  );

  return {
    public_history_available: true,
    history_summary: summary.slice(0, 240)
  };
}

async function fetchKolesaPriceInsight(advertUrl) {
  const snapshot = await fetchKolesaListingSnapshot(advertUrl);
  if (snapshot.actuality_status !== "active") {
    throw new Error(snapshot.actuality_status === "archived" ? "listing-archived" : "digital-data-not-found");
  }

  if (!snapshot.avg_price || !snapshot.price) {
    throw new Error("avg-price-not-found");
  }

  return {
    avgPrice: snapshot.avg_price,
    currentPrice: snapshot.price,
    marketDifference: snapshot.market_difference,
    marketDifferencePercent: snapshot.market_difference_percent,
    marketPosition: snapshot.market_difference > 0 ? "below" : snapshot.market_difference < 0 ? "above" : "equal",
    brand: String(snapshot.brand || ""),
    model: String(snapshot.model || ""),
    city: String(snapshot.city || "")
  };
}

async function fetchKolesaListingSnapshot(advertUrl, { force = false, jobId = "" } = {}) {
  const parsedUrl = new URL(advertUrl);
  if (!parsedUrl.hostname.endsWith("kolesa.kz")) {
    throw new Error("unsupported-host");
  }

  const cacheKey = normalizeKolesaCacheKey(advertUrl);
  if (!force) {
    const cached = readCache(listingSnapshotCache, cacheKey);
    if (cached) {
      return cached;
    }
  }

  const checkedAt = new Date().toISOString();
  const remote = await fetchKolesaHtml(parsedUrl.toString(), {
    jobId,
    context: "карточка объявления",
    guardScope: "detail"
  });

  if (remote.status === 404) {
    return {
      actuality_status: "archived",
      last_checked_at: checkedAt
    };
  }

  const html = remote.text;
  const $ = cheerio.load(html);
  const jsonText = extractAssignedJsonObject(html, "window.digitalData =");
  if (!jsonText) {
    return {
      actuality_status: "unavailable",
      last_checked_at: checkedAt
    };
  }
  const pageDataText = extractAssignedJsonObject(html, "var data =");

  const digitalData = JSON.parse(jsonText);
  const product = digitalData?.product || {};
  const pageData = pageDataText ? JSON.parse(pageDataText) : {};
  const advertData = pageData?.advert || {};
  const avgPrice = Number(product?.attributes?.avgPrice) || 0;
  const currentPrice = Number(product?.unitPrice) || 0;
  const marketDifference = avgPrice - currentPrice;
  const marketDifferencePercent = avgPrice
    ? Number(((marketDifference / avgPrice) * 100).toFixed(2))
    : null;
  const creditTerms = extractCreditTerms($);
  const publicHistory = extractPublicHistorySummary(html);
  const photoGallery = extractGalleryImages($, advertData);
  const offerParams = extractOfferParameters($);
  const description = normalizeWhitespace(
    String(advertData?.descriptionText || "")
      .replace(/<br\s*\/?>/gi, "\n")
  );
  const title = String(advertData?.title || product?.name || "");
  const titleMeta = extractTitleBrandModel(title, product?.attributes?.brand, product?.attributes?.model);
  const descriptionMeta = extractDescriptionMeta(description, {
    title,
    alt: String(advertData?.photo?.alt || "")
  });
  const snapshot = {
    actuality_status: "active",
    advert_id: String(product?.id || extractAdvertIdFromUrl(advertUrl)),
    title,
    price: currentPrice || null,
    image: photoGallery[0] || normalizeRemoteUrl(advertData?.photo?.src || advertData?.photo?.path),
    photo_gallery: photoGallery,
    publication_date: String(product?.publicationDate || ""),
    last_update: String(product?.lastUpdate || ""),
    avg_price: avgPrice || null,
    market_difference: avgPrice && currentPrice ? marketDifference : null,
    market_difference_percent: avgPrice && currentPrice ? marketDifferencePercent : null,
    brand: titleMeta.brand,
    model: titleMeta.model,
    fuel_type: offerParams.fuelType || descriptionMeta.fuelType,
    drive_type: offerParams.driveType || descriptionMeta.driveType,
    steering_side: offerParams.steeringSide || descriptionMeta.steeringSide,
    color: offerParams.color || descriptionMeta.color,
    options: descriptionMeta.options,
    repair_state: descriptionMeta.repairState,
    transmission: offerParams.transmission || descriptionMeta.transmission,
    body_type: offerParams.bodyType || descriptionMeta.bodyType,
    engine_volume: offerParams.engineVolume || descriptionMeta.engineVolume || null,
    city: offerParams.city || String(advertData?.region || product?.city || ""),
    description,
    mileage: offerParams.mileage || descriptionMeta.mileage || null,
    photo_count: Number(product?.photoCount || photoGallery.length || 0) || null,
    phone_count: Number(advertData?.nbPhones || 0) || null,
    phone_prefix: String(advertData?.phonePrefix || ""),
    credit_available: Boolean(product?.isCreditAvailable || advertData?.hasAcrCredit),
    credit_monthly_payment: creditTerms.credit_monthly_payment,
    credit_down_payment: creditTerms.credit_down_payment,
    seller_user_id: String(product?.seller?.userId || advertData?.userId || ""),
    seller_type_id: Number(product?.seller?.userTypeId || 0) || null,
    is_verified_dealer: normalizeBoolean(advertData?.isVerifiedDealer),
    is_used_car_dealer: normalizeBoolean(pageData?.isUsedCarDealer),
    public_history_available: publicHistory.public_history_available,
    history_summary: publicHistory.history_summary,
    last_checked_at: checkedAt
  };

  writeCache(listingSnapshotCache, cacheKey, snapshot, LOW_LOAD_SETTINGS.detailCacheTtlMs);
  return snapshot;
}

function uniqueListings(listings) {
  const seen = new Set();
  return listings.filter(item => {
    const key = getListingStorageKey(item);
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function mergeImportedListingsDetailed(existingRows, importedRows) {
  const existing = normalizeListings(existingRows);
  const imported = normalizeListings(importedRows);
  const existingByKey = new Map(existing.map(item => [getListingStorageKey(item), item]));
  const importedKeys = new Set(imported.map(item => getListingStorageKey(item)));
  const nowIso = new Date().toISOString();
  let newCount = 0;
  let updatedCount = 0;
  let duplicateCount = 0;

  const mergedImported = imported.map(item => {
    const previous = existingByKey.get(getListingStorageKey(item));
    const nextStatus = normalizeActualityStatus(item.actuality_status || "active");
    const previousStatus = normalizeActualityStatus(previous?.actuality_status || "active");

    const merged = {
      ...previous,
      ...item,
      advert_id: item.advert_id || previous?.advert_id || extractAdvertIdFromUrl(item.url),
      first_seen_at: previous?.first_seen_at || item.first_seen_at || nowIso,
      last_seen_at: item.last_seen_at || nowIso,
      last_checked_at: item.last_checked_at || nowIso,
      actuality_status: nextStatus,
      last_status_change_at:
        previousStatus !== nextStatus
          ? nowIso
          : item.last_status_change_at || previous?.last_status_change_at || nowIso
    };

    if (!previous) {
      newCount += 1;
    } else {
      const previousHash = buildListingSnapshotHash(previous);
      const mergedHash = buildListingSnapshotHash(merged);
      if (previousHash !== mergedHash || previousStatus !== nextStatus) {
        updatedCount += 1;
      } else {
        duplicateCount += 1;
      }
    }

    return merged;
  });

  const untouchedExisting = existing.filter(item => !importedKeys.has(getListingStorageKey(item)));
  return {
    items: [...mergedImported, ...untouchedExisting],
    summary: {
      imported_count: imported.length,
      new_count: newCount,
      updated_count: updatedCount,
      duplicate_count: duplicateCount
    }
  };
}

function mergeImportedListings(existingRows, importedRows) {
  return mergeImportedListingsDetailed(existingRows, importedRows).items;
}

function applyImportVisibility(listings, importedRows, { sourceUrl = "" } = {}) {
  const normalized = normalizeListings(listings);
  const importedKeys = new Set(normalizeListings(importedRows).map(item => getListingStorageKey(item)));
  const batchId = `batch:${Date.now()}`;
  const hiddenAt = new Date().toISOString();
  const scopeLabel = String(sourceUrl || "").trim();

  return normalized.map(item => {
    if (importedKeys.has(getListingStorageKey(item))) {
      return {
        ...item,
        hidden_after_import: false,
        hidden_reason: "",
        hidden_at: "",
        import_batch_id: batchId,
        import_scope_label: scopeLabel
      };
    }

    if (String(item.source || "").trim() !== "kolesa.kz") {
      return item;
    }

    return {
      ...item,
      hidden_after_import: true,
      hidden_reason: item.hidden_reason || "not_in_last_import",
      hidden_at: item.hidden_at || hiddenAt
    };
  });
}

function saveListingsWithSnapshots(listings, { capturedAt } = {}) {
  const normalized = normalizeListings(listings);
  const snapshots = mergeListingSnapshots(readListingSnapshots(), normalized, { capturedAt });
  const synced = syncListingArchiveStorage(normalized, readArchivedListings());
  writeListings(synced.activeItems);
  writeListingSnapshots(snapshots);
  return synced.activeItems;
}

function applyKolesaSnapshotToListing(item, snapshot, { checkedAt } = {}) {
  const nextStatus = normalizeActualityStatus(snapshot.actuality_status);
  const previousStatus = normalizeActualityStatus(item.actuality_status);
  const resolvedCheckedAt = checkedAt || snapshot.last_checked_at || new Date().toISOString();

  return {
    ...item,
    title: snapshot.title || item.title,
    advert_id: snapshot.advert_id || item.advert_id || extractAdvertIdFromUrl(item.url),
    price: pickDefined(snapshot.price, item.price),
    mileage: pickDefined(snapshot.mileage, item.mileage),
    publication_date: snapshot.publication_date || item.publication_date,
    last_update: snapshot.last_update || item.last_update,
    description: snapshot.description || item.description,
    image: snapshot.image || item.image,
    photo_gallery: normalizePhotoGallery([...(item.photo_gallery || []), ...(snapshot.photo_gallery || [])]),
    avg_price: pickDefined(snapshot.avg_price, item.avg_price),
    market_difference:
      snapshot.market_difference !== null && snapshot.market_difference !== undefined
        ? snapshot.market_difference
        : item.market_difference,
    market_difference_percent:
      snapshot.market_difference_percent !== null && snapshot.market_difference_percent !== undefined
        ? snapshot.market_difference_percent
        : item.market_difference_percent,
    photo_count: pickDefined(snapshot.photo_count, item.photo_count),
    phone_count: pickDefined(snapshot.phone_count, item.phone_count),
    phone_prefix: snapshot.phone_prefix || item.phone_prefix,
    fuel_type: pickDefined(snapshot.fuel_type, item.fuel_type) || item.fuel_type,
    transmission: pickDefined(snapshot.transmission, item.transmission) || item.transmission,
    body_type: pickDefined(snapshot.body_type, item.body_type) || item.body_type,
    drive_type: pickDefined(snapshot.drive_type, item.drive_type) || item.drive_type,
    steering_side: pickDefined(snapshot.steering_side, item.steering_side) || item.steering_side,
    color: pickDefined(snapshot.color, item.color) || item.color,
    engine_volume: pickDefined(snapshot.engine_volume, item.engine_volume),
    credit_available: snapshot.credit_available !== undefined
      ? normalizeBoolean(snapshot.credit_available)
      : normalizeBoolean(item.credit_available),
    credit_monthly_payment: pickDefined(snapshot.credit_monthly_payment, item.credit_monthly_payment),
    credit_down_payment: pickDefined(snapshot.credit_down_payment, item.credit_down_payment),
    seller_user_id: snapshot.seller_user_id || item.seller_user_id,
    seller_type_id: pickDefined(snapshot.seller_type_id, item.seller_type_id),
    is_verified_dealer: snapshot.is_verified_dealer !== undefined
      ? normalizeBoolean(snapshot.is_verified_dealer)
      : normalizeBoolean(item.is_verified_dealer),
    is_used_car_dealer: snapshot.is_used_car_dealer !== undefined
      ? normalizeBoolean(snapshot.is_used_car_dealer)
      : normalizeBoolean(item.is_used_car_dealer),
    public_history_available: snapshot.public_history_available !== undefined
      ? normalizeBoolean(snapshot.public_history_available)
      : normalizeBoolean(item.public_history_available),
    history_summary: snapshot.history_summary || item.history_summary,
    last_checked_at: resolvedCheckedAt,
    last_seen_at: nextStatus === "active" ? resolvedCheckedAt : item.last_seen_at,
    actuality_status: nextStatus,
    last_status_change_at: previousStatus !== nextStatus ? resolvedCheckedAt : item.last_status_change_at || resolvedCheckedAt
  };
}

function clampImportLimit(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 100;
  }
  return Math.min(Math.max(Math.round(parsed), 1), 1000);
}

function buildKolesaPageUrl(sourceUrl, page) {
  const pageUrl = new URL(sourceUrl);
  if (page <= 1) {
    pageUrl.searchParams.delete("page");
  } else {
    pageUrl.searchParams.set("page", String(page));
  }
  return pageUrl.toString();
}

async function fetchKolesaPage(pageUrl, { force = false, jobId = "" } = {}) {
  const parsedUrl = new URL(pageUrl);
  if (!parsedUrl.hostname.endsWith("kolesa.kz")) {
    throw new Error("unsupported-host");
  }

  const cacheKey = parsedUrl.toString();
  if (!force) {
    const cached = readCache(pageCache, cacheKey);
    if (cached) {
      return cached;
    }
  }

  const remote = await fetchKolesaHtml(parsedUrl.toString(), {
    jobId,
    context: "страница поиска"
  });
  if (!remote.ok) {
    throw new Error(`fetch-failed-${remote.status}`);
  }

  const html = remote.text;
  const $ = cheerio.load(html);
  const checkedAt = new Date().toISOString();
  const listingMeta = new Map();
  extractAssignedJsonObjects(html, "listing.items.push(").forEach(jsonText => {
    try {
      const item = JSON.parse(jsonText);
      const advertId = String(item?.id || extractAdvertIdFromUrl(item?.url));
      if (advertId) {
        listingMeta.set(advertId, item);
      }
    } catch (error) {
      // Ignore malformed listing payloads and keep parsing cards from DOM.
    }
  });
  const items = [];

  $(".a-card.js__a-card").each((_, element) => {
    const card = $(element);
    const titleNode = card.find('[data-test="advert-title"] a').first();
    const title = normalizeWhitespace(titleNode.text());
    const href = titleNode.attr("href") || card.find(".a-card__link").first().attr("href") || "";
    const priceText = normalizeWhitespace(card.find('[data-test="a-card-price"]').first().text());
    const description = normalizeWhitespace(card.find('[data-test="advert-description"]').first().text());
    const image = extractImage(card);
    const alt = normalizeWhitespace(card.find("img").first().attr("alt"));
    const price = parseNumber(priceText);
    const advertId = extractAdvertIdFromUrl(href);
    const meta = advertId ? listingMeta.get(advertId) : null;
    const titleMeta = extractTitleBrandModel(title, meta?.attributes?.brand, meta?.attributes?.model);
    const descriptionMeta = extractDescriptionMeta(description, { title, alt });
    const year = descriptionMeta.year;
    const mileage = descriptionMeta.mileage;
    const engineVolume = descriptionMeta.engineVolume;
    const city = extractCardRegion(card) || extractCityFromAlt(alt);
    const cardPublicationDate = extractCardPublicationDate(card);
    const avgPrice = Number(meta?.attributes?.avgPrice) || 0;
    const unitPrice = Number(meta?.unitPrice) || price;
    const marketDifference = avgPrice && unitPrice ? avgPrice - unitPrice : null;
    const marketDifferencePercent = avgPrice && unitPrice
      ? Number(((marketDifference / avgPrice) * 100).toFixed(2))
      : null;

    if (!title || !price) {
      return;
    }

    items.push({
      title,
      price,
      year,
      mileage,
      owners: 0,
      advert_id: advertId,
      engine_volume: engineVolume,
      city,
      url: href.startsWith("http") ? href : `https://kolesa.kz${href}`,
      image,
      photo_gallery: image ? [image] : [],
      description,
      source: "kolesa.kz",
      brand: titleMeta.brand,
      model: titleMeta.model,
      fuel_type: descriptionMeta.fuelType,
      drive_type: descriptionMeta.driveType,
      steering_side: descriptionMeta.steeringSide,
      color: descriptionMeta.color,
      options: descriptionMeta.options,
      repair_state: descriptionMeta.repairState,
      transmission: descriptionMeta.transmission,
      body_type: descriptionMeta.bodyType,
      publication_date: String(meta?.publicationDate || cardPublicationDate || ""),
      last_update: String(meta?.lastUpdate || ""),
      first_seen_at: checkedAt,
      last_seen_at: checkedAt,
      last_checked_at: checkedAt,
      last_status_change_at: checkedAt,
      actuality_status: "active",
      photo_count: Number(meta?.photoCount || 0) || null,
      credit_available: Boolean(meta?.isCreditAvailable),
      paid_services: normalizeTextList(meta?.appliedPaidServices),
      seller_user_id: String(meta?.seller?.userId || ""),
      seller_type_id: Number(meta?.seller?.userTypeId || 0) || null,
      avg_price: avgPrice || null,
      market_difference: marketDifference,
      market_difference_percent: marketDifferencePercent
    });
  });

  const normalized = normalizeListings(items);
  writeCache(pageCache, cacheKey, normalized, LOW_LOAD_SETTINGS.pageCacheTtlMs);
  return normalized;
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function enrichKolesaListingsWithSnapshots(listings, { maxItems = 12, concurrency = LOW_LOAD_SETTINGS.enrichConcurrency, onProgress = null, jobId = "" } = {}) {
  const targets = listings
    .filter(item => item?.url && item.url.includes("/a/show/"))
    .slice(0, Math.max(0, maxItems));

  if (!targets.length) {
    return {
      items: listings,
      detailFailures: 0,
      detailRequested: 0
    };
  }

  const snapshotMap = new Map();
  let cursor = 0;
  let detailFailures = 0;

  async function worker() {
    while (cursor < targets.length) {
      const index = cursor;
      cursor += 1;
      const item = targets[index];
      try {
        const snapshot = await fetchKolesaListingSnapshot(item.url, { jobId });
        if (snapshot?.actuality_status === "active") {
          snapshotMap.set(item.url, snapshot);
        }
      } catch (error) {
        detailFailures += 1;
        // Keep imported list usable even if some detail pages fail to load.
      }
      if (typeof onProgress === "function") {
        onProgress(index + 1, targets.length);
      }
      if (index < targets.length - 1) {
        await waitRandom(LOW_LOAD_SETTINGS.detailDelayMinMs, LOW_LOAD_SETTINGS.detailDelayMaxMs);
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, targets.length) }, () => worker())
  );

  return {
    items: listings.map(item => {
      const snapshot = snapshotMap.get(item.url);
      if (!snapshot) {
        return item;
      }

      return {
        ...item,
        title: pickDefined(snapshot.title, item.title) || item.title,
        price: pickDefined(snapshot.price, item.price) || item.price,
        city: pickDefined(snapshot.city, item.city) || item.city,
        description: pickDefined(snapshot.description, item.description) || item.description,
        image: pickDefined(snapshot.image, item.image) || item.image,
        photo_gallery: normalizePhotoGallery([...(item.photo_gallery || []), ...(snapshot.photo_gallery || [])]),
        brand: pickDefined(snapshot.brand, item.brand) || item.brand,
        model: pickDefined(snapshot.model, item.model) || item.model,
        fuel_type: pickDefined(snapshot.fuel_type, item.fuel_type) || item.fuel_type,
        drive_type: pickDefined(snapshot.drive_type, item.drive_type) || item.drive_type,
        steering_side: pickDefined(snapshot.steering_side, item.steering_side) || item.steering_side,
        color: pickDefined(snapshot.color, item.color) || item.color,
        options: normalizeTextList([...(item.options || []), ...(snapshot.options || [])]),
        repair_state: pickDefined(snapshot.repair_state, item.repair_state) || item.repair_state,
        transmission: pickDefined(snapshot.transmission, item.transmission) || item.transmission,
        body_type: pickDefined(snapshot.body_type, item.body_type) || item.body_type,
        engine_volume: pickDefined(snapshot.engine_volume, item.engine_volume),
        publication_date: pickDefined(snapshot.publication_date, item.publication_date) || item.publication_date,
        last_update: pickDefined(snapshot.last_update, item.last_update) || item.last_update,
        photo_count: pickDefined(snapshot.photo_count, item.photo_count),
        phone_count: pickDefined(snapshot.phone_count, item.phone_count),
        phone_prefix: pickDefined(snapshot.phone_prefix, item.phone_prefix) || item.phone_prefix,
        credit_available: snapshot.credit_available ?? item.credit_available,
        credit_monthly_payment: pickDefined(snapshot.credit_monthly_payment, item.credit_monthly_payment),
        credit_down_payment: pickDefined(snapshot.credit_down_payment, item.credit_down_payment),
        seller_user_id: pickDefined(snapshot.seller_user_id, item.seller_user_id) || item.seller_user_id,
        seller_type_id: pickDefined(snapshot.seller_type_id, item.seller_type_id),
        is_verified_dealer: snapshot.is_verified_dealer ?? item.is_verified_dealer,
        is_used_car_dealer: snapshot.is_used_car_dealer ?? item.is_used_car_dealer,
        public_history_available: snapshot.public_history_available ?? item.public_history_available,
        history_summary: pickDefined(snapshot.history_summary, item.history_summary) || item.history_summary,
        avg_price: pickDefined(snapshot.avg_price, item.avg_price),
        market_difference: pickDefined(snapshot.market_difference, item.market_difference),
        market_difference_percent: pickDefined(snapshot.market_difference_percent, item.market_difference_percent),
        last_checked_at: pickDefined(snapshot.last_checked_at, item.last_checked_at) || item.last_checked_at
      };
    }),
    detailFailures,
    detailRequested: targets.length
  };
}

async function fetchKolesaListings(sourceUrl, limit = 100, { onProgress = null, jobId = "" } = {}) {
  const safeLimit = clampImportLimit(limit);
  const maxPages = Math.ceil(safeLimit / 20) + 2;
  const pages = [];
  let combined = [];
  const parsedSourceUrl = new URL(sourceUrl);
  const repairState = normalizeRepairState(parsedSourceUrl.searchParams.get("need-repair"));

  for (let page = 1; page <= maxPages && combined.length < safeLimit; page += 1) {
    const pageUrl = buildKolesaPageUrl(sourceUrl, page);
    const pageItems = await fetchKolesaPage(pageUrl, { jobId });
    if (!pageItems.length) {
      break;
    }

    pages.push(pageUrl);
    combined = uniqueListings([...combined, ...pageItems]);
    if (typeof onProgress === "function") {
      onProgress({
        stage: "pages",
        page,
        pagesLoaded: pages.length,
        collected: combined.length,
        limit: safeLimit,
        message: `Страница ${page}, собрано ${combined.length} объявлений`
      });
    }

    if (pageItems.length < 20) {
      break;
    }

    if (combined.length < safeLimit) {
      await waitRandom(LOW_LOAD_SETTINGS.pageDelayMinMs, LOW_LOAD_SETTINGS.pageDelayMaxMs);
    }
  }

  const enrichedResult = await enrichKolesaListingsWithSnapshots(combined.slice(0, safeLimit), {
    maxItems: safeLimit <= 25 ? safeLimit : 8,
    concurrency: LOW_LOAD_SETTINGS.enrichConcurrency,
    jobId,
    onProgress: (completed, total) => {
      if (typeof onProgress === "function") {
        onProgress({
          stage: "details",
          completed,
          total,
          collected: combined.length,
          limit: safeLimit,
          message: `Добираю полные карточки ${completed}/${total}`
        });
      }
    }
  });

  return {
    items: enrichedResult.items.map(item => ({
      ...item,
      repair_state: item.repair_state && item.repair_state !== "unknown" ? item.repair_state : repairState
    })),
    pagesLoaded: pages.length,
    detailFailures: enrichedResult.detailFailures,
    detailRequested: enrichedResult.detailRequested
  };
}

async function fetchKolesaListingsPreview(sourceUrl, limit = 1000) {
  const safeLimit = clampImportLimit(limit);
  const cacheKey = `${String(sourceUrl || "").trim()}::${safeLimit}`;
  const cached = readCache(previewCache, cacheKey);
  if (cached) {
    return cached;
  }

  const firstPageUrl = buildKolesaPageUrl(sourceUrl, 1);
  const firstPageRemote = await fetchKolesaHtml(firstPageUrl, {
    context: "preview количества",
    failFastOnPause: true
  });
  const exactTotalCount = extractKolesaSearchResultCount(firstPageRemote.text);
  if (Number.isFinite(exactTotalCount)) {
    const preview = {
      availableCount: Math.min(exactTotalCount, safeLimit),
      totalCount: exactTotalCount,
      exactTotalKnown: true,
      hasMore: exactTotalCount > safeLimit,
      pagesLoaded: 1
    };
    writeCache(previewCache, cacheKey, preview, LOW_LOAD_SETTINGS.previewCacheTtlMs);
    return preview;
  }

  const previewLimit = Math.min(safeLimit, LOW_LOAD_SETTINGS.previewProbeCap);
  const probeLimit = previewLimit + 1;
  const maxPages = Math.ceil(probeLimit / 20) + 2;
  const pages = [];
  let combined = [];

  for (let page = 1; page <= maxPages && combined.length < probeLimit; page += 1) {
    const pageUrl = buildKolesaPageUrl(sourceUrl, page);
    const pageItems = await fetchKolesaPage(pageUrl);
    if (!pageItems.length) {
      break;
    }

    pages.push(pageUrl);
    combined = uniqueListings([...combined, ...pageItems]);

    if (pageItems.length < 20) {
      break;
    }

    if (combined.length < probeLimit) {
      await waitRandom(LOW_LOAD_SETTINGS.previewDelayMinMs, LOW_LOAD_SETTINGS.previewDelayMaxMs);
    }
  }

  const preview = {
    availableCount: combined.length > previewLimit ? previewLimit : combined.length,
    totalCount: combined.length > previewLimit ? null : combined.length,
    exactTotalKnown: false,
    hasMore: combined.length > previewLimit,
    pagesLoaded: pages.length
  };
  writeCache(previewCache, cacheKey, preview, LOW_LOAD_SETTINGS.previewCacheTtlMs);
  return preview;
}

async function runKolesaImportJob(job) {
  const sourceUrl = String(job.payload?.url || "").trim();
  const save = job.payload?.save !== false;
  const limit = clampImportLimit(job.payload?.limit);

  if (!sourceUrl) {
    throw new Error("Нужна ссылка на поиск или список объявлений Kolesa.");
  }

  job.total = limit;
  job.progress = 0;
  job.message = "Готовим мягкий импорт";

  const result = await fetchKolesaListings(sourceUrl, limit, {
    jobId: job.id,
    onProgress: progress => {
      if (progress?.stage === "pages") {
        job.progress = Math.min(limit, progress.collected || 0);
        job.total = limit;
        job.message = progress.message;
        return;
      }

      if (progress?.stage === "details") {
        const completed = Number(progress.completed) || 0;
        const total = Number(progress.total) || 0;
        const ratio = total > 0 ? completed / total : 0;
        const detailsWeight = Math.min(Math.round(limit * 0.2), total || 0);
        job.progress = Math.min(limit, Math.max(job.progress, (limit - detailsWeight) + Math.round(detailsWeight * ratio)));
        job.total = limit;
        job.message = progress.message;
      }
    }
  });

  const listings = result.items;
  if (!listings.length) {
    throw new Error("Не удалось найти объявления на странице.");
  }

  let importSummary = {
    imported_count: listings.length,
    new_count: listings.length,
    updated_count: 0,
    duplicate_count: 0,
    error_count: Number(result.detailFailures || 0)
  };

  if (save) {
    const mergeResult = mergeImportedListingsDetailed(readListings(), listings);
    const visibleItems = applyImportVisibility(mergeResult.items, listings, { sourceUrl });
    saveListingsWithSnapshots(visibleItems);
    importSummary = {
      ...mergeResult.summary,
      error_count: Number(result.detailFailures || 0)
    };
  }

  job.result = {
    ok: true,
    source: "kolesa.kz",
    sourceUrl,
    limit,
    count: listings.length,
    pagesLoaded: result.pagesLoaded,
    activeCount: listings.filter(item => normalizeActualityStatus(item.actuality_status) === "active").length,
    detailFailures: Number(result.detailFailures || 0),
    detailRequested: Number(result.detailRequested || 0),
    summary: importSummary
  };
}

async function runCollectSnapshotsJob(job) {
  const urls = Array.isArray(job.payload?.urls)
    ? job.payload.urls.map(value => String(value || "").trim()).filter(Boolean)
    : [];
  const maxItems = clampImportLimit(job.payload?.limit || urls.length || 50);
  const concurrency = Math.min(
    Math.max(Number(job.payload?.concurrency) || LOW_LOAD_SETTINGS.collectConcurrency, 1),
    LOW_LOAD_SETTINGS.collectMaxConcurrency
  );
  const listings = readListings();
  const urlSet = urls.length ? new Set(urls) : null;
  const targets = listings
    .filter(item => item.source === "kolesa.kz" && (!urlSet || urlSet.has(item.url)))
    .slice(0, maxItems);

  if (!targets.length) {
    throw new Error("Нет объявлений Kolesa для сбора истории.");
  }

  const updatedByUrl = new Map();
  let checked = 0;
  let failed = 0;

  job.total = targets.length;
  job.progress = 0;
  job.message = "Готовим сбор истории";

  for (let index = 0; index < targets.length; index += concurrency) {
    const batch = targets.slice(index, index + concurrency);
    const results = await Promise.all(batch.map(async item => {
      try {
        const snapshot = await fetchKolesaListingSnapshot(item.url, { jobId: job.id });
        const checkedAt = snapshot.last_checked_at || new Date().toISOString();
        return {
          ok: true,
          url: item.url,
          item: applyKolesaSnapshotToListing(item, snapshot, { checkedAt })
        };
      } catch (error) {
        return {
          ok: false,
          url: item.url
        };
      }
    }));

    results.forEach(result => {
      if (result.ok && result.item) {
        updatedByUrl.set(result.url, result.item);
        checked += 1;
      } else {
        failed += 1;
      }
    });

    job.progress = Math.min(targets.length, checked + failed);
    job.message = `Собираю историю ${job.progress}/${targets.length}`;

    if (index + concurrency < targets.length) {
      await waitRandom(LOW_LOAD_SETTINGS.detailDelayMinMs, LOW_LOAD_SETTINGS.detailDelayMaxMs);
    }
  }

  const nextListings = listings.map(item => updatedByUrl.get(item.url) || item);
  saveListingsWithSnapshots(nextListings);
  job.result = {
    ok: true,
    checked,
    failed,
    requested: targets.length
  };
}

function serveStaticFile(filePath, response) {
  if (!fs.existsSync(filePath)) {
    sendText(response, 404, "Not found");
    return;
  }

  const extension = path.extname(filePath).toLowerCase();
  const mimeType = MIME_TYPES[extension] || "application/octet-stream";
  response.writeHead(200, { "Content-Type": mimeType });
  fs.createReadStream(filePath).pipe(response);
}

async function proxyRemoteImage(imageUrl, response) {
  try {
    const remoteUrl = new URL(imageUrl);
    const remoteResponse = await fetch(remoteUrl.toString(), {
      headers: {
        "User-Agent": "Mozilla/5.0 AutoAnalytics/1.0",
        "Referer": "https://kolesa.kz/",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
      }
    });

    if (!remoteResponse.ok) {
      sendText(response, 404, "Image not found");
      return;
    }

    response.writeHead(200, {
      "Content-Type": remoteResponse.headers.get("content-type") || "image/jpeg",
      "Cache-Control": "public, max-age=3600"
    });

    const arrayBuffer = await remoteResponse.arrayBuffer();
    response.end(Buffer.from(arrayBuffer));
  } catch (error) {
    sendText(response, 400, "Bad image request");
  }
}

const server = http.createServer(async (request, response) => {
  const requestUrl = new URL(request.url, `http://${request.headers.host}`);
  const pathname = decodeURIComponent(requestUrl.pathname);

  if (request.method === "OPTIONS") {
    response.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization"
    });
    response.end();
    return;
  }

  if (pathname === "/api/health" && request.method === "GET") {
    sendJson(response, 200, {
      ok: true,
      port: PORT,
      activeJobId: activeJobId || null,
      queuedJobs: jobQueue.length,
      lowLoad: {
        detailCacheTtlMs: LOW_LOAD_SETTINGS.detailCacheTtlMs,
        pageCacheTtlMs: LOW_LOAD_SETTINGS.pageCacheTtlMs,
        previewCacheTtlMs: LOW_LOAD_SETTINGS.previewCacheTtlMs,
        requestWindowMs: LOW_LOAD_SETTINGS.requestWindowMs,
        maxRequestsPerWindow: LOW_LOAD_SETTINGS.maxRequestsPerWindow,
        protectionPauseMs: LOW_LOAD_SETTINGS.protectionPauseMs,
        collectConcurrency: LOW_LOAD_SETTINGS.collectConcurrency,
        enrichConcurrency: LOW_LOAD_SETTINGS.enrichConcurrency
      },
      remoteGuard: getRemoteGuardStatus()
    });
    return;
  }

  if (pathname === "/api/jobs/import/kolesa" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const sourceUrl = String(payload.url || "").trim();
      const limit = clampImportLimit(payload.limit);
      if (!sourceUrl) {
        sendJson(response, 400, { error: "Нужна ссылка на поиск или список объявлений Kolesa." });
        return;
      }

      const job = createJob("kolesa-import", {
        url: sourceUrl,
        limit,
        save: payload.save !== false
      });
      sendJson(response, 202, { ok: true, job });
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось поставить импорт в очередь.", detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/jobs/collect-snapshots" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const urls = Array.isArray(payload.urls)
        ? payload.urls.map(value => String(value || "").trim()).filter(Boolean)
        : [];
      const job = createJob("collect-snapshots", {
        urls,
        limit: clampImportLimit(payload.limit || urls.length || 50),
        concurrency: Math.min(
          Math.max(Number(payload.concurrency) || LOW_LOAD_SETTINGS.collectConcurrency, 1),
          LOW_LOAD_SETTINGS.collectMaxConcurrency
        )
      });
      sendJson(response, 202, { ok: true, job });
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось поставить сбор истории в очередь.", detail: String(error.message || error) });
    }
    return;
  }

  if (pathname.startsWith("/api/jobs/") && request.method === "GET") {
    const jobId = pathname.split("/").pop();
    const job = getJobSnapshot(jobId);
    if (!job) {
      sendJson(response, 404, { error: "Задача не найдена." });
      return;
    }
    sendJson(response, 200, {
      ok: true,
      job
    });
    return;
  }

  if (pathname === "/api/auth/register" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const username = String(payload.username || "").trim();
      const password = String(payload.password || "");

      if (username.length < 3) {
        sendJson(response, 400, { error: "Логин должен быть не короче 3 символов." });
        return;
      }
      if (password.length < 4) {
        sendJson(response, 400, { error: "Пароль должен быть не короче 4 символов." });
        return;
      }

      const store = readAuthStore();
      const usernameKey = username.toLowerCase();
      if (store.users.some(user => user.usernameKey === usernameKey)) {
        sendJson(response, 400, { error: "Такой логин уже существует." });
        return;
      }

      const credentials = hashPassword(password);
      const user = normalizeUser({
        id: createUserId(username),
        username,
        usernameKey,
        passwordHash: credentials.passwordHash,
        passwordSalt: credentials.passwordSalt,
        activeProfileId: "default",
        profiles: [createDefaultProfile()]
      }, store.users.length);
      store.users.push(user);
      const token = createSession(store, user.id);
      writeAuthStore(store);

      sendJson(response, 200, {
        ok: true,
        token,
        user: sanitizeUser(user),
        state: buildClientStateForUser(user)
      });
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось зарегистрировать пользователя." });
    }
    return;
  }

  if (pathname === "/api/auth/login" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const usernameKey = String(payload.username || "").trim().toLowerCase();
      const password = String(payload.password || "");
      const store = readAuthStore();
      const user = store.users.find(item => item.usernameKey === usernameKey);

      if (!user || !verifyPassword(password, user)) {
        sendJson(response, 401, { error: "Неверный логин или пароль." });
        return;
      }

      const token = createSession(store, user.id);
      writeAuthStore(store);
      sendJson(response, 200, {
        ok: true,
        token,
        user: sanitizeUser(user),
        state: buildClientStateForUser(user)
      });
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось выполнить вход." });
    }
    return;
  }

  if (pathname === "/api/auth/session" && request.method === "GET") {
    const context = requireAuth(request, response);
    if (!context) {
      return;
    }
    sendJson(response, 200, {
      ok: true,
      user: sanitizeUser(context.user),
      state: buildClientStateForUser(context.user)
    });
    return;
  }

  if (pathname === "/api/auth/logout" && request.method === "POST") {
    const context = requireAuth(request, response);
    if (!context) {
      return;
    }
    context.store.sessions = context.store.sessions.filter(session => session.token !== context.token);
    writeAuthStore(context.store);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (pathname === "/api/listings" && request.method === "GET") {
    try {
      const baseListings = readListings();
      const snapshots = ensureListingSnapshotsForListings(baseListings);
      const listings = enrichListingsForClient(baseListings, snapshots);
      sendJson(response, 200, {
        items: listings,
        count: listings.length,
        hiddenCount: listings.filter(item => item.hidden_after_import).length
      });
    } catch (error) {
      sendJson(response, 500, { error: "Не удалось прочитать объявления." });
    }
    return;
  }

  if (pathname === "/api/archive/listings" && request.method === "GET") {
    try {
      const items = enrichListingsForClient(readArchivedListings(), readListingSnapshots());
      sendJson(response, 200, { items, count: items.length });
    } catch (error) {
      sendJson(response, 500, { error: "Не удалось прочитать архив объявлений." });
    }
    return;
  }

  if (pathname === "/api/listing-snapshots" && request.method === "GET") {
    try {
      const targetUrl = String(requestUrl.searchParams.get("url") || "").trim();
      const targetAdvertId = String(requestUrl.searchParams.get("advertId") || "").trim();
      const allSnapshots = readListingSnapshots();

      if (!targetUrl && !targetAdvertId) {
        sendJson(response, 200, { items: allSnapshots, count: allSnapshots.length });
        return;
      }

      const targetKey = targetAdvertId || extractAdvertIdFromUrl(targetUrl) || targetUrl;
      const items = allSnapshots.filter(snapshot => snapshot.listing_id === targetKey || snapshot.listing_id.endsWith(`:${targetKey}`) || snapshot.listing_id === targetUrl);
      sendJson(response, 200, { items, count: items.length });
    } catch (error) {
      sendJson(response, 500, { error: "Не удалось прочитать историю объявления." });
    }
    return;
  }

  if (pathname === "/api/autoparts/catalog" && request.method === "GET") {
    sendJson(response, 200, readAutopartsCatalog());
    return;
  }

  if (pathname === "/api/autoparts/coverage" && request.method === "GET") {
    sendJson(response, 200, getAutopartsCoverage(readListings()));
    return;
  }

  if (pathname === "/api/listings" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const rows = Array.isArray(payload) ? payload : payload.items;
      if (!Array.isArray(rows)) {
        sendJson(response, 400, { error: "Ожидался массив объявлений или объект с items." });
        return;
      }

      const listings = normalizeListings(rows);
      if (!listings.length) {
        sendJson(response, 400, { error: "После проверки не осталось валидных объявлений." });
        return;
      }

      const mergeResult = mergeImportedListingsDetailed(readListings(), listings);
      saveListingsWithSnapshots(mergeResult.items);
      sendJson(response, 200, { ok: true, count: listings.length, summary: mergeResult.summary });
    } catch (error) {
      sendJson(response, 400, { error: "Некорректный JSON." });
    }
    return;
  }

  if (pathname === "/api/state" && request.method === "GET") {
    const context = requireAuth(request, response);
    if (!context) {
      return;
    }
    sendJson(response, 200, buildClientStateForUser(context.user));
    return;
  }

  if (pathname === "/api/state" && request.method === "POST") {
    const context = requireAuth(request, response);
    if (!context) {
      return;
    }
    try {
      const payload = await parseRequestBody(request);
      if (payload.activeProfileId && context.user.profiles.some(profile => profile.id === String(payload.activeProfileId))) {
        context.user.activeProfileId = String(payload.activeProfileId);
      }
      updateActiveUserState(context.user, payload || {});
      writeAuthStore(context.store);
      sendJson(response, 200, { ok: true });
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось сохранить состояние приложения." });
    }
    return;
  }

  if (pathname === "/api/profiles" && request.method === "GET") {
    const context = requireAuth(request, response);
    if (!context) {
      return;
    }
    sendJson(response, 200, {
      activeProfileId: context.user.activeProfileId,
      profiles: context.user.profiles.map(profile => ({
        id: profile.id,
        name: profile.name
      }))
    });
    return;
  }

  if (pathname === "/api/profiles" && request.method === "POST") {
    const context = requireAuth(request, response);
    if (!context) {
      return;
    }
    try {
      const payload = await parseRequestBody(request);
      const name = String(payload.name || "").trim();
      if (!name) {
        sendJson(response, 400, { error: "Нужно имя профиля." });
        return;
      }

      const profile = normalizeProfile({
        id: createProfileId(name),
        name,
        favorites: [],
        comparisonHistory: []
      });
      context.user.profiles = [profile, ...context.user.profiles].slice(0, 20);
      context.user.activeProfileId = profile.id;
      writeAuthStore(context.store);
      sendJson(response, 200, buildClientStateForUser(context.user));
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось создать профиль." });
    }
    return;
  }

  if (pathname === "/api/profiles/active" && request.method === "POST") {
    const context = requireAuth(request, response);
    if (!context) {
      return;
    }
    try {
      const payload = await parseRequestBody(request);
      const profileId = String(payload.profileId || "").trim();
      if (!context.user.profiles.some(profile => profile.id === profileId)) {
        sendJson(response, 404, { error: "Профиль не найден." });
        return;
      }

      context.user.activeProfileId = profileId;
      writeAuthStore(context.store);
      sendJson(response, 200, buildClientStateForUser(context.user));
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось переключить профиль." });
    }
    return;
  }

  if (pathname === "/api/import/kolesa" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const sourceUrl = String(payload.url || "").trim();
      const save = payload.save !== false;
      const limit = clampImportLimit(payload.limit);

      if (!sourceUrl) {
        sendJson(response, 400, { error: "Нужна ссылка на поиск или список объявлений Kolesa." });
        return;
      }

      const result = await fetchKolesaListings(sourceUrl, limit);
      const listings = result.items;
      if (!listings.length) {
        sendJson(response, 404, { error: "Не удалось найти объявления на странице." });
        return;
      }

      let responseItems = enrichListingsForClient(listings, readListingSnapshots());
      let importSummary = {
        imported_count: listings.length,
        new_count: listings.length,
        updated_count: 0,
        duplicate_count: 0,
        error_count: Number(result.detailFailures || 0)
      };
      if (save) {
        const mergeResult = mergeImportedListingsDetailed(readListings(), listings);
        const visibleItems = applyImportVisibility(mergeResult.items, listings, { sourceUrl });
        const savedListings = saveListingsWithSnapshots(visibleItems);
        const savedSnapshots = readListingSnapshots();
        responseItems = enrichListingsForClient(savedListings, savedSnapshots);
        importSummary = {
          ...mergeResult.summary,
          error_count: Number(result.detailFailures || 0)
        };
      }

      sendJson(response, 200, {
        ok: true,
        source: "kolesa.kz",
        sourceUrl,
        limit,
        pagesLoaded: result.pagesLoaded,
        count: listings.length,
        detailFailures: Number(result.detailFailures || 0),
        summary: importSummary,
        items: responseItems
      });
    } catch (error) {
      const message =
        error.message === "unsupported-host"
          ? "Поддерживаются только ссылки вида https://kolesa.kz/..."
          : "Не удалось импортировать объявления с Kolesa.";
      console.error("Kolesa import failed:", error);
      sendJson(response, 400, { error: message, detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/import/kolesa/preview" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const sourceUrl = String(payload.url || "").trim();

      if (!sourceUrl) {
        sendJson(response, 400, { error: "Нужна ссылка на поиск Kolesa." });
        return;
      }

      const preview = await fetchKolesaListingsPreview(sourceUrl, 1000);
      sendJson(response, 200, {
        ok: true,
        source: "kolesa.kz",
        sourceUrl,
        ...preview
      });
    } catch (error) {
      const message =
        error.code === "kolesa-paused"
          ? `Kolesa временно поставила импорт на паузу. Подожди ${Math.max(1, Math.ceil((error.waitMs || 0) / 1000))} сек и попробуй снова.`
          : error.message === "unsupported-host"
          ? "Поддерживаются только ссылки вида https://kolesa.kz/..."
          : "Не удалось получить количество объявлений с Kolesa.";
      console.error("Kolesa preview failed:", error);
      sendJson(response, 400, { error: message, detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/import/kolesa/models" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const mark = String(payload.mark || "").trim().toLowerCase();
      const city = String(payload.city || "").trim().toLowerCase();

      if (!mark) {
        sendJson(response, 200, { ok: true, items: [] });
        return;
      }

      const cacheKey = `${city}::${mark}`;
      const cached = readCache(importModelsCache, cacheKey);
      if (cached) {
        sendJson(response, 200, {
          ok: true,
          source: "kolesa.kz",
          sourceUrl: buildKolesaPageUrl(`https://kolesa.kz/cars/${city ? `${city}/` : ""}${mark}/`, 1),
          items: cached.items || []
        });
        return;
      }

      const sourceUrl = buildKolesaPageUrl(`https://kolesa.kz/cars/${city ? `${city}/` : ""}${mark}/`, 1);
      const remote = await fetchKolesaHtml(sourceUrl, {
        context: "список моделей",
        failFastOnPause: true
      });
      const items = extractKolesaModelOptions(remote.text, mark, city);
      writeCache(importModelsCache, cacheKey, { items }, LOW_LOAD_SETTINGS.previewCacheTtlMs);

      sendJson(response, 200, {
        ok: true,
        source: "kolesa.kz",
        sourceUrl,
        items
      });
    } catch (error) {
      const message =
        error.code === "kolesa-paused"
          ? `Kolesa временно поставила импорт на паузу. Подожди ${Math.max(1, Math.ceil((error.waitMs || 0) / 1000))} сек и попробуй снова.`
          : error.message === "unsupported-host"
          ? "Не удалось получить модели с Kolesa."
          : "Не удалось загрузить модели с Kolesa.";
      sendJson(response, 400, { error: message, detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/listings/collect-snapshots" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const urls = Array.isArray(payload.urls)
        ? payload.urls.map(value => String(value || "").trim()).filter(Boolean)
        : [];
      const maxItems = clampImportLimit(payload.limit || urls.length || 50);
      const concurrency = Math.min(Math.max(Number(payload.concurrency) || 3, 1), 5);
      const listings = readListings();
      const urlSet = urls.length ? new Set(urls) : null;
      const targets = listings
        .filter(item => item.source === "kolesa.kz" && (!urlSet || urlSet.has(item.url)))
        .slice(0, maxItems);

      if (!targets.length) {
        sendJson(response, 400, { error: "Нет объявлений Kolesa для сбора истории." });
        return;
      }

      const updatedByUrl = new Map();
      let checked = 0;
      let failed = 0;

      for (let index = 0; index < targets.length; index += concurrency) {
        const batch = targets.slice(index, index + concurrency);
        const results = await Promise.all(batch.map(async item => {
          try {
            const snapshot = await fetchKolesaListingSnapshot(item.url);
            const checkedAt = snapshot.last_checked_at || new Date().toISOString();
            return {
              ok: true,
              url: item.url,
              item: applyKolesaSnapshotToListing(item, snapshot, { checkedAt })
            };
          } catch (error) {
            return {
              ok: false,
              url: item.url
            };
          }
        }));

        results.forEach(result => {
          if (result.ok && result.item) {
            updatedByUrl.set(result.url, result.item);
            checked += 1;
          } else {
            failed += 1;
          }
        });

        if (index + concurrency < targets.length) {
          await wait(200);
        }
      }

      const nextListings = listings.map(item => updatedByUrl.get(item.url) || item);
      const savedListings = saveListingsWithSnapshots(nextListings);
      const savedSnapshots = readListingSnapshots();
      const enriched = enrichListingsForClient(savedListings, savedSnapshots);

      sendJson(response, 200, {
        ok: true,
        checked,
        failed,
        requested: targets.length,
        items: enriched
      });
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось собрать историю по объявлениям.", detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/listings/check" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const targetUrl = String(payload.url || "").trim();
      const targetAdvertId = String(payload.advertId || extractAdvertIdFromUrl(targetUrl) || "").trim();
      if (!targetUrl && !targetAdvertId) {
        sendJson(response, 400, { error: "Нужен url объявления." });
        return;
      }

      const listings = readListings();
      const current = listings.find(item => listingMatchesTarget(item, { targetUrl, advertId: targetAdvertId }));
      if (!current) {
        sendJson(response, 404, { error: "Объявление не найдено в текущем списке." });
        return;
      }

      if (current.source !== "kolesa.kz") {
        sendJson(response, 400, { error: "Проверка актуальности пока доступна только для Kolesa." });
        return;
      }

      const snapshot = await fetchKolesaListingSnapshot(targetUrl);
      const nextStatus = normalizeActualityStatus(snapshot.actuality_status);
      const checkedAt = snapshot.last_checked_at || new Date().toISOString();
      const updatedListings = listings.map(item => (
        !listingMatchesTarget(item, { targetUrl, advertId: targetAdvertId })
          ? item
          : applyKolesaSnapshotToListing(item, snapshot, { checkedAt, nextStatus })
      ));

      saveListingsWithSnapshots(updatedListings, { capturedAt: checkedAt });
      const refreshedListings = readListings();
      const refreshedSnapshots = readListingSnapshots();
      const updated = enrichListingsForClient(refreshedListings, refreshedSnapshots).find(item => listingMatchesTarget(item, { targetUrl, advertId: targetAdvertId }));
      sendJson(response, 200, { ok: true, item: updated });
    } catch (error) {
      const message =
        error.message === "unsupported-host"
          ? "Поддерживаются только ссылки вида https://kolesa.kz/..."
          : "Не удалось проверить актуальность объявления.";
      sendJson(response, 400, { error: message, detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/listings/vin" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const targetUrl = String(payload.url || "").trim();
      const advertId = String(payload.advertId || extractAdvertIdFromUrl(targetUrl) || "").trim();
      const vin = normalizeVin(payload.vin);
      const vinNote = String(payload.vinNote || "").trim().slice(0, 1000);

      if (!targetUrl && !advertId) {
        sendJson(response, 400, { error: "Нужен url или advertId объявления." });
        return;
      }

      if (vin && !isValidVin(vin)) {
        sendJson(response, 400, { error: "VIN должен содержать 17 символов без I, O и Q." });
        return;
      }

      const listings = readListings();
      let updated = null;
      const updatedListings = listings.map(item => {
        const matches = listingMatchesTarget(item, { targetUrl, advertId });
        if (!matches) {
          return item;
        }

        updated = {
          ...item,
          vin,
          vin_note: vinNote
        };
        return updated;
      });

      if (!updated) {
        sendJson(response, 404, { error: "Объявление не найдено." });
        return;
      }

      writeListings(updatedListings);
      sendJson(response, 200, {
        ok: true,
        item: enrichListingsForClient(readListings()).find(item => item.url === updated.url) || updated
      });
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось сохранить VIN." });
    }
    return;
  }

  if (pathname === "/api/kolesa/price-insight" && request.method === "GET") {
    const advertUrl = requestUrl.searchParams.get("url") || "";
    if (!advertUrl) {
      sendJson(response, 400, { error: "Нужен url объявления." });
      return;
    }

    try {
      const insight = await fetchKolesaPriceInsight(advertUrl);
      sendJson(response, 200, { ok: true, ...insight, url: advertUrl });
    } catch (error) {
      const message =
        error.message === "unsupported-host"
          ? "Поддерживаются только ссылки вида https://kolesa.kz/..."
          : "Не удалось получить аналитику цены по объявлению.";
      sendJson(response, 400, { error: message, detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/listings/gallery" && request.method === "GET") {
    const advertUrl = String(requestUrl.searchParams.get("url") || "").trim();
    if (!advertUrl) {
      sendJson(response, 400, { error: "Нужен url объявления." });
      return;
    }

    try {
      const current = readListings().find(item => item.url === advertUrl);
      if (current && current.source !== "kolesa.kz") {
        const images = normalizePhotoGallery(current.photo_gallery || [current.image]);
        sendJson(response, 200, {
          ok: true,
          url: advertUrl,
          image: current.image || images[0] || "",
          images,
          photoCount: current.photo_count || images.length || null
        });
        return;
      }

      const snapshot = await fetchKolesaListingSnapshot(advertUrl);
      const images = normalizePhotoGallery(snapshot.photo_gallery || [snapshot.image]);
      sendJson(response, 200, {
        ok: true,
        url: advertUrl,
        image: snapshot.image || images[0] || "",
        images,
        photoCount: pickDefined(snapshot.photo_count, current?.photo_count, images.length || null)
      });
    } catch (error) {
      const message =
        error.message === "unsupported-host"
          ? "Поддерживаются только ссылки вида https://kolesa.kz/..."
          : "Не удалось получить фотографии объявления.";
      sendJson(response, 400, { error: message, detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/image" && request.method === "GET") {
    const imageUrl = requestUrl.searchParams.get("url") || "";
    if (!imageUrl) {
      sendText(response, 400, "Missing url");
      return;
    }

    await proxyRemoteImage(imageUrl, response);
    return;
  }

  if (pathname === "/api/translate/languages" && request.method === "GET") {
    try {
      const payload = await runOfflineTranslator("languages");
      sendJson(response, 200, payload);
    } catch (error) {
      sendJson(response, 500, { error: "Не удалось загрузить локальные языки.", detail: String(error.message || error) });
    }
    return;
  }

  if (pathname === "/api/translate" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const sourceCode = String(payload.from || "").trim().toLowerCase();
      const targetCode = String(payload.to || "").trim().toLowerCase();
      const text = String(payload.text || "");

      if (!sourceCode || !targetCode) {
        sendJson(response, 400, { error: "Нужно указать языки перевода." });
        return;
      }

      if (!text.trim()) {
        sendJson(response, 200, { translatedText: "", usedPivotEnglish: false });
        return;
      }

      const result = await runOfflineTranslator("translate", {
        from: sourceCode,
        to: targetCode,
        text
      });
      sendJson(response, 200, result);
    } catch (error) {
      sendJson(response, 400, { error: "Не удалось выполнить локальный перевод.", detail: String(error.message || error) });
    }
    return;
  }

  const safePath = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
  const resolvedPath = path.normalize(path.join(ROOT, safePath));
  if (!resolvedPath.startsWith(ROOT)) {
    sendText(response, 403, "Forbidden");
    return;
  }

  serveStaticFile(resolvedPath, response);
});

server.on("error", async error => {
  if (error.code !== "EADDRINUSE") {
    console.error(error);
    process.exit(1);
  }

  try {
    const response = await fetch(`http://localhost:${PORT}/api/health`);
    if (response.ok) {
      console.log(`Server already running: http://localhost:${PORT}`);
      process.exit(0);
      return;
    }
  } catch (healthError) {
    // Ignore and report the original port collision below.
  }

  console.error(`Port ${PORT} is already in use.`);
  process.exit(1);
});

server.listen(PORT, () => {
  console.log(`Server started: http://localhost:${PORT}`);
});
