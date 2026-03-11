const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { URL } = require("url");
const cheerio = require("cheerio");

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, "data");
const DATA_FILE = path.join(DATA_DIR, "listings.json");
const AUTH_FILE = path.join(DATA_DIR, "auth-store.json");
const SAMPLE_FILE = path.join(ROOT, "sample_listings.json");

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

function readListings() {
  const filePath = fs.existsSync(DATA_FILE) ? DATA_FILE : SAMPLE_FILE;
  const raw = fs.readFileSync(filePath, "utf-8");
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed) ? parsed : parsed.items || [];
}

function writeListings(listings) {
  ensureDataDir();
  fs.writeFileSync(DATA_FILE, JSON.stringify(listings, null, 2), "utf-8");
}

function normalizeListings(rows) {
  return rows
    .map(item => ({
      title: String(item.title || item.name || "Без названия").trim(),
      price: Number(item.price) || 0,
      year: Number(item.year) > 0 ? Number(item.year) : null,
      mileage: Number(item.mileage) > 0 ? Number(item.mileage) : null,
      owners: Number.isFinite(Number(item.owners)) && Number(item.owners) > 0 ? Number(item.owners) : null,
      city: String(item.city || "").trim(),
      url: String(item.url || "").trim(),
      image: String(item.image || "").trim(),
      description: String(item.description || "").trim(),
      source: String(item.source || "").trim(),
      engine_volume: Number(item.engine_volume) > 0 ? Number(item.engine_volume) : null
    }))
    .filter(item => item.title && item.price > 0);
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
  const match = normalizeWhitespace(description).match(/(\d(?:[.,]\d)?)\s*л\b/i);
  return match ? Number(match[1].replace(",", ".")) : 0;
}

function extractImage(card) {
  const src =
    card.find(".thumb-gallery__pic img").first().attr("src") ||
    card.find("img").first().attr("src") ||
    "";
  return src.trim();
}

function uniqueListings(listings) {
  const seen = new Set();
  return listings.filter(item => {
    const key = item.url || `${item.title}|${item.price}|${item.city}`;
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function clampImportLimit(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 100;
  }
  return Math.min(Math.max(Math.round(parsed), 20), 300);
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

async function fetchKolesaPage(pageUrl) {
  const parsedUrl = new URL(pageUrl);
  if (!parsedUrl.hostname.endsWith("kolesa.kz")) {
    throw new Error("unsupported-host");
  }

  const response = await fetch(parsedUrl.toString(), {
    headers: {
      "User-Agent": "Mozilla/5.0 AutoAnalytics/1.0",
      "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"
    }
  });

  if (!response.ok) {
    throw new Error(`fetch-failed-${response.status}`);
  }

  const html = await response.text();
  const $ = cheerio.load(html);
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
    const year = extractYear(description, alt);
    const mileage = extractMileage(description);
    const engineVolume = extractEngineVolume(description);
    const city = extractCityFromAlt(alt);

    if (!title || !price) {
      return;
    }

    items.push({
      title,
      price,
      year,
      mileage,
      owners: 0,
      engine_volume: engineVolume,
      city,
      url: href.startsWith("http") ? href : `https://kolesa.kz${href}`,
      image,
      description,
      source: "kolesa.kz"
    });
  });

  return normalizeListings(items);
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchKolesaListings(sourceUrl, limit = 100) {
  const safeLimit = clampImportLimit(limit);
  const maxPages = Math.ceil(safeLimit / 20) + 2;
  const pages = [];
  let combined = [];

  for (let page = 1; page <= maxPages && combined.length < safeLimit; page += 1) {
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

    if (combined.length < safeLimit) {
      await wait(250);
    }
  }

  return {
    items: combined.slice(0, safeLimit),
    pagesLoaded: pages.length
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
    sendJson(response, 200, { ok: true, port: PORT });
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
      const listings = readListings();
      sendJson(response, 200, { items: listings, count: listings.length });
    } catch (error) {
      sendJson(response, 500, { error: "Не удалось прочитать объявления." });
    }
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

      writeListings(listings);
      sendJson(response, 200, { ok: true, count: listings.length });
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

      if (save) {
        writeListings(listings);
      }

      sendJson(response, 200, {
        ok: true,
        source: "kolesa.kz",
        sourceUrl,
        limit,
        pagesLoaded: result.pagesLoaded,
        count: listings.length,
        items: listings
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

  if (pathname === "/api/image" && request.method === "GET") {
    const imageUrl = requestUrl.searchParams.get("url") || "";
    if (!imageUrl) {
      sendText(response, 400, "Missing url");
      return;
    }

    await proxyRemoteImage(imageUrl, response);
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

server.listen(PORT, () => {
  console.log(`Server started: http://localhost:${PORT}`);
});
