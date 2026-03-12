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
const STALE_AFTER_HOURS = Number(process.env.LISTING_STALE_AFTER_HOURS) > 0
  ? Number(process.env.LISTING_STALE_AFTER_HOURS)
  : 72;
const STALE_AFTER_MS = STALE_AFTER_HOURS * 60 * 60 * 1000;

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
  return normalizeListings(Array.isArray(parsed) ? parsed : parsed.items || []);
}

function writeListings(listings) {
  ensureDataDir();
  fs.writeFileSync(DATA_FILE, JSON.stringify(normalizeListings(listings), null, 2), "utf-8");
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
  if (/левый руль/i.test(normalized)) {
    return "Левый";
  }
  if (/правый руль/i.test(normalized)) {
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

function extractAdvertIdFromUrl(url) {
  const match = String(url || "").match(/\/a\/show\/(\d+)/);
  return match ? String(match[1]) : "";
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

async function fetchKolesaListingSnapshot(advertUrl) {
  const parsedUrl = new URL(advertUrl);
  if (!parsedUrl.hostname.endsWith("kolesa.kz")) {
    throw new Error("unsupported-host");
  }

  const checkedAt = new Date().toISOString();
  const response = await fetch(parsedUrl.toString(), {
    headers: {
      "User-Agent": "Mozilla/5.0 AutoAnalytics/1.0",
      "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"
    }
  });

  if (response.status === 404) {
    return {
      actuality_status: "archived",
      last_checked_at: checkedAt
    };
  }

  if (!response.ok) {
    throw new Error(`fetch-failed-${response.status}`);
  }

  const html = await response.text();
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

  return {
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
    fuel_type: descriptionMeta.fuelType,
    drive_type: descriptionMeta.driveType,
    steering_side: descriptionMeta.steeringSide,
    color: descriptionMeta.color,
    options: descriptionMeta.options,
    repair_state: descriptionMeta.repairState,
    transmission: descriptionMeta.transmission,
    body_type: descriptionMeta.bodyType,
    engine_volume: descriptionMeta.engineVolume || null,
    city: String(advertData?.region || product?.city || ""),
    description,
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

function mergeImportedListings(existingRows, importedRows) {
  const existing = normalizeListings(existingRows);
  const imported = normalizeListings(importedRows);
  const existingByKey = new Map(existing.map(item => [getListingStorageKey(item), item]));
  const nowIso = new Date().toISOString();

  return imported.map(item => {
    const previous = existingByKey.get(getListingStorageKey(item));
    const nextStatus = normalizeActualityStatus(item.actuality_status || "active");
    const previousStatus = normalizeActualityStatus(previous?.actuality_status || "active");

    return {
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

  return normalizeListings(items);
}

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function enrichKolesaListingsWithSnapshots(listings, { maxItems = 12, concurrency = 3 } = {}) {
  const targets = listings
    .filter(item => item?.url && item.url.includes("/a/show/"))
    .slice(0, Math.max(0, maxItems));

  if (!targets.length) {
    return listings;
  }

  const snapshotMap = new Map();
  let cursor = 0;

  async function worker() {
    while (cursor < targets.length) {
      const index = cursor;
      cursor += 1;
      const item = targets[index];
      try {
        const snapshot = await fetchKolesaListingSnapshot(item.url);
        if (snapshot?.actuality_status === "active") {
          snapshotMap.set(item.url, snapshot);
        }
      } catch (error) {
        // Keep imported list usable even if some detail pages fail to load.
      }
      if (index < targets.length - 1) {
        await wait(120);
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, targets.length) }, () => worker())
  );

  return listings.map(item => {
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
  });
}

async function fetchKolesaListings(sourceUrl, limit = 100) {
  const safeLimit = clampImportLimit(limit);
  const maxPages = Math.ceil(safeLimit / 20) + 2;
  const pages = [];
  let combined = [];
  const parsedSourceUrl = new URL(sourceUrl);
  const repairState = normalizeRepairState(parsedSourceUrl.searchParams.get("need-repair"));

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

  const enriched = await enrichKolesaListingsWithSnapshots(combined.slice(0, safeLimit), {
    maxItems: safeLimit <= 40 ? safeLimit : 15,
    concurrency: 3
  });

  return {
    items: enriched.map(item => ({
      ...item,
      repair_state: item.repair_state && item.repair_state !== "unknown" ? item.repair_state : repairState
    })),
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

      writeListings(mergeImportedListings(readListings(), listings));
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
        writeListings(mergeImportedListings(readListings(), listings));
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

  if (pathname === "/api/listings/check" && request.method === "POST") {
    try {
      const payload = await parseRequestBody(request);
      const targetUrl = String(payload.url || "").trim();
      if (!targetUrl) {
        sendJson(response, 400, { error: "Нужен url объявления." });
        return;
      }

      const listings = readListings();
      const current = listings.find(item => item.url === targetUrl);
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
      const updatedListings = listings.map(item => {
        if (item.url !== targetUrl) {
          return item;
        }

        const previousStatus = normalizeActualityStatus(item.actuality_status);
        return {
          ...item,
          title: snapshot.title || item.title,
          advert_id: snapshot.advert_id || item.advert_id || extractAdvertIdFromUrl(item.url),
          price: pickDefined(snapshot.price, item.price),
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
          last_checked_at: checkedAt,
          last_seen_at: nextStatus === "active" ? checkedAt : item.last_seen_at,
          actuality_status: nextStatus,
          last_status_change_at: previousStatus !== nextStatus ? checkedAt : item.last_status_change_at || checkedAt
        };
      });

      writeListings(updatedListings);
      const updated = readListings().find(item => item.url === targetUrl);
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
      const advertId = String(payload.advertId || "").trim();
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
        const matches = (targetUrl && item.url === targetUrl) || (advertId && String(item.advert_id || "") === advertId);
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
      sendJson(response, 200, { ok: true, item: readListings().find(item => item.url === updated.url) || updated });
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
