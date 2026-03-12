const AUTH_TOKEN_KEY = "autoAnalyticsAuthToken";

const defaultListings = [
  {
    title: "Toyota Camry 2018",
    price: 11200000,
    year: 2018,
    mileage: 98000,
    owners: 2,
    city: "Алматы",
    url: "https://example.com/camry-2018",
    description: "Хорошее состояние",
    source: "demo",
    image: ""
  },
  {
    title: "Hyundai Elantra 2020",
    price: 9900000,
    year: 2020,
    mileage: 74000,
    owners: 1,
    city: "Астана",
    url: "https://example.com/elantra-2020",
    description: "Один хозяин",
    source: "demo",
    image: ""
  },
  {
    title: "Kia K5 2021",
    price: 13100000,
    year: 2021,
    mileage: 54000,
    owners: 1,
    city: "Алматы",
    url: "https://example.com/k5-2021",
    description: "Свежий авто",
    source: "demo",
    image: ""
  },
  {
    title: "Volkswagen Passat 2017",
    price: 8700000,
    year: 2017,
    mileage: 126000,
    owners: 3,
    city: "Шымкент",
    url: "https://example.com/passat-2017",
    description: "Есть торг",
    source: "demo",
    image: ""
  }
];

const IMPORT_CITIES = [
  { value: "", label: "Все города" },
  { value: "aktau", label: "Актау" },
  { value: "almaty", label: "Алматы" },
  { value: "astana", label: "Астана" },
  { value: "shymkent", label: "Шымкент" },
  { value: "atyrau", label: "Атырау" },
  { value: "karaganda", label: "Караганда" }
];

const IMPORT_MARKS = [
  { value: "", label: "Все марки" },
  { value: "audi", label: "Audi" },
  { value: "bmw", label: "BMW" },
  { value: "chevrolet", label: "Chevrolet" },
  { value: "daewoo", label: "Daewoo" },
  { value: "honda", label: "Honda" },
  { value: "hyundai", label: "Hyundai" },
  { value: "infiniti", label: "Infiniti" },
  { value: "kia", label: "Kia" },
  { value: "lexus", label: "Lexus" },
  { value: "mazda", label: "Mazda" },
  { value: "mercedes-benz", label: "Mercedes-Benz" },
  { value: "mitsubishi", label: "Mitsubishi" },
  { value: "nissan", label: "Nissan" },
  { value: "opel", label: "Opel" },
  { value: "renault", label: "Renault" },
  { value: "subaru", label: "Subaru" },
  { value: "toyota", label: "Toyota" },
  { value: "volkswagen", label: "Volkswagen" },
  { value: "vaz", label: "ВАЗ (Lada)" },
  { value: "gaz", label: "ГАЗ" },
  { value: "skoda", label: "Skoda" },
  { value: "ford", label: "Ford" },
  { value: "uaz", label: "УАЗ" },
  { value: "changan", label: "Changan" },
  { value: "zaz", label: "ЗАЗ" },
  { value: "chery", label: "Chery" },
  { value: "geely", label: "Geely" },
  { value: "ravon", label: "Ravon" },
  { value: "suzuki", label: "Suzuki" },
  { value: "ssang-yong", label: "SsangYong" },
  { value: "jeep", label: "Jeep" },
  { value: "exeed", label: "EXEED" },
  { value: "byd", label: "BYD" },
  { value: "gac", label: "GAC" },
  { value: "dodge", label: "Dodge" },
  { value: "faw", label: "FAW" },
  { value: "jetour", label: "Jetour" },
  { value: "land-rover", label: "Land Rover" },
  { value: "saab", label: "Saab" },
  { value: "cadillac", label: "Cadillac" }
];

const IMPORT_BODIES = [
  { value: "", label: "Любой кузов" },
  { value: "11", label: "седан" },
  { value: "12", label: "универсал" },
  { value: "13", label: "хэтчбек" },
  { value: "14", label: "купе" },
  { value: "15", label: "кабриолет" },
  { value: "17", label: "микровэн" },
  { value: "18", label: "фургон" },
  { value: "21", label: "внедорожник" },
  { value: "22", label: "пикап" },
  { value: "23", label: "кроссовер" },
  { value: "31", label: "минивэн" },
  { value: "32", label: "микроавтобус" },
  { value: "35", label: "лифтбек" }
];

const IMPORT_TRANSMISSIONS = [
  { value: "", label: "Любая КПП" },
  { value: "1", label: "механика" },
  { value: "2", label: "автомат" },
  { value: "3", label: "типтроник" },
  { value: "4", label: "вариатор" },
  { value: "5", label: "робот" }
];

const state = {
  listings: defaultListings.map(normalizeRow),
  renderedListings: [],
  tableSort: {
    key: "",
    direction: "asc"
  },
  selectedListingId: null,
  modalGalleryIndex: 0,
  compareIds: [],
  compareWinnerId: null,
  favoriteIds: [],
  comparisonHistory: [],
  activeProfileId: "default",
  profiles: [{ id: "default", name: "Основной" }],
  authToken: localStorage.getItem(AUTH_TOKEN_KEY) || "",
  currentUser: null,
  listingInsights: {},
  listingGalleries: {}
};

const elements = {
  authGuestView: document.getElementById("auth-guest-view"),
  authUserView: document.getElementById("auth-user-view"),
  authUsernameInput: document.getElementById("auth-username-input"),
  authPasswordInput: document.getElementById("auth-password-input"),
  loginBtn: document.getElementById("login-btn"),
  registerBtn: document.getElementById("register-btn"),
  logoutBtn: document.getElementById("logout-btn"),
  authUserText: document.getElementById("auth-user-text"),
  profileBox: document.getElementById("profile-box"),
  profileSelect: document.getElementById("profile-select"),
  profileNameInput: document.getElementById("profile-name-input"),
  createProfileBtn: document.getElementById("create-profile-btn"),
  searchInput: document.getElementById("search-input"),
  yearFromInput: document.getElementById("year-from-input"),
  yearToInput: document.getElementById("year-to-input"),
  priceFromInput: document.getElementById("price-from-input"),
  priceToInput: document.getElementById("price-to-input"),
  mileageFromInput: document.getElementById("mileage-from-input"),
  mileageToInput: document.getElementById("mileage-to-input"),
  citySelect: document.getElementById("city-select"),
  markFilterSelect: document.getElementById("mark-filter-select"),
  modelFilterSelect: document.getElementById("model-filter-select"),
  creditFilterSelect: document.getElementById("credit-filter-select"),
  maxMonthlyPaymentInput: document.getElementById("max-monthly-payment-input"),
  repairFilterSelect: document.getElementById("repair-filter-select"),
  fuelFilterSelect: document.getElementById("fuel-filter-select"),
  driveFilterSelect: document.getElementById("drive-filter-select"),
  steeringFilterSelect: document.getElementById("steering-filter-select"),
  colorFilterSelect: document.getElementById("color-filter-select"),
  transmissionFilterSelect: document.getElementById("transmission-filter-select"),
  bodyFilterSelect: document.getElementById("body-filter-select"),
  sellerFilterSelect: document.getElementById("seller-filter-select"),
  optionSearchInput: document.getElementById("option-search-input"),
  sortSelect: document.getElementById("sort-select"),
  showInactiveToggle: document.getElementById("show-inactive-toggle"),
  importCitySelect: document.getElementById("import-city-select"),
  importMarkSelect: document.getElementById("import-mark-select"),
  importBodySelect: document.getElementById("import-body-select"),
  importTransmissionSelect: document.getElementById("import-transmission-select"),
  importCustomSelect: document.getElementById("import-custom-select"),
  importNeedRepairSelect: document.getElementById("import-need-repair-select"),
  importPriceFromInput: document.getElementById("import-price-from-input"),
  importPriceToInput: document.getElementById("import-price-to-input"),
  kolesaUrlInput: document.getElementById("kolesa-url-input"),
  importLimitSelect: document.getElementById("import-limit-select"),
  importKolesaBtn: document.getElementById("import-kolesa-btn"),
  importAktauBtn: document.getElementById("import-aktau-btn"),
  fileInput: document.getElementById("file-input"),
  resetBtn: document.getElementById("reset-btn"),
  totalCount: document.getElementById("total-count"),
  avgPrice: document.getElementById("avg-price"),
  avgYear: document.getElementById("avg-year"),
  avgMileage: document.getElementById("avg-mileage"),
  avgMonthlyPayment: document.getElementById("avg-monthly-payment"),
  compareCount: document.getElementById("compare-count"),
  compareWinnerText: document.getElementById("compare-winner-text"),
  compareChips: document.getElementById("compare-chips"),
  favoritesList: document.getElementById("favorites-list"),
  historyList: document.getElementById("history-list"),
  pickBestCompareBtn: document.getElementById("pick-best-compare-btn"),
  openCompareBtn: document.getElementById("open-compare-btn"),
  clearCompareBtn: document.getElementById("clear-compare-btn"),
  bars: document.getElementById("bars"),
  resultsHead: document.getElementById("results-head"),
  resultsBody: document.getElementById("results-body"),
  resultsCount: document.getElementById("results-count"),
  bulkCheckBtn: document.getElementById("bulk-check-btn"),
  topScoreList: document.getElementById("top-score-list"),
  topDealList: document.getElementById("top-deal-list"),
  topFreshList: document.getElementById("top-fresh-list"),
  topBadList: document.getElementById("top-bad-list"),
  topCreditList: document.getElementById("top-credit-list"),
  bestTitle: document.getElementById("best-title"),
  bestPrice: document.getElementById("best-price"),
  bestScore: document.getElementById("best-score"),
  syncStatus: document.getElementById("sync-status"),
  detailModal: document.getElementById("detail-modal"),
  modalBackdrop: document.getElementById("modal-backdrop"),
  modalCloseBtn: document.getElementById("modal-close-btn"),
  modalMedia: document.getElementById("modal-media"),
  modalSource: document.getElementById("modal-source"),
  modalTitle: document.getElementById("modal-title"),
  modalPrice: document.getElementById("modal-price"),
  modalCity: document.getElementById("modal-city"),
  modalFacts: document.getElementById("modal-facts"),
  modalBreakdown: document.getElementById("modal-breakdown"),
  modalSignals: document.getElementById("modal-signals"),
  modalVinInput: document.getElementById("modal-vin-input"),
  modalVinNoteInput: document.getElementById("modal-vin-note-input"),
  modalVinSaveBtn: document.getElementById("modal-vin-save-btn"),
  modalVinCopyBtn: document.getElementById("modal-vin-copy-btn"),
  modalVinStatus: document.getElementById("modal-vin-status"),
  modalVinEgovLink: document.getElementById("modal-vin-egov-link"),
  modalVinKolesaLink: document.getElementById("modal-vin-kolesa-link"),
  modalDescription: document.getElementById("modal-description"),
  modalLink: document.getElementById("modal-link"),
  modalFavoriteBtn: document.getElementById("modal-favorite-btn"),
  modalCompareBtn: document.getElementById("modal-compare-btn"),
  modalCheckBtn: document.getElementById("modal-check-btn"),
  compareModal: document.getElementById("compare-modal"),
  compareBackdrop: document.getElementById("compare-backdrop"),
  compareCloseBtn: document.getElementById("compare-close-btn"),
  compareGrid: document.getElementById("compare-grid"),
  exportCompareCsvBtn: document.getElementById("export-compare-csv-btn"),
  exportComparePdfBtn: document.getElementById("export-compare-pdf-btn")
};

function normalizeSortValue(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (typeof value === "string") {
    const timestamp = Date.parse(value);
    if (Number.isFinite(timestamp)) {
      return timestamp;
    }
    return value.toLowerCase();
  }

  return value;
}

function getTableSortValue(item, key) {
  switch (key) {
    case "title":
      return item.title || "";
    case "price":
      return item.price;
    case "year":
      return item.year;
    case "publicationDate":
      return getListingDateMeta(item).value;
    case "mileage":
      return item.mileage;
    case "owners":
      return item.owners;
    case "score":
      return item.score;
    default:
      return item[key];
  }
}

function applyTableSort(listings) {
  const { key, direction } = state.tableSort;
  if (!key) {
    return listings;
  }

  const multiplier = direction === "desc" ? -1 : 1;
  listings.sort((left, right) => {
    const leftValue = normalizeSortValue(getTableSortValue(left, key));
    const rightValue = normalizeSortValue(getTableSortValue(right, key));

    if (leftValue === rightValue) {
      return 0;
    }

    if (leftValue === null) {
      return 1;
    }

    if (rightValue === null) {
      return -1;
    }

    if (leftValue > rightValue) {
      return 1 * multiplier;
    }

    if (leftValue < rightValue) {
      return -1 * multiplier;
    }

    return 0;
  });

  return listings;
}

function renderTableSortHeaders() {
  if (!elements.resultsHead) {
    return;
  }

  const buttons = elements.resultsHead.querySelectorAll("[data-sort-key]");
  buttons.forEach(button => {
    const isActive = button.dataset.sortKey === state.tableSort.key;
    const indicator = button.querySelector(".sort-indicator");
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
    if (indicator) {
      indicator.textContent = isActive
        ? (state.tableSort.direction === "asc" ? "↑" : "↓")
        : "-";
    }
  });
}

function createListingId(item) {
  return [
    item.url || "",
    item.title || "",
    item.price || "",
    item.year || "",
    item.city || ""
  ].join("|");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function number(value) {
  if (value === "" || value === null || value === undefined) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function getNormalizedRange(minValue, maxValue) {
  if (minValue !== null && maxValue !== null && minValue > maxValue) {
    return { min: maxValue, max: minValue };
  }

  return { min: minValue, max: maxValue };
}

function optionalPositiveNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function booleanValue(value) {
  return value === true || value === "true" || value === 1 || value === "1";
}

function normalizePhotoGallery(value) {
  const items = Array.isArray(value) ? value : [value];
  const unique = [];
  const seen = new Set();

  items.forEach(item => {
    const normalized = String(item || "").trim();
    if (!normalized || seen.has(normalized)) {
      return;
    }

    seen.add(normalized);
    unique.push(normalized);
  });

  return unique.slice(0, 30);
}

function normalizeVin(value) {
  return String(value || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function isValidVin(value) {
  const vin = normalizeVin(value);
  return vin.length === 17 && !/[IOQ]/.test(vin);
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

function normalizeRow(item) {
  const photoGallery = normalizePhotoGallery(item.photo_gallery ?? item.photoGallery ?? []);
  const image = String(item.image || photoGallery[0] || "").trim();
  const normalizedGallery = photoGallery.length ? photoGallery : (image ? [image] : []);

  return {
    id: item.id || createListingId(item),
    title: item.title || item.name || "Без названия",
    price: number(item.price) || 0,
    year: optionalPositiveNumber(item.year),
    mileage: optionalPositiveNumber(item.mileage),
    owners: optionalPositiveNumber(item.owners),
    city: item.city || "",
    url: item.url || "",
    image,
    photoGallery: normalizedGallery,
    description: item.description || "",
    source: item.source || "",
    brand: item.brand || "",
    model: item.model || "",
    fuelType: item.fuel_type || item.fuelType || "",
    transmission: item.transmission || "",
    bodyType: item.body_type || item.bodyType || "",
    driveType: item.drive_type || item.driveType || "",
    steeringSide: item.steering_side || item.steeringSide || "",
    color: item.color || "",
    options: Array.isArray(item.options) ? item.options.map(value => String(value || "").trim()).filter(Boolean) : [],
    vin: normalizeVin(item.vin),
    vinNote: item.vin_note || item.vinNote || "",
    repairState: normalizeRepairState(item.repair_state ?? item.repairState),
    advertId: item.advert_id || item.advertId || "",
    engineVolume: optionalPositiveNumber(item.engine_volume ?? item.engineVolume),
    publicationDate: item.publication_date || item.publicationDate || "",
    lastUpdate: item.last_update || item.lastUpdate || "",
    firstSeenAt: item.first_seen_at || item.firstSeenAt || "",
    lastSeenAt: item.last_seen_at || item.lastSeenAt || "",
    lastCheckedAt: item.last_checked_at || item.lastCheckedAt || "",
    lastStatusChangeAt: item.last_status_change_at || item.lastStatusChangeAt || "",
    actualityStatus: normalizeActualityStatus(item.actuality_status || item.actualityStatus),
    photoCount: optionalPositiveNumber(item.photo_count ?? item.photoCount) || optionalPositiveNumber(normalizedGallery.length),
    phoneCount: optionalPositiveNumber(item.phone_count ?? item.phoneCount),
    phonePrefix: item.phone_prefix || item.phonePrefix || "",
    creditAvailable: booleanValue(item.credit_available ?? item.creditAvailable),
    paidServices: Array.isArray(item.paid_services ?? item.paidServices)
      ? (item.paid_services ?? item.paidServices).map(value => String(value || "").trim()).filter(Boolean)
      : [],
    creditMonthlyPayment: optionalPositiveNumber(item.credit_monthly_payment ?? item.creditMonthlyPayment),
    creditDownPayment: optionalPositiveNumber(item.credit_down_payment ?? item.creditDownPayment),
    sellerUserId: item.seller_user_id || item.sellerUserId || "",
    sellerTypeId: optionalPositiveNumber(item.seller_type_id ?? item.sellerTypeId),
    isVerifiedDealer: booleanValue(item.is_verified_dealer ?? item.isVerifiedDealer),
    isUsedCarDealer: booleanValue(item.is_used_car_dealer ?? item.isUsedCarDealer),
    publicHistoryAvailable: booleanValue(item.public_history_available ?? item.publicHistoryAvailable),
    historySummary: item.history_summary || item.historySummary || "",
    riskScore: number(item.risk_score ?? item.riskScore),
    riskFlags: Array.isArray(item.risk_flags ?? item.riskFlags)
      ? (item.risk_flags ?? item.riskFlags).map(value => String(value || "").trim()).filter(Boolean)
      : [],
    avgPrice: optionalPositiveNumber(item.avg_price ?? item.avgPrice),
    marketDifference: number(item.market_difference ?? item.marketDifference),
    marketDifferencePercent: number(item.market_difference_percent ?? item.marketDifferencePercent)
  };
}

function normalizeRows(rows) {
  return rows.map(normalizeRow).filter(item => item.price > 0);
}

function formatPrice(value) {
  return new Intl.NumberFormat("ru-RU").format(Math.round(value)) + " ₸";
}

function formatInteger(value) {
  return new Intl.NumberFormat("ru-RU").format(Math.round(Number(value || 0)));
}

function formatMileage(value) {
  return new Intl.NumberFormat("ru-RU").format(Math.round(value)) + " км";
}

function formatScore(value) {
  return Number(value || 0).toFixed(2);
}

function formatScorePercent(value) {
  return `${Math.round(Number(value || 0) * 100)}%`;
}

function formatPercent(value) {
  return `${Math.abs(Number(value || 0)).toFixed(1)}%`;
}

function formatYesNo(value) {
  return value ? "Да" : "Нет";
}

function daysSince(value) {
  if (!value) {
    return null;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return Math.max(0, (Date.now() - date.getTime()) / (24 * 60 * 60 * 1000));
}

function normalizeActualityStatus(value) {
  const status = String(value || "").trim().toLowerCase();
  return ["active", "stale", "archived", "unavailable"].includes(status) ? status : "active";
}

function formatShortDate(value) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const target = new Date(date.getFullYear(), date.getMonth(), date.getDate());
  const diffDays = Math.round((today.getTime() - target.getTime()) / (24 * 60 * 60 * 1000));

  if (diffDays === 0) {
    return "сегодня";
  }

  if (diffDays === 1) {
    return "вчера";
  }

  return new Intl.DateTimeFormat("ru-RU", {
    day: "numeric",
    month: "short"
  }).format(date);
}

function formatTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "";
  }

  return new Intl.DateTimeFormat("ru-RU", {
    hour: "2-digit",
    minute: "2-digit"
  }).format(date);
}

function getListingDateMeta(item) {
  const candidates = [
    { value: item?.publicationDate, shortLabel: "Публ.", fullLabel: "Публикация" },
    { value: item?.lastUpdate, shortLabel: "Обновл.", fullLabel: "Обновлено" },
    { value: item?.firstSeenAt || item?.lastSeenAt, shortLabel: "Замеч.", fullLabel: "Замечено" },
    { value: item?.lastCheckedAt, shortLabel: "Провер.", fullLabel: "Проверено" }
  ];

  for (const candidate of candidates) {
    if (!candidate.value) {
      continue;
    }

    const date = new Date(candidate.value);
    if (!Number.isNaN(date.getTime())) {
      return candidate;
    }
  }

  return {
    value: "",
    shortLabel: "",
    fullLabel: ""
  };
}

function formatRelativeListingDate(item) {
  const rawValue = getListingDateMeta(item).value;
  if (!rawValue) {
    return "-";
  }

  const date = new Date(rawValue);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const target = new Date(date.getFullYear(), date.getMonth(), date.getDate());
  const diffDays = Math.round((today.getTime() - target.getTime()) / (24 * 60 * 60 * 1000));

  if (diffDays === 0) {
    const time = formatTime(rawValue);
    return time ? `сегодня ${time}` : "сегодня";
  }

  if (diffDays === 1) {
    return "вчера";
  }

  if (diffDays > 1 && diffDays <= 7) {
    return `${diffDays} дн. назад`;
  }

  return formatShortDate(rawValue);
}

function formatListingDateBadge(item) {
  const meta = getListingDateMeta(item);
  if (!meta.value) {
    return "-";
  }

  return `${meta.shortLabel} ${formatRelativeListingDate(item)}`;
}

function formatListingDateDetailed(item) {
  const meta = getListingDateMeta(item);
  if (!meta.value) {
    return "-";
  }

  return `${meta.fullLabel}: ${formatDateTime(meta.value)}`;
}

function formatDateTime(value) {
  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return new Intl.DateTimeFormat("ru-RU", {
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit"
  }).format(date);
}

function getActualityMeta(item) {
  switch (normalizeActualityStatus(item?.actualityStatus)) {
    case "stale":
      return { label: "Не подтверждено", className: "status-badge status-badge--stale" };
    case "archived":
      return { label: "В архиве", className: "status-badge status-badge--archived" };
    case "unavailable":
      return { label: "Недоступно", className: "status-badge status-badge--archived" };
    default:
      return { label: "Активно", className: "status-badge status-badge--active" };
  }
}

function isListingActual(item) {
  return normalizeActualityStatus(item?.actualityStatus) === "active";
}

function isKolesaListing(item) {
  return Boolean(item?.url && item?.source === "kolesa.kz" && item.url.includes("kolesa.kz"));
}

function toProxiedImageUrl(imageUrl) {
  return imageUrl ? `/api/image?url=${encodeURIComponent(imageUrl)}` : "";
}

function getListingGalleryState(item) {
  const cached = item?.url ? state.listingGalleries[item.url] : null;
  const cachedImages = normalizePhotoGallery(cached?.images || []);
  const itemImages = normalizePhotoGallery(item?.photoGallery || (item?.image ? [item.image] : []));

  return {
    status: cached?.status || "",
    images: cachedImages.length ? cachedImages : itemImages
  };
}

function shouldAutoCheckActuality(item) {
  if (!isKolesaListing(item)) {
    return false;
  }

  if (!hasDetailEnrichment(item)) {
    return true;
  }

  if (!item.lastCheckedAt) {
    return true;
  }

  const checkedAt = new Date(item.lastCheckedAt);
  if (Number.isNaN(checkedAt.getTime())) {
    return true;
  }

  const twelveHours = 12 * 60 * 60 * 1000;
  return !isListingActual(item) || (Date.now() - checkedAt.getTime() > twelveHours);
}

function renderActualityBadge(item) {
  const meta = getActualityMeta(item);
  return `<span class="${meta.className}">${meta.label}</span>`;
}

function getPromotionLabel(item) {
  const services = item.paidServices || [];
  if (!services.length) {
    return "";
  }

  if (services.some(service => /vip/i.test(service))) {
    return "VIP";
  }

  if (services.some(service => /top/i.test(service))) {
    return "TOP";
  }

  return services[0];
}

function getFreshnessMeta(item) {
  const age = daysSince(item.publicationDate || item.lastUpdate || item.lastCheckedAt);
  if (age === null) {
    return { label: "Без даты", className: "status-badge" };
  }

  if (age <= 2) {
    return { label: "Свежее", className: "status-badge status-badge--fresh" };
  }

  if (age >= 21) {
    return { label: "Давно висит", className: "status-badge status-badge--stale" };
  }

  return { label: `~${Math.round(age)} дн.`, className: "status-badge" };
}

function getMarketBadge(item) {
  if (!Number.isFinite(item.marketDifferencePercent)) {
    return "";
  }

  if (item.marketDifferencePercent >= 12) {
    return "Недооценено";
  }

  if (item.marketDifferencePercent <= -12) {
    return "Дорого";
  }

  return "";
}

function getRepairStateLabel(item) {
  switch (normalizeRepairState(item?.repairState)) {
    case "yes":
      return "Не на ходу";
    case "no":
      return "Не аварийная";
    default:
      return "";
  }
}

function getSellerFilterType(item) {
  return item.isUsedCarDealer || item.isVerifiedDealer || Number(item.sellerTypeId) === 2
    ? "dealer"
    : "private";
}

function renderListingBadges(item) {
  const badges = [renderActualityBadge(item)];
  const promotion = getPromotionLabel(item);
  const freshness = getFreshnessMeta(item);
  const market = getMarketBadge(item);

  if (promotion) {
    badges.push(`<span class="status-badge status-badge--promotion">${escapeHtml(promotion)}</span>`);
  }

  if (item.creditAvailable) {
    badges.push(`<span class="status-badge status-badge--credit">Кредит</span>`);
  }

  if (item.photoCount) {
    badges.push(`<span class="status-badge">Фото ${item.photoCount}</span>`);
  }

  if (market) {
    badges.push(`<span class="status-badge status-badge--deal">${escapeHtml(market)}</span>`);
  }

  const repairState = getRepairStateLabel(item);
  if (repairState) {
    badges.push(`<span class="status-badge status-badge--repair">${escapeHtml(repairState)}</span>`);
  }

  if (freshness.label) {
    badges.push(`<span class="${freshness.className}">${escapeHtml(freshness.label)}</span>`);
  }

  return badges.join("");
}

function hasDetailEnrichment(item) {
  const detailFields = [
    item.publicationDate,
    item.lastUpdate,
    item.brand,
    item.model,
    item.fuelType,
    item.transmission,
    item.bodyType,
    item.driveType,
    item.color,
    item.engineVolume,
    item.phoneCount,
    item.photoCount,
    item.avgPrice
  ];

  return detailFields.filter(value => value !== null && value !== undefined && value !== "").length >= 5;
}

function getSellerLabel(item) {
  if (item.isVerifiedDealer || item.isUsedCarDealer) {
    return "Дилер";
  }

  if (item.sellerUserId || item.sellerTypeId) {
    return "Продавец";
  }

  return "-";
}

function setStatus(text) {
  elements.syncStatus.textContent = text;
}

function wait(ms) {
  return new Promise(resolve => window.setTimeout(resolve, ms));
}

function setImportBusy(isBusy) {
  elements.importKolesaBtn.disabled = isBusy;
  elements.importAktauBtn.disabled = isBusy;
  elements.importLimitSelect.disabled = isBusy;
  elements.kolesaUrlInput.disabled = isBusy;
  elements.importCitySelect.disabled = isBusy;
  elements.importMarkSelect.disabled = isBusy;
  elements.importBodySelect.disabled = isBusy;
  elements.importTransmissionSelect.disabled = isBusy;
  elements.importCustomSelect.disabled = isBusy;
  elements.importNeedRepairSelect.disabled = isBusy;
  elements.importPriceFromInput.disabled = isBusy;
  elements.importPriceToInput.disabled = isBusy;
  elements.importKolesaBtn.textContent = isBusy ? "Загрузка..." : "Импортировать";
}

function getImportLimit() {
  const value = Number(elements.importLimitSelect.value);
  if (!Number.isFinite(value) || value <= 0) {
    return 100;
  }
  return Math.min(Math.max(Math.round(value), 20), 300);
}

function fillSelectOptions(element, options, selectedValue = "") {
  element.innerHTML = "";
  options.forEach(optionData => {
    const option = document.createElement("option");
    option.value = optionData.value;
    option.textContent = optionData.label;
    element.append(option);
  });
  element.value = selectedValue;
}

function populateImportFilters() {
  fillSelectOptions(elements.importCitySelect, IMPORT_CITIES, "");
  fillSelectOptions(elements.importMarkSelect, IMPORT_MARKS, "");
  fillSelectOptions(elements.importBodySelect, IMPORT_BODIES, "");
  fillSelectOptions(elements.importTransmissionSelect, IMPORT_TRANSMISSIONS, "");
}

function syncImportUrlPreview() {
  if (elements.kolesaUrlInput.dataset.manual === "true") {
    return;
  }
  elements.kolesaUrlInput.value = buildImportUrlFromFilters();
}

function buildImportUrlFromFilters() {
  const city = elements.importCitySelect.value;
  const mark = elements.importMarkSelect.value;
  const body = elements.importBodySelect.value;
  const transmission = elements.importTransmissionSelect.value;
  const custom = elements.importCustomSelect.value;
  const needRepair = elements.importNeedRepairSelect.value;
  const priceFrom = number(elements.importPriceFromInput.value);
  const priceTo = number(elements.importPriceToInput.value);

  let url = "https://kolesa.kz/cars/";
  if (city) {
    url += `${city}/`;
  }
  if (mark) {
    url += `${mark}/`;
  }

  const params = new URLSearchParams();
  if (body) {
    params.set("auto-car-body", body);
  }
  if (transmission) {
    params.set("auto-car-transm", transmission);
  }
  if (custom) {
    params.set("auto-custom", custom);
  }
  if (needRepair) {
    params.set("need-repair", needRepair);
  }
  if (priceFrom) {
    params.set("price[from]", String(priceFrom));
  }
  if (priceTo) {
    params.set("price[to]", String(priceTo));
  }

  const query = params.toString();
  return query ? `${url}?${query}` : url;
}

function isAuthenticated() {
  return Boolean(state.authToken && state.currentUser);
}

function setAuthToken(token) {
  state.authToken = token || "";
  if (state.authToken) {
    localStorage.setItem(AUTH_TOKEN_KEY, state.authToken);
  } else {
    localStorage.removeItem(AUTH_TOKEN_KEY);
  }
}

function resetUserState() {
  state.currentUser = null;
  state.favoriteIds = [];
  state.comparisonHistory = [];
  state.activeProfileId = "default";
  state.profiles = [{ id: "default", name: "Основной" }];
  state.compareIds = [];
  state.compareWinnerId = null;
}

function applyServerState(payload) {
  state.activeProfileId = String(payload.activeProfileId || "default");
  state.profiles = Array.isArray(payload.profiles) && payload.profiles.length
    ? payload.profiles
    : [{ id: "default", name: "Основной" }];
  state.favoriteIds = Array.isArray(payload.favorites) ? payload.favorites.map(String) : [];
  state.comparisonHistory = Array.isArray(payload.comparisonHistory) ? payload.comparisonHistory : [];
}

async function apiFetch(url, options = {}, authRequired = false) {
  const headers = {
    ...(options.headers || {})
  };

  if (state.authToken) {
    headers.Authorization = `Bearer ${state.authToken}`;
  }

  const response = await fetch(url, {
    ...options,
    headers
  });

  if (response.status === 401 && authRequired) {
    setAuthToken("");
    resetUserState();
  }

  return response;
}

function renderAuth() {
  const loggedIn = isAuthenticated();
  elements.authGuestView.hidden = loggedIn;
  elements.authUserView.hidden = !loggedIn;
  elements.profileBox.hidden = !loggedIn;
  elements.authUserText.textContent = loggedIn
    ? `Вход выполнен: ${state.currentUser.username}`
    : "Вход не выполнен";
}

async function handleAuthSuccess(payload) {
  setAuthToken(payload.token || state.authToken);
  state.currentUser = payload.user || null;
  applyServerState(payload.state || {});
  state.compareIds = [];
  state.compareWinnerId = null;
  elements.authPasswordInput.value = "";
  render();
}

async function registerUser() {
  const username = elements.authUsernameInput.value.trim();
  const password = elements.authPasswordInput.value;
  if (!username || !password) {
    window.alert("Заполни логин и пароль.");
    return;
  }

  try {
    const response = await apiFetch("/api/auth/register", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ username, password })
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Register failed");
    }
    await handleAuthSuccess(payload);
  } catch (error) {
    window.alert(error.message || "Не удалось зарегистрироваться.");
  }
}

async function loginUser() {
  const username = elements.authUsernameInput.value.trim();
  const password = elements.authPasswordInput.value;
  if (!username || !password) {
    window.alert("Заполни логин и пароль.");
    return;
  }

  try {
    const response = await apiFetch("/api/auth/login", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ username, password })
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Login failed");
    }
    await handleAuthSuccess(payload);
  } catch (error) {
    window.alert(error.message || "Не удалось войти.");
  }
}

async function logoutUser() {
  try {
    await apiFetch("/api/auth/logout", { method: "POST" }, true);
  } catch (error) {
    // Ignore transport failures during logout.
  }

  setAuthToken("");
  resetUserState();
  render();
}

async function loadAuthSession() {
  if (!state.authToken) {
    resetUserState();
    return;
  }

  try {
    const response = await apiFetch("/api/auth/session", { method: "GET" }, true);
    if (!response.ok) {
      throw new Error("Session load failed");
    }
    const payload = await response.json();
    state.currentUser = payload.user || null;
    applyServerState(payload.state || {});
  } catch (error) {
    setAuthToken("");
    resetUserState();
  }
}

function normalize(value, min, max, invert = false) {
  if (
    value === null ||
    value === undefined ||
    min === null ||
    min === undefined ||
    max === null ||
    max === undefined
  ) {
    return 0.5;
  }
  if (max === min) {
    return 1;
  }
  const result = (value - min) / (max - min);
  return invert ? 1 - result : result;
}

function scoreListings(listings) {
  const prices = listings.map(item => item.price).filter(Boolean);
  const years = listings.map(item => item.year).filter(item => item !== null && item !== undefined);
  const mileages = listings.map(item => item.mileage).filter(item => item !== null && item !== undefined);
  const owners = listings.map(item => item.owners).filter(item => item !== null && item !== undefined);
  const photoCounts = listings.map(item => item.photoCount).filter(item => item !== null && item !== undefined);
  const risks = listings.map(item => item.riskScore).filter(item => item !== null && item !== undefined);
  const monthlyPayments = listings.map(item => item.creditMonthlyPayment).filter(item => item !== null && item !== undefined);
  const freshnessValues = listings
    .map(item => {
      const age = daysSince(getListingDateMeta(item).value);
      return age === null ? null : Math.min(age, 60);
    })
    .filter(item => item !== null && item !== undefined);

  const bounds = {
    priceMin: Math.min(...prices),
    priceMax: Math.max(...prices),
    yearMin: years.length ? Math.min(...years) : null,
    yearMax: years.length ? Math.max(...years) : null,
    mileageMin: mileages.length ? Math.min(...mileages) : null,
    mileageMax: mileages.length ? Math.max(...mileages) : null,
    ownersMin: owners.length ? Math.min(...owners) : null,
    ownersMax: owners.length ? Math.max(...owners) : null,
    photoMin: photoCounts.length ? Math.min(...photoCounts) : null,
    photoMax: photoCounts.length ? Math.max(...photoCounts) : null,
    riskMin: risks.length ? Math.min(...risks) : null,
    riskMax: risks.length ? Math.max(...risks) : null,
    monthlyPaymentMin: monthlyPayments.length ? Math.min(...monthlyPayments) : 0,
    monthlyPaymentMax: monthlyPayments.length ? Math.max(...monthlyPayments) : 1,
    freshMin: freshnessValues.length ? Math.min(...freshnessValues) : null,
    freshMax: freshnessValues.length ? Math.max(...freshnessValues) : null
  };

  return listings.map(item => {
    const priceScore = normalize(item.price, bounds.priceMin, bounds.priceMax, true);
    const yearScore = normalize(item.year, bounds.yearMin, bounds.yearMax);
    const mileageScore = normalize(item.mileage, bounds.mileageMin, bounds.mileageMax, true);
    const ownersScore = normalize(item.owners, bounds.ownersMin, bounds.ownersMax, true);
    const freshAge = daysSince(getListingDateMeta(item).value);
    const freshnessScore = normalize(freshAge === null ? null : Math.min(freshAge, 60), bounds.freshMin, bounds.freshMax, true);
    const photoScore = normalize(item.photoCount, bounds.photoMin, bounds.photoMax);
    const riskSafetyScore = normalize(item.riskScore, bounds.riskMin, bounds.riskMax, true);
    const marketScore = Number.isFinite(item.marketDifferencePercent)
      ? Math.max(0, Math.min(1, (item.marketDifferencePercent + 15) / 30))
      : 0.5;
    const sellerPenalty = item.isUsedCarDealer ? 0.2 : 0;
    const resellerScore = Math.max(
      0,
      Math.min(
        1,
        (item.isUsedCarDealer ? 0.55 : 0.15) +
        (Number(item.photoCount || 0) >= 12 ? 0.1 : 0) +
        ((item.paidServices || []).length >= 1 ? 0.15 : 0) +
        ((item.phoneCount || 0) >= 2 ? 0.15 : 0) +
        ((item.description || "").length < 80 ? 0.1 : 0)
      )
    );
    const sellerTrustScore = Math.max(0, 1 - resellerScore);
    const qualityScore =
      yearScore * 0.22 +
      mileageScore * 0.2 +
      ownersScore * 0.08 +
      freshnessScore * 0.12 +
      photoScore * 0.08 +
      riskSafetyScore * 0.22 +
      sellerTrustScore * 0.08;
    const valueScore =
      priceScore * 0.48 +
      marketScore * 0.34 +
      freshnessScore * 0.08 +
      sellerTrustScore * 0.1;
    const score =
      valueScore * 0.56 +
      qualityScore * 0.44 -
      sellerPenalty * 0.35;
    const dealScore =
      priceScore * 0.40 +
      marketScore * 0.35 +
      freshnessScore * 0.1 +
      yearScore * 0.1 +
      mileageScore * 0.05;
    const liquidityScore =
      marketScore * 0.3 +
      freshnessScore * 0.25 +
      photoScore * 0.1 +
      yearScore * 0.15 +
      mileageScore * 0.1 +
      riskSafetyScore * 0.1;
    const creditScore = item.creditAvailable && item.creditMonthlyPayment
      ? (
          normalize(item.creditMonthlyPayment, bounds.monthlyPaymentMin, bounds.monthlyPaymentMax, true) * 0.55 +
          marketScore * 0.2 +
          freshnessScore * 0.1 +
          riskSafetyScore * 0.1 +
          yearScore * 0.05
        )
      : 0;
    const badScore =
      (item.riskScore !== null ? item.riskScore / 100 : 0.35) * 0.45 +
      resellerScore * 0.2 +
      (photoScore ? 1 - photoScore : 0.4) * 0.15 +
      (Number.isFinite(item.marketDifferencePercent) && item.marketDifferencePercent < 0
        ? Math.min(Math.abs(item.marketDifferencePercent) / 20, 1)
        : 0) * 0.2;

    return {
      ...item,
      score: Number(Math.max(0, Math.min(1, score)).toFixed(4)),
      qualityScore: Number(qualityScore.toFixed(4)),
      valueScore: Number(valueScore.toFixed(4)),
      dealScore: Number(dealScore.toFixed(4)),
      liquidityScore: Number(liquidityScore.toFixed(4)),
      creditScore: Number(creditScore.toFixed(4)),
      freshnessScore: Number(freshnessScore.toFixed(4)),
      resellerScore: Number(resellerScore.toFixed(4)),
      badScore: Number(badScore.toFixed(4)),
      scoreParts: {
        price: Number(priceScore.toFixed(4)),
        quality: Number(qualityScore.toFixed(4)),
        year: Number(yearScore.toFixed(4)),
        mileage: Number(mileageScore.toFixed(4)),
        owners: Number(ownersScore.toFixed(4)),
        market: Number(marketScore.toFixed(4)),
        freshness: Number(freshnessScore.toFixed(4)),
        risk: Number(riskSafetyScore.toFixed(4)),
        photos: Number(photoScore.toFixed(4)),
        seller: Number(sellerTrustScore.toFixed(4))
      }
    };
  });
}

function populateCities() {
  const current = elements.citySelect.value;
  const cities = [...new Set(state.listings.map(item => item.city).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  elements.citySelect.innerHTML = '<option value="">Все города</option>';

  cities.forEach(city => {
    const option = document.createElement("option");
    option.value = city;
    option.textContent = city;
    elements.citySelect.append(option);
  });

  elements.citySelect.value = cities.includes(current) ? current : "";
}

function populateMarks() {
  const current = elements.markFilterSelect.value;
  const marks = [...new Set(state.listings.map(item => item.brand).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  elements.markFilterSelect.innerHTML = '<option value="">Все марки</option>';

  marks.forEach(mark => {
    const option = document.createElement("option");
    option.value = mark;
    option.textContent = mark;
    elements.markFilterSelect.append(option);
  });

  elements.markFilterSelect.value = marks.includes(current) ? current : "";
}

function populateModels() {
  const current = elements.modelFilterSelect.value;
  const selectedMark = elements.markFilterSelect.value;
  const models = [...new Set(
    state.listings
      .filter(item => !selectedMark || item.brand === selectedMark)
      .map(item => item.model)
      .filter(Boolean)
  )].sort((a, b) => a.localeCompare(b, "ru"));

  elements.modelFilterSelect.innerHTML = '<option value="">Все модели</option>';
  models.forEach(model => {
    const option = document.createElement("option");
    option.value = model;
    option.textContent = model;
    elements.modelFilterSelect.append(option);
  });

  elements.modelFilterSelect.value = models.includes(current) ? current : "";
}

function populateAttributeSelect(element, values, placeholder) {
  const current = element.value;
  element.innerHTML = `<option value="">${placeholder}</option>`;
  values.forEach(value => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    element.append(option);
  });
  element.value = values.includes(current) ? current : "";
}

function populateAttributeFilters() {
  const fuels = [...new Set(state.listings.map(item => item.fuelType).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const transmissions = [...new Set(state.listings.map(item => item.transmission).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const bodyTypes = [...new Set(state.listings.map(item => item.bodyType).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const drives = [...new Set(state.listings.map(item => item.driveType).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const steerings = [...new Set(state.listings.map(item => item.steeringSide).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const colors = [...new Set(state.listings.map(item => item.color).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));

  populateAttributeSelect(elements.fuelFilterSelect, fuels, "Любое");
  populateAttributeSelect(elements.transmissionFilterSelect, transmissions, "Любая");
  populateAttributeSelect(elements.bodyFilterSelect, bodyTypes, "Любой");
  populateAttributeSelect(elements.driveFilterSelect, drives, "Любой");
  populateAttributeSelect(elements.steeringFilterSelect, steerings, "Любой");
  populateAttributeSelect(elements.colorFilterSelect, colors, "Любой");
}

function getFilteredListings() {
  const search = elements.searchInput.value.trim().toLowerCase();
  const yearRange = getNormalizedRange(number(elements.yearFromInput.value), number(elements.yearToInput.value));
  const priceRange = getNormalizedRange(number(elements.priceFromInput.value), number(elements.priceToInput.value));
  const mileageRange = getNormalizedRange(number(elements.mileageFromInput.value), number(elements.mileageToInput.value));
  const city = elements.citySelect.value;
  const mark = elements.markFilterSelect.value;
  const model = elements.modelFilterSelect.value;
  const credit = elements.creditFilterSelect.value;
  const maxMonthlyPayment = number(elements.maxMonthlyPaymentInput.value);
  const repair = elements.repairFilterSelect.value;
  const fuel = elements.fuelFilterSelect.value;
  const transmission = elements.transmissionFilterSelect.value;
  const bodyType = elements.bodyFilterSelect.value;
  const drive = elements.driveFilterSelect.value;
  const steering = elements.steeringFilterSelect.value;
  const color = elements.colorFilterSelect.value;
  const seller = elements.sellerFilterSelect.value;
  const optionSearch = elements.optionSearchInput.value.trim().toLowerCase();
  const sort = elements.sortSelect.value;
  const showInactive = elements.showInactiveToggle.checked;

  const results = scoreListings(state.listings).filter(item => {
    const actualityMatch = showInactive || isListingActual(item);
    const titleMatch = !search || item.title.toLowerCase().includes(search);
    const yearMatch =
      (yearRange.min === null || (item.year !== null && item.year >= yearRange.min)) &&
      (yearRange.max === null || (item.year !== null && item.year <= yearRange.max));
    const priceMatch =
      (priceRange.min === null || item.price >= priceRange.min) &&
      (priceRange.max === null || item.price <= priceRange.max);
    const mileageMatch =
      (mileageRange.min === null || (item.mileage !== null && item.mileage >= mileageRange.min)) &&
      (mileageRange.max === null || (item.mileage !== null && item.mileage <= mileageRange.max));
    const cityMatch = !city || item.city === city;
    const markMatch = !mark || item.brand === mark;
    const modelMatch = !model || item.model === model;
    const creditMatch = !credit || (credit === "yes" ? item.creditAvailable : !item.creditAvailable);
    const monthlyPaymentMatch = !maxMonthlyPayment || (item.creditMonthlyPayment !== null && item.creditMonthlyPayment <= maxMonthlyPayment);
    const repairMatch = !repair || item.repairState === repair;
    const fuelMatch = !fuel || item.fuelType === fuel;
    const transmissionMatch = !transmission || item.transmission === transmission;
    const bodyTypeMatch = !bodyType || item.bodyType === bodyType;
    const driveMatch = !drive || item.driveType === drive;
    const steeringMatch = !steering || item.steeringSide === steering;
    const colorMatch = !color || item.color === color;
    const sellerMatch = !seller || getSellerFilterType(item) === seller;
    const optionMatch = !optionSearch || item.options.some(option => option.toLowerCase().includes(optionSearch));
    return actualityMatch && titleMatch && yearMatch && priceMatch && mileageMatch && cityMatch && markMatch && modelMatch && creditMatch && monthlyPaymentMatch && repairMatch && fuelMatch && transmissionMatch && bodyTypeMatch && driveMatch && steeringMatch && colorMatch && sellerMatch && optionMatch;
  });

  switch (sort) {
    case "credit-desc":
      results.sort((a, b) => b.creditScore - a.creditScore || a.creditMonthlyPayment - b.creditMonthlyPayment);
      break;
    case "liquidity-desc":
      results.sort((a, b) => b.liquidityScore - a.liquidityScore || b.score - a.score);
      break;
    case "fresh-desc":
      results.sort((a, b) => b.freshnessScore - a.freshnessScore || b.score - a.score);
      break;
    case "risk-asc":
      results.sort((a, b) => (a.riskScore ?? 50) - (b.riskScore ?? 50) || b.score - a.score);
      break;
    case "price-asc":
      results.sort((a, b) => a.price - b.price);
      break;
    case "price-desc":
      results.sort((a, b) => b.price - a.price);
      break;
    case "year-desc":
      results.sort((a, b) => (b.year ?? 0) - (a.year ?? 0));
      break;
    case "mileage-asc":
      results.sort((a, b) => (a.mileage ?? Number.MAX_SAFE_INTEGER) - (b.mileage ?? Number.MAX_SAFE_INTEGER));
      break;
    default:
      results.sort((a, b) => b.score - a.score || a.price - b.price);
      break;
  }

  return applyTableSort(results);
}

function renderStats(listings) {
  elements.totalCount.textContent = formatInteger(listings.length);

  if (!listings.length) {
    elements.avgPrice.textContent = "-";
    elements.avgYear.textContent = "-";
    elements.avgMileage.textContent = "-";
    elements.avgMonthlyPayment.textContent = "-";
    elements.bestTitle.textContent = "Нет данных";
    elements.bestPrice.textContent = "-";
    elements.bestScore.textContent = "-";
    return;
  }

  const avgPrice = listings.reduce((sum, item) => sum + item.price, 0) / listings.length;
  const yearValues = listings.map(item => item.year).filter(item => item !== null && item !== undefined);
  const mileageValues = listings.map(item => item.mileage).filter(item => item !== null && item !== undefined);
  const paymentValues = listings.map(item => item.creditMonthlyPayment).filter(item => item !== null && item !== undefined);
  const best = [...listings].sort((a, b) => b.score - a.score)[0];

  elements.avgPrice.textContent = formatPrice(avgPrice);
  elements.avgYear.textContent = yearValues.length
    ? String(Math.round(yearValues.reduce((sum, item) => sum + item, 0) / yearValues.length))
    : "-";
  elements.avgMileage.textContent = mileageValues.length
    ? formatMileage(mileageValues.reduce((sum, item) => sum + item, 0) / mileageValues.length)
    : "-";
  elements.avgMonthlyPayment.textContent = paymentValues.length
    ? formatPrice(paymentValues.reduce((sum, item) => sum + item, 0) / paymentValues.length)
    : "-";
  elements.bestTitle.textContent = best.title;
  elements.bestPrice.textContent = formatPrice(best.price);
  elements.bestScore.textContent = formatScore(best.score);
}

function renderBars(listings) {
  elements.bars.innerHTML = "";

  listings.slice(0, 6).forEach(item => {
    const row = document.createElement("div");
    row.className = "bar-row";
    row.innerHTML = `
      <div class="bar-label">${escapeHtml(item.title)}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.max(item.score * 100, 5)}%"></div></div>
      <div>${item.score.toFixed(2)}</div>
    `;
    elements.bars.append(row);
  });
}

function getAllScoredListings() {
  return scoreListings(state.listings);
}

function renderThumb(imageUrl, className = "thumb") {
  if (imageUrl) {
    const proxied = toProxiedImageUrl(imageUrl);
    return `<div class="${className}"><img src="${proxied}" alt="Фото авто" loading="lazy"></div>`;
  }

  return `<div class="${className} thumb--empty">нет фото</div>`;
}

function renderTopLists(listings) {
  elements.topScoreList.innerHTML = "";
  elements.topDealList.innerHTML = "";
  elements.topFreshList.innerHTML = "";
  elements.topBadList.innerHTML = "";
  elements.topCreditList.innerHTML = "";

  const topByScore = [...listings]
    .sort((a, b) => b.score - a.score || a.price - b.price)
    .slice(0, 5);
  const topByDeal = [...listings]
    .sort((a, b) => b.dealScore - a.dealScore || a.price - b.price)
    .slice(0, 5);
  const topByFresh = [...listings]
    .sort((a, b) => b.freshnessScore - a.freshnessScore || b.score - a.score)
    .slice(0, 5);
  const topBad = [...listings]
    .sort((a, b) => b.badScore - a.badScore || b.riskScore - a.riskScore)
    .slice(0, 5);
  const topCredit = [...listings]
    .filter(item => item.creditAvailable && item.creditMonthlyPayment)
    .sort((a, b) => b.creditScore - a.creditScore || a.creditMonthlyPayment - b.creditMonthlyPayment)
    .slice(0, 5);

  const renderList = (target, items, valueKey, valueLabel) => {
    if (!items.length) {
      target.innerHTML = `<div class="muted">Нет данных.</div>`;
      return;
    }

    items.forEach(item => {
      const row = document.createElement("button");
      row.className = "top-item";
      row.type = "button";
      row.dataset.id = item.id;
      row.innerHTML = `
        ${renderThumb(item.image, "thumb thumb--small")}
        <div>
          <div class="top-item-title">${escapeHtml(item.title)}</div>
          <div class="top-item-meta">${escapeHtml(item.city || "Без города")} · ${formatPrice(item.price)} · ${escapeHtml(formatListingDateBadge(item))}</div>
          <div class="top-item-badges">${renderListingBadges(item)}</div>
        </div>
        <div class="top-item-value">${formatScore(item[valueKey])} ${valueLabel}</div>
      `;
      target.append(row);
    });
  };

  renderList(elements.topScoreList, topByScore, "score", "ц/к");
  renderList(elements.topDealList, topByDeal, "dealScore", "выгода");
  renderList(elements.topFreshList, topByFresh, "freshnessScore", "fresh");
  renderList(elements.topBadList, topBad, "badScore", "bad");
  renderList(elements.topCreditList, topCredit, "creditScore", "credit");
}

function renderTable(listings) {
  renderTableSortHeaders();
  elements.resultsBody.innerHTML = "";
  elements.resultsCount.textContent = `${formatInteger(listings.length)} результатов`;

  if (!listings.length) {
    const row = document.createElement("tr");
    row.innerHTML = `<td colspan="9">Ничего не найдено по текущим фильтрам.</td>`;
    elements.resultsBody.append(row);
    return;
  }

  listings.forEach(item => {
    const row = document.createElement("tr");
    row.className = "is-clickable";
    row.dataset.id = item.id;
    row.innerHTML = `
      <td>${renderThumb(item.image)}</td>
      <td>
        <strong>${escapeHtml(item.title)}</strong><br>
        <span class="muted">${escapeHtml([item.city, item.brand, item.model].filter(Boolean).join(" · "))}</span>
        <div class="listing-subline">${renderListingBadges(item)}<span class="muted">Проверено: ${escapeHtml(formatDateTime(item.lastCheckedAt))}</span></div>
      </td>
      <td>${formatPrice(item.price)}</td>
      <td>${item.year ?? "-"}</td>
      <td>${escapeHtml(formatListingDateBadge(item))}</td>
      <td>${item.mileage ? formatMileage(item.mileage) : "-"}</td>
      <td>${item.owners ?? "-"}</td>
      <td><span class="score-badge">${formatScore(item.score)}</span></td>
      <td>${item.url ? `<a href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">Открыть</a>` : "-"}</td>
    `;
    elements.resultsBody.append(row);
  });
}

function renderFact(label, value) {
  return `
    <div class="modal-fact">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}

function renderMarketFacts(item) {
  if (item.avgPrice) {
    const difference = item.marketDifference ?? (item.avgPrice - item.price);
    const percent = item.marketDifferencePercent ?? (item.avgPrice ? ((difference / item.avgPrice) * 100) : 0);
    const label = difference > 0
      ? "Ниже рынка"
      : difference < 0
        ? "Выше рынка"
        : "По рынку";
    const value = difference === 0
      ? "почти без отклонения"
      : `${formatPrice(Math.abs(difference))} · ${formatPercent(percent)}`;

    return [
      renderFact("Средняя цена Kolesa", formatPrice(item.avgPrice)),
      renderFact(label, value)
    ];
  }

  const insightState = item.url ? state.listingInsights[item.url] : null;
  if (insightState?.status === "loading") {
    return [renderFact("Средняя цена Kolesa", "загрузка...")];
  }

  if (insightState?.status === "error") {
    return [renderFact("Средняя цена Kolesa", "не удалось получить")];
  }

  if (item.source === "kolesa.kz" && item.url.includes("/a/show/")) {
    return [renderFact("Средняя цена Kolesa", "анализируется...")];
  }

  return [];
}

function getBreakdownComment(label, value, item) {
  if (label === "Рынок" && Number.isFinite(item.marketDifferencePercent)) {
    if (item.marketDifferencePercent >= 10) {
      return `ниже рынка на ${formatPercent(item.marketDifferencePercent)}`;
    }
    if (item.marketDifferencePercent <= -10) {
      return `выше рынка на ${formatPercent(Math.abs(item.marketDifferencePercent))}`;
    }
    return "около средней цены по рынку";
  }

  if (label === "Цена") {
    return `текущая цена ${formatPrice(item.price)}`;
  }

  if (label === "Качество") {
    return value >= 0.7 ? "сильное общее состояние по данным объявления" : value >= 0.45 ? "средний уровень по качеству" : "есть слабые сигналы по качеству";
  }

  if (label === "Год") {
    return item.year ? `${item.year} год` : "год не указан";
  }

  if (label === "Пробег") {
    return item.mileage ? formatMileage(item.mileage) : "пробег не указан";
  }

  if (label === "Владельцы") {
    return item.owners ? `${item.owners}` : "нет данных";
  }

  if (label === "Свежесть") {
    return getListingDateMeta(item).value ? formatListingDateDetailed(item).toLowerCase() : "дата не указана";
  }

  if (label === "Риск") {
    return Number.isFinite(item.riskScore) ? `${Math.round(item.riskScore)}/100 риска` : "риск по объявлению средний";
  }

  if (label === "Фото") {
    return item.photoCount ? `${item.photoCount} фото` : "мало данных по фото";
  }

  if (label === "Продавец") {
    return item.isUsedCarDealer ? "похоже на автосалон или перекупа" : "ближе к частному продавцу";
  }

  return value >= 0.72 ? "сильная сторона" : value >= 0.45 ? "нейтрально" : "слабое место";
}

function renderListingFacts(item) {
  const actualityMeta = getActualityMeta(item);
  elements.modalFacts.innerHTML = [
    renderFact("Статус", actualityMeta.label),
    renderFact("Проверено", formatDateTime(item.lastCheckedAt)),
    renderFact("Цена/качество", formatScore(item.score)),
    renderFact("Качество", formatScore(item.qualityScore)),
    renderFact("Выгода", formatScore(item.dealScore)),
    ...renderMarketFacts(item),
    renderFact("Дата объявления", formatListingDateDetailed(item)),
    renderFact("Год", item.year ?? "-"),
    renderFact("Пробег", item.mileage ? formatMileage(item.mileage) : "-"),
    renderFact("Владельцы", item.owners ?? "-"),
    renderFact("Состояние", getRepairStateLabel(item) || "Неизвестно"),
    renderFact("VIN", item.vin || "-"),
    renderFact("Марка", item.brand || "-"),
    renderFact("Модель", item.model || "-"),
    renderFact("Топливо", item.fuelType || "-"),
    renderFact("КПП", item.transmission || "-"),
    renderFact("Кузов", item.bodyType || "-"),
    renderFact("Привод", item.driveType || "-"),
    renderFact("Руль", item.steeringSide || "-"),
    renderFact("Цвет", item.color || "-"),
    renderFact("Двигатель", item.engineVolume ? `${item.engineVolume} л` : "-"),
    renderFact("Фото", item.photoCount ?? "-"),
    renderFact("Кредит", formatYesNo(item.creditAvailable)),
    renderFact("Платёж / мес", item.creditMonthlyPayment ? formatPrice(item.creditMonthlyPayment) : "-"),
    renderFact("Кредит-скор", Number.isFinite(item.creditScore) ? `${(item.creditScore * 100).toFixed(0)}%` : "-"),
    renderFact("Продавец", getSellerLabel(item)),
    renderFact("Риск", Number.isFinite(item.riskScore) ? `${Math.round(item.riskScore)}/100` : "-"),
    renderFact("Ликвидность", Number.isFinite(item.liquidityScore) ? `${(item.liquidityScore * 100).toFixed(0)}%` : "-"),
    renderFact("Перекуп-скор", Number.isFinite(item.resellerScore) ? `${(item.resellerScore * 100).toFixed(0)}%` : "-"),
    renderFact("Цена", formatPrice(item.price)),
    renderFact("Город", item.city || "-")
  ].join("");
}

function renderListingBreakdown(item) {
  const parts = item.scoreParts || {
    price: 0.5,
    quality: 0.5,
    year: 0.5,
    mileage: 0.5,
    owners: 0.5,
    market: 0.5,
    freshness: 0.5,
    risk: 0.5,
    photos: 0.5,
    seller: 0.5
  };

  elements.modalBreakdown.innerHTML = [
    ["Качество", parts.quality],
    ["Цена", parts.price],
    ["Рынок", parts.market],
    ["Год", parts.year],
    ["Пробег", parts.mileage],
    ["Владельцы", parts.owners],
    ["Свежесть", parts.freshness],
    ["Риск", parts.risk],
    ["Фото", parts.photos],
    ["Продавец", parts.seller]
  ]
    .map(([label, value]) => `
      <div class="breakdown-item">
        <div class="breakdown-meta">
          <span>${escapeHtml(label)}</span>
          <small>${escapeHtml(getBreakdownComment(label, Number(value), item))}</small>
        </div>
        <div class="breakdown-track"><div class="breakdown-fill" style="width:${Math.max(Number(value) * 100, 4)}%"></div></div>
        <strong>${formatScorePercent(value)}</strong>
      </div>
    `)
    .join("");
}

function applyListingInsight(listingId, insight) {
  const mergeInsight = item => {
    if (!item || item.id !== listingId) {
      return item;
    }

    return {
      ...item,
      avgPrice: optionalPositiveNumber(insight.avgPrice),
      marketDifference: number(insight.marketDifference),
      marketDifferencePercent: number(insight.marketDifferencePercent)
    };
  };

  state.listings = state.listings.map(mergeInsight);
  state.renderedListings = state.renderedListings.map(mergeInsight);
}

function replaceListing(updatedItem) {
  const normalized = normalizeRow(updatedItem);
  if (normalized.url && normalized.photoGallery.length) {
    state.listingGalleries[normalized.url] = {
      status: normalized.photoGallery.length > 1 ? "loaded" : (state.listingGalleries[normalized.url]?.status || ""),
      images: normalized.photoGallery
    };
  }
  const replace = item => (item.id === normalized.id ? { ...item, ...normalized } : item);
  state.listings = state.listings.map(replace);
  state.renderedListings = state.renderedListings.map(replace);
}

function applyListingGallery(listingId, payload) {
  const current = getListingById(listingId);
  if (!current) {
    return;
  }

  const images = normalizePhotoGallery(payload.images ?? payload.photoGallery ?? []);
  const photoGallery = images.length ? images : current.photoGallery;
  const patch = {
    image: String(payload.image || photoGallery[0] || current.image || "").trim(),
    photoGallery,
    photoCount:
      optionalPositiveNumber(payload.photoCount ?? payload.photo_count)
      || optionalPositiveNumber(photoGallery.length)
      || current.photoCount
  };
  const merge = item => (item.id === listingId ? { ...item, ...patch } : item);

  state.listings = state.listings.map(merge);
  state.renderedListings = state.renderedListings.map(merge);
  if (current.url) {
    state.listingGalleries[current.url] = {
      status: "loaded",
      images: photoGallery
    };
  }
}

function renderSignals(item) {
  const finance = [];
  const signals = [];

  if (item.creditAvailable) {
    finance.push(`Кредит доступен`);
  }
  if (item.creditMonthlyPayment) {
    finance.push(`Ежемесячный платёж: ${formatPrice(item.creditMonthlyPayment)}`);
  }
  if (item.creditDownPayment) {
    finance.push(`Первоначальный взнос: ${formatPrice(item.creditDownPayment)}`);
  }
  if (item.phoneCount) {
    signals.push(`Контактов в объявлении: ${item.phoneCount}`);
  }
  if (item.phonePrefix) {
    signals.push(`Префикс номера: ${item.phonePrefix}`);
  }
  if (item.photoCount) {
    signals.push(`Фотографий: ${item.photoCount}`);
  }
  if (item.fuelType) {
    signals.push(`Топливо: ${item.fuelType}`);
  }
  if (item.driveType) {
    signals.push(`Привод: ${item.driveType}`);
  }
  if (item.steeringSide) {
    signals.push(`Руль: ${item.steeringSide}`);
  }
  if (item.color) {
    signals.push(`Цвет: ${item.color}`);
  }
  if (item.options.length) {
    signals.push(`Опции: ${item.options.join(", ")}`);
  }
  if (item.paidServices.length) {
    signals.push(`Продвижение: ${item.paidServices.join(", ")}`);
  }
  if (item.publicHistoryAvailable) {
    signals.push(`Есть публичная история авто на странице`);
  }
  if (item.historySummary) {
    signals.push(item.historySummary);
  }
  if (item.riskFlags.length) {
    signals.push(...item.riskFlags);
  }

  const lines = [...finance, ...signals];
  if (!lines.length) {
    return `<div class="muted">После проверки карточки здесь появятся кредитные условия и открытые сигналы по объявлению.</div>`;
  }

  return `<ul class="signal-list">${lines.map(line => `<li>${escapeHtml(line)}</li>`).join("")}</ul>`;
}

function renderVinStatusText(item) {
  if (item.vin) {
    return isValidVin(item.vin)
      ? `VIN сохранён: ${item.vin}`
      : "VIN сохранён, но выглядит некорректно.";
  }

  return "VIN можно сохранить для этого объявления и потом использовать для официальной проверки.";
}

function updateVinUi(item) {
  elements.modalVinInput.value = item?.vin || "";
  elements.modalVinNoteInput.value = item?.vinNote || "";
  elements.modalVinStatus.textContent = renderVinStatusText(item || {});
}

function renderModalGallery(item) {
  const galleryState = getListingGalleryState(item);
  const images = galleryState.images;
  if (!images.length) {
    elements.modalMedia.innerHTML = `<div class="modal-media-empty">Фото не найдено</div>`;
    return;
  }

  const currentIndex = Math.max(0, Math.min(state.modalGalleryIndex, images.length - 1));
  const currentImage = images[currentIndex];
  const proxiedImage = toProxiedImageUrl(currentImage);
  const hasMultiple = images.length > 1;
  const loadingNote = galleryState.status === "loading"
    ? `<span class="modal-gallery-note">Загружаем все фото...</span>`
    : galleryState.status === "error"
      ? `<span class="modal-gallery-note">Не удалось загрузить остальные фото</span>`
      : "";

  state.modalGalleryIndex = currentIndex;
  elements.modalMedia.innerHTML = `
    <div class="modal-gallery">
      <div class="modal-gallery-main">
        ${hasMultiple ? '<button class="modal-gallery-nav modal-gallery-nav--prev" type="button" data-gallery-step="-1" aria-label="Предыдущее фото">‹</button>' : ""}
        <a class="modal-gallery-link" href="${escapeHtml(proxiedImage)}" target="_blank" rel="noreferrer">
          <img src="${escapeHtml(proxiedImage)}" alt="${escapeHtml(item.title)}" loading="eager">
        </a>
        ${hasMultiple ? '<button class="modal-gallery-nav modal-gallery-nav--next" type="button" data-gallery-step="1" aria-label="Следующее фото">›</button>' : ""}
      </div>
      <div class="modal-gallery-caption">
        <div class="modal-gallery-meta">
          <strong>${currentIndex + 1} / ${images.length}</strong>
          ${loadingNote}
        </div>
        <a class="modal-gallery-open" href="${escapeHtml(proxiedImage)}" target="_blank" rel="noreferrer">Открыть фото</a>
      </div>
      ${hasMultiple ? `
        <div class="modal-gallery-thumbs">
          ${images.map((image, index) => `
            <button
              class="modal-gallery-thumb ${index === currentIndex ? "is-active" : ""}"
              type="button"
              data-gallery-index="${index}"
              aria-label="Фото ${index + 1}"
            >
              <img src="${escapeHtml(toProxiedImageUrl(image))}" alt="${escapeHtml(`${item.title} ${index + 1}`)}" loading="lazy">
            </button>
          `).join("")}
        </div>
      ` : ""}
    </div>
  `;
}

function stepModalGallery(direction) {
  const item = getListingById(state.selectedListingId);
  if (!item) {
    return;
  }

  const { images } = getListingGalleryState(item);
  if (images.length <= 1) {
    return;
  }

  state.modalGalleryIndex = (state.modalGalleryIndex + direction + images.length) % images.length;
  renderModalGallery(item);
}

async function loadListingGallery(item) {
  if (!item?.url || !isKolesaListing(item) || !item.url.includes("/a/show/")) {
    return;
  }

  const cached = state.listingGalleries[item.url];
  const fallbackImages = normalizePhotoGallery(item.photoGallery || (item.image ? [item.image] : []));
  if (cached?.status === "loading" || cached?.status === "loaded") {
    return;
  }

  state.listingGalleries[item.url] = {
    status: "loading",
    images: fallbackImages
  };
  if (state.selectedListingId === item.id) {
    renderModalGallery(getListingById(item.id) || item);
  }

  try {
    const response = await fetch(`/api/listings/gallery?url=${encodeURIComponent(item.url)}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Gallery failed");
    }

    applyListingGallery(item.id, {
      image: payload.image,
      images: payload.images || [],
      photoCount: payload.photoCount
    });
    if (state.selectedListingId === item.id) {
      const updated = getListingById(item.id) || item;
      renderModalGallery(updated);
      renderListingFacts(updated);
    }
  } catch (error) {
    state.listingGalleries[item.url] = {
      status: "error",
      images: fallbackImages
    };
    if (state.selectedListingId === item.id) {
      renderModalGallery(getListingById(item.id) || item);
    }
  }
}

function getActualListingsCount(listings = state.listings) {
  return listings.filter(isListingActual).length;
}

async function loadKolesaPriceInsight(item) {
  if (!item || item.avgPrice || !item.url || !item.url.includes("/a/show/") || item.source !== "kolesa.kz") {
    return;
  }

  const cached = state.listingInsights[item.url];
  if (cached?.status === "loading" || cached?.status === "loaded") {
    return;
  }

  state.listingInsights[item.url] = { status: "loading" };
  if (state.selectedListingId === item.id) {
    renderListingFacts(item);
  }

  try {
    const response = await fetch(`/api/kolesa/price-insight?url=${encodeURIComponent(item.url)}`);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Insight failed");
    }

    const insight = {
      avgPrice: payload.avgPrice,
      marketDifference: payload.marketDifference,
      marketDifferencePercent: payload.marketDifferencePercent
    };
    state.listingInsights[item.url] = {
      status: "loaded",
      ...insight
    };
    applyListingInsight(item.id, insight);

    if (state.selectedListingId === item.id) {
      const updatedItem = getListingById(item.id);
      if (updatedItem) {
        renderListingFacts(updatedItem);
      }
    }
  } catch (error) {
    state.listingInsights[item.url] = { status: "error" };
    if (state.selectedListingId === item.id) {
      const updatedItem = getListingById(item.id) || item;
      renderListingFacts(updatedItem);
    }
  }
}

async function checkListingActuality(listingId, { silent = false } = {}) {
  const item = getListingById(listingId);
  if (!item || !isKolesaListing(item)) {
    return;
  }

  elements.modalCheckBtn.disabled = true;
  elements.modalCheckBtn.textContent = "Проверка...";

  if (!silent) {
    setStatus("Источник: проверяем актуальность объявления...");
  }

  try {
    const response = await fetch("/api/listings/check", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ url: item.url })
    });

    const payload = await response.json();
    if (!response.ok || !payload.item) {
      throw new Error(payload.error || "Check failed");
    }

    replaceListing(payload.item);
    const updated = getListingById(listingId) || normalizeRow(payload.item);

    populateCities();
    render();

    if (state.selectedListingId === listingId && updated) {
      elements.modalTitle.textContent = updated.title;
      elements.modalPrice.textContent = formatPrice(updated.price);
      elements.modalCity.textContent = updated.city || "-";
      renderModalGallery(updated);
      renderListingFacts(updated);
      renderListingBreakdown(updated);
      elements.modalSignals.innerHTML = renderSignals(updated);
      updateVinUi(updated);
      elements.modalSource.textContent = updated.source || "Карточка объявления";
    }

    if (!silent) {
      setStatus(`Источник: статус обновлен (${getActualityMeta(updated).label.toLowerCase()})`);
    }
  } catch (error) {
    if (!silent) {
      window.alert(error.message || "Не удалось проверить объявление.");
      setStatus("Источник: ошибка проверки актуальности");
    }
  } finally {
    if (state.selectedListingId === listingId) {
      const updated = getListingById(listingId) || item;
      elements.modalCheckBtn.disabled = !isKolesaListing(updated);
      elements.modalCheckBtn.textContent = "Проверить актуальность";
    } else {
      elements.modalCheckBtn.disabled = false;
      elements.modalCheckBtn.textContent = "Проверить актуальность";
    }
  }
}

async function bulkCheckRenderedListings() {
  const targets = state.renderedListings.filter(isKolesaListing);
  if (!targets.length) {
    window.alert("В текущем списке нет объявлений Kolesa для проверки.");
    return;
  }

  elements.bulkCheckBtn.disabled = true;
  elements.bulkCheckBtn.textContent = `Проверка 0/${targets.length}`;
  setStatus(`Источник: перепроверка ${targets.length} объявлений...`);

  let checked = 0;
  let active = 0;
  let stale = 0;
  let archived = 0;
  let unavailable = 0;
  let failed = 0;

  try {
    for (const item of targets) {
      try {
        const response = await fetch("/api/listings/check", {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({ url: item.url })
        });

        const payload = await response.json();
        if (!response.ok || !payload.item) {
          throw new Error(payload.error || "Check failed");
        }

        replaceListing(payload.item);
        const updated = normalizeRow(payload.item);
        switch (updated.actualityStatus) {
          case "stale":
            stale += 1;
            break;
          case "archived":
            archived += 1;
            break;
          case "unavailable":
            unavailable += 1;
            break;
          default:
            active += 1;
            break;
        }
      } catch (error) {
        failed += 1;
      }

      checked += 1;
      elements.bulkCheckBtn.textContent = `Проверка ${checked}/${targets.length}`;
      setStatus(`Источник: перепроверка ${checked}/${targets.length} объявлений...`);

      if (checked % 5 === 0 || checked === targets.length) {
        populateCities();
        render();

        if (state.selectedListingId) {
          const selected = getListingById(state.selectedListingId);
          if (selected && !elements.detailModal.hidden) {
            elements.modalTitle.textContent = selected.title;
            elements.modalPrice.textContent = formatPrice(selected.price);
            elements.modalCity.textContent = selected.city || "-";
            renderModalGallery(selected);
            renderListingFacts(selected);
            renderListingBreakdown(selected);
          }
        }
      }

      if (checked < targets.length) {
        await wait(150);
      }
    }

    setStatus(
      `Источник: проверено ${checked}. Активно ${active}, не подтверждено ${stale}, архив ${archived}, недоступно ${unavailable}, ошибок ${failed}.`
    );
  } finally {
    elements.bulkCheckBtn.disabled = false;
    elements.bulkCheckBtn.textContent = "Проверить текущие";
  }
}

async function saveVinForSelectedListing() {
  const item = getListingById(state.selectedListingId);
  if (!item) {
    return;
  }

  const vin = normalizeVin(elements.modalVinInput.value);
  const vinNote = elements.modalVinNoteInput.value.trim();

  if (vin && !isValidVin(vin)) {
    window.alert("VIN должен содержать 17 символов без I, O и Q.");
    return;
  }

  elements.modalVinSaveBtn.disabled = true;
  elements.modalVinStatus.textContent = "Сохраняем VIN...";

  try {
    const response = await fetch("/api/listings/vin", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        url: item.url,
        advertId: item.advertId,
        vin,
        vinNote
      })
    });

    const payload = await response.json();
    if (!response.ok || !payload.item) {
      throw new Error(payload.error || "Save failed");
    }

    replaceListing(payload.item);
    const updated = getListingById(item.id) || normalizeRow(payload.item);
    updateVinUi(updated);
    render();
    setStatus(vin ? "Источник: VIN сохранён" : "Источник: VIN очищен");
  } catch (error) {
    elements.modalVinStatus.textContent = error.message || "Не удалось сохранить VIN.";
  } finally {
    elements.modalVinSaveBtn.disabled = false;
  }
}

async function copySelectedVin() {
  const vin = normalizeVin(elements.modalVinInput.value);
  if (!vin) {
    window.alert("Сначала введи VIN.");
    return;
  }

  try {
    await navigator.clipboard.writeText(vin);
    elements.modalVinStatus.textContent = `VIN скопирован: ${vin}`;
  } catch (error) {
    elements.modalVinStatus.textContent = "Не удалось скопировать VIN.";
  }
}

function getListingById(listingId) {
  return state.renderedListings.find(item => item.id === listingId) ||
    getAllScoredListings().find(item => item.id === listingId) ||
    null;
}

function getComparedItems() {
  return state.compareIds
    .map(id => getListingById(id))
    .filter(Boolean);
}

function getFavoriteItems() {
  return state.favoriteIds
    .map(id => getListingById(id))
    .filter(Boolean);
}

async function saveAppState() {
  if (!isAuthenticated()) {
    return;
  }

  try {
    await apiFetch("/api/state", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        activeProfileId: state.activeProfileId,
        favorites: state.favoriteIds,
        comparisonHistory: state.comparisonHistory
      })
    }, true);
  } catch (error) {
    setStatus("Источник: серверные данные, но состояние не сохранено");
  }
}

async function loadAppState() {
  await loadAuthSession();
}

function renderProfiles() {
  renderAuth();
  elements.profileSelect.innerHTML = "";

  if (!isAuthenticated()) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "Нужен вход";
    elements.profileSelect.append(option);
    elements.profileSelect.disabled = true;
    elements.profileNameInput.disabled = true;
    elements.createProfileBtn.disabled = true;
    return;
  }

  elements.profileSelect.disabled = false;
  elements.profileNameInput.disabled = false;
  elements.createProfileBtn.disabled = false;

  const profiles = state.profiles.length ? state.profiles : [{ id: "default", name: "Основной" }];
  profiles.forEach(profile => {
    const option = document.createElement("option");
    option.value = profile.id;
    option.textContent = profile.name;
    elements.profileSelect.append(option);
  });

  elements.profileSelect.value = profiles.some(profile => profile.id === state.activeProfileId)
    ? state.activeProfileId
    : profiles[0].id;
}

async function createProfile() {
  if (!isAuthenticated()) {
    window.alert("Сначала войди в аккаунт.");
    return;
  }

  const name = elements.profileNameInput.value.trim();
  if (!name) {
    window.alert("Введите имя профиля.");
    return;
  }

  try {
    const response = await apiFetch("/api/profiles", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ name })
    }, true);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Create profile failed");
    }

    applyServerState(payload);
    state.compareIds = [];
    state.compareWinnerId = null;
    elements.profileNameInput.value = "";
    updateComparePanel();
    render();
  } catch (error) {
    window.alert(error.message || "Не удалось создать профиль.");
  }
}

async function switchProfile(profileId) {
  if (!isAuthenticated() || !profileId || profileId === state.activeProfileId) {
    return;
  }

  try {
    const response = await apiFetch("/api/profiles/active", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ profileId })
    }, true);
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Switch profile failed");
    }

    applyServerState(payload);
    state.compareIds = [];
    state.compareWinnerId = null;
    updateComparePanel();
    render();
  } catch (error) {
    window.alert(error.message || "Не удалось переключить профиль.");
  }
}

function updateComparePanel() {
  elements.compareCount.textContent = `${formatInteger(state.compareIds.length)} из 3 выбрано`;
  elements.compareChips.innerHTML = "";
  elements.openCompareBtn.disabled = state.compareIds.length < 2;
  elements.pickBestCompareBtn.disabled = state.compareIds.length < 2;
  elements.exportCompareCsvBtn.disabled = state.compareIds.length < 2;
  elements.exportComparePdfBtn.disabled = state.compareIds.length < 2;

  if (!state.compareIds.length) {
    state.compareWinnerId = null;
    elements.compareWinnerText.textContent = "Победитель пока не выбран.";
    elements.compareChips.innerHTML = `<span class="muted">Добавь машины из карточки объявления.</span>`;
    return;
  }

  if (state.compareWinnerId && !state.compareIds.includes(state.compareWinnerId)) {
    state.compareWinnerId = null;
  }

  if (state.compareWinnerId) {
    const winner = getListingById(state.compareWinnerId);
    elements.compareWinnerText.textContent = winner
      ? `Лучший вариант сейчас: ${winner.title} (${winner.score.toFixed(2)})`
      : "Победитель пока не выбран.";
  } else {
    elements.compareWinnerText.textContent = "Победитель пока не выбран.";
  }

  state.compareIds.forEach(id => {
    const item = getListingById(id);
    if (!item) {
      return;
    }

    const chip = document.createElement("div");
    chip.className = "compare-chip";
    chip.innerHTML = `
      <span>${escapeHtml(item.title)}</span>
      <button type="button" data-remove-id="${escapeHtml(id)}">×</button>
    `;
    elements.compareChips.append(chip);
  });
}

function renderFavorites() {
  elements.favoritesList.innerHTML = "";

  if (!isAuthenticated()) {
    elements.favoritesList.innerHTML = `<div class="muted">Войди в аккаунт, чтобы сохранять избранное.</div>`;
    return;
  }

  const items = getFavoriteItems();
  if (!items.length) {
    elements.favoritesList.innerHTML = `<div class="muted">Пока пусто.</div>`;
    return;
  }

  items.forEach(item => {
    const row = document.createElement("div");
    row.className = "saved-item";
    row.dataset.id = item.id;
    row.innerHTML = `
      <div class="saved-item-row">
        <div class="saved-item-title">${escapeHtml(item.title)}</div>
        <div class="saved-item-actions">
          <button type="button" data-remove-favorite="${escapeHtml(item.id)}">убрать</button>
        </div>
      </div>
      <div class="saved-item-meta">${escapeHtml(item.city || "Без города")} · ${formatPrice(item.price)}</div>
    `;
    elements.favoritesList.append(row);
  });
}

function renderHistory() {
  elements.historyList.innerHTML = "";

  if (!isAuthenticated()) {
    elements.historyList.innerHTML = `<div class="muted">Войди в аккаунт, чтобы хранить историю.</div>`;
    return;
  }

  if (!state.comparisonHistory.length) {
    elements.historyList.innerHTML = `<div class="muted">Истории пока нет.</div>`;
    return;
  }

  state.comparisonHistory.slice(0, 8).forEach((entry, index) => {
    const row = document.createElement("div");
    row.className = "saved-item";
    row.dataset.historyIndex = String(index);
    row.innerHTML = `
      <div class="saved-item-row">
        <div class="saved-item-title">${escapeHtml(entry.winnerTitle || "Без победителя")}</div>
        <div class="saved-item-actions">
          <button type="button" data-restore-history="${index}">восстановить</button>
        </div>
      </div>
      <div class="saved-item-meta">${escapeHtml((entry.titles || []).join(" • "))}</div>
      <div class="saved-item-meta">${escapeHtml(new Date(entry.createdAt).toLocaleString("ru-RU"))}</div>
    `;
    elements.historyList.append(row);
  });
}

function toggleFavorite(listingId) {
  if (!isAuthenticated()) {
    window.alert("Сначала войди в аккаунт.");
    return;
  }

  if (state.favoriteIds.includes(listingId)) {
    state.favoriteIds = state.favoriteIds.filter(id => id !== listingId);
  } else {
    state.favoriteIds = [listingId, ...state.favoriteIds.filter(id => id !== listingId)].slice(0, 30);
  }

  if (state.selectedListingId === listingId) {
    elements.modalFavoriteBtn.textContent = state.favoriteIds.includes(listingId)
      ? "Убрать из избранного"
      : "В избранное";
  }

  renderFavorites();
  void saveAppState();
}

function restoreHistory(index) {
  if (!isAuthenticated()) {
    window.alert("Сначала войди в аккаунт.");
    return;
  }

  const entry = state.comparisonHistory[index];
  if (!entry) {
    return;
  }

  state.compareIds = Array.isArray(entry.compareIds) ? entry.compareIds.slice(0, 3) : [];
  state.compareWinnerId = entry.winnerId || null;
  updateComparePanel();
  openCompareModal();
}

function toggleCompare(listingId) {
  const exists = state.compareIds.includes(listingId);
  if (exists) {
    state.compareIds = state.compareIds.filter(id => id !== listingId);
  } else {
    if (state.compareIds.length >= 3) {
      window.alert("Можно сравнить максимум 3 машины.");
      return;
    }
    state.compareIds = [...state.compareIds, listingId];
  }

  if (state.compareIds.length < 2) {
    state.compareWinnerId = null;
  }

  if (state.selectedListingId === listingId) {
    const selected = state.compareIds.includes(listingId);
    elements.modalCompareBtn.textContent = selected ? "Убрать из сравнения" : "Добавить к сравнению";
  }

  updateComparePanel();
}

function pickBestComparedListing() {
  const items = getComparedItems();

  if (items.length < 2) {
    window.alert("Нужно выбрать минимум 2 машины.");
    return null;
  }

  const winner = [...items].sort((a, b) => {
    if (b.score !== a.score) {
      return b.score - a.score;
    }
    if (b.qualityScore !== a.qualityScore) {
      return b.qualityScore - a.qualityScore;
    }
    return a.price - b.price;
  })[0];

  state.compareWinnerId = winner.id;
  if (isAuthenticated()) {
    state.comparisonHistory = [
      {
        createdAt: new Date().toISOString(),
        winnerId: winner.id,
        compareIds: items.map(item => item.id),
        winnerTitle: winner.title,
        titles: items.map(item => item.title)
      },
      ...state.comparisonHistory
    ].slice(0, 20);
  }
  updateComparePanel();
  if (isAuthenticated()) {
    renderHistory();
    void saveAppState();
  }
  return winner;
}

function exportComparedAsCsv() {
  const items = getComparedItems();
  if (items.length < 2) {
    window.alert("Нужно выбрать минимум 2 машины.");
    return;
  }

  const rows = [
    [
      "title",
      "price",
      "year",
      "mileage",
      "owners",
      "city",
      "score",
      "qualityScore",
      "dealScore",
      "engineVolume",
      "source",
      "url",
      "description"
    ],
    ...items.map(item => [
      item.title,
      item.price,
      item.year ?? "",
      item.mileage ?? "",
      item.owners ?? "",
      item.city || "",
      formatScore(item.score),
      formatScore(item.qualityScore),
      formatScore(item.dealScore),
      item.engineVolume ?? "",
      item.source || "",
      item.url || "",
      item.description || ""
    ])
  ];

  const csv = rows
    .map(row => row.map(value => `"${String(value ?? "").replace(/"/g, '""')}"`).join(","))
    .join("\r\n");

  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "compare-cars.csv";
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function exportComparedAsPdf() {
  const items = getComparedItems();
  if (items.length < 2) {
    window.alert("Нужно выбрать минимум 2 машины.");
    return;
  }

  const winner = state.compareWinnerId ? getListingById(state.compareWinnerId) : null;
  const printWindow = window.open("", "_blank", "width=1200,height=900");
  if (!printWindow) {
    window.alert("Браузер заблокировал окно печати.");
    return;
  }

  const cards = items.map(item => `
    <section class="card ${winner && winner.id === item.id ? "winner" : ""}">
      ${winner && winner.id === item.id ? `<div class="badge">Лучший выбор</div>` : ""}
      <h2>${escapeHtml(item.title)}</h2>
      <div class="price">${formatPrice(item.price)}</div>
      <div class="grid">
        <div><span>Город</span><strong>${escapeHtml(item.city || "-")}</strong></div>
        <div><span>Год</span><strong>${item.year ?? "-"}</strong></div>
        <div><span>Пробег</span><strong>${item.mileage ? formatMileage(item.mileage) : "-"}</strong></div>
        <div><span>Владельцы</span><strong>${item.owners ?? "-"}</strong></div>
        <div><span>Цена/качество</span><strong>${formatScore(item.score)}</strong></div>
        <div><span>Качество</span><strong>${formatScore(item.qualityScore)}</strong></div>
        <div><span>Выгода</span><strong>${formatScore(item.dealScore)}</strong></div>
      </div>
      <p>${escapeHtml(item.description || "Описание не указано.")}</p>
      <p class="url">${escapeHtml(item.url || "")}</p>
    </section>
  `).join("");

  printWindow.document.write(`
    <!DOCTYPE html>
    <html lang="ru">
    <head>
      <meta charset="UTF-8">
      <title>Сравнение машин</title>
      <style>
        body { font-family: Arial, sans-serif; margin: 24px; color: #111827; }
        h1 { margin: 0 0 8px; }
        .meta { color: #6b7280; margin-bottom: 20px; }
        .wrap { display: grid; grid-template-columns: repeat(${Math.min(items.length, 3)}, 1fr); gap: 16px; }
        .card { border: 1px solid #d1d5db; border-radius: 16px; padding: 16px; page-break-inside: avoid; }
        .winner { border-color: #111827; }
        .badge { display: inline-block; margin-bottom: 10px; padding: 6px 10px; background: #111827; color: #fff; border-radius: 999px; font-size: 12px; font-weight: 700; }
        .price { font-size: 24px; font-weight: 800; margin-bottom: 12px; }
        .grid { display: grid; gap: 10px; margin-bottom: 12px; }
        .grid div { border-bottom: 1px solid #e5e7eb; padding-bottom: 8px; }
        .grid span { display: block; color: #6b7280; font-size: 12px; margin-bottom: 4px; }
        .url { color: #6b7280; font-size: 12px; word-break: break-all; }
      </style>
    </head>
    <body>
      <h1>Сравнение машин</h1>
      <div class="meta">Экспортировано ${new Date().toLocaleString("ru-RU")}</div>
      <div class="wrap">${cards}</div>
      <script>
        window.onload = () => {
          window.print();
        };
      </script>
    </body>
    </html>
  `);
  printWindow.document.close();
}

function renderCompareCard(item) {
  const isWinner = state.compareWinnerId === item.id;
  return `
    <article class="compare-card${isWinner ? " is-winner" : ""}">
      ${renderThumb(item.image, "thumb")}
      <div class="compare-card-body">
        ${isWinner ? `<div class="compare-badge">Лучший выбор</div>` : ""}
        <h3>${escapeHtml(item.title)}</h3>
        <div class="compare-card-price">${formatPrice(item.price)}</div>
        <div class="compare-specs">
          <div class="compare-spec"><span>Город</span><strong>${escapeHtml(item.city || "-")}</strong></div>
          <div class="compare-spec"><span>Год</span><strong>${item.year ?? "-"}</strong></div>
          <div class="compare-spec"><span>Пробег</span><strong>${item.mileage ? formatMileage(item.mileage) : "-"}</strong></div>
          <div class="compare-spec"><span>Владельцы</span><strong>${item.owners ?? "-"}</strong></div>
          <div class="compare-spec"><span>Цена/качество</span><strong>${formatScore(item.score)}</strong></div>
          <div class="compare-spec"><span>Качество</span><strong>${formatScore(item.qualityScore)}</strong></div>
          <div class="compare-spec"><span>Выгода</span><strong>${formatScore(item.dealScore)}</strong></div>
          <div class="compare-spec"><span>Двигатель</span><strong>${item.engineVolume ? `${item.engineVolume} л` : "-"}</strong></div>
        </div>
        <p class="compare-description">${escapeHtml(item.description || "Описание не указано.")}</p>
        ${item.url ? `<a class="primary-btn modal-link" href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">Открыть</a>` : ""}
      </div>
    </article>
  `;
}

function openCompareModal() {
  const items = getComparedItems();

  if (items.length < 2) {
    window.alert("Нужно выбрать минимум 2 машины.");
    return;
  }

  if (state.compareWinnerId && !items.some(item => item.id === state.compareWinnerId)) {
    state.compareWinnerId = null;
  }

  elements.compareGrid.innerHTML = items.map(renderCompareCard).join("");
  elements.compareModal.hidden = false;
  document.body.style.overflow = "hidden";
}

function closeCompareModal() {
  elements.compareModal.hidden = true;
  if (elements.detailModal.hidden) {
    document.body.style.overflow = "";
  }
}

function openListingDetails(listingId) {
  const item = getListingById(listingId);
  if (!item) {
    return;
  }

  state.selectedListingId = listingId;
  state.modalGalleryIndex = 0;
  renderModalGallery(item);
  elements.modalSource.textContent = item.source || "Карточка объявления";
  elements.modalTitle.textContent = item.title;
  elements.modalPrice.textContent = formatPrice(item.price);
  elements.modalCity.textContent = item.city || "Без города";
  renderListingFacts(item);
  renderListingBreakdown(item);
  elements.modalDescription.textContent = item.description || "Описание не указано.";
  elements.modalSignals.innerHTML = renderSignals(item);
  updateVinUi(item);

  if (item.url) {
    elements.modalLink.href = item.url;
    elements.modalLink.removeAttribute("aria-disabled");
  } else {
    elements.modalLink.href = "#";
    elements.modalLink.setAttribute("aria-disabled", "true");
  }

  elements.modalCompareBtn.textContent = state.compareIds.includes(listingId)
    ? "Убрать из сравнения"
    : "Добавить к сравнению";
  elements.modalFavoriteBtn.textContent = !isAuthenticated()
    ? "Войти для избранного"
    : state.favoriteIds.includes(listingId)
      ? "Убрать из избранного"
      : "В избранное";
  elements.modalCheckBtn.disabled = !isKolesaListing(item);
  elements.modalCheckBtn.textContent = "Проверить актуальность";

  elements.detailModal.hidden = false;
  document.body.style.overflow = "hidden";
  void loadListingGallery(item);
  void loadKolesaPriceInsight(item);
  if (shouldAutoCheckActuality(item)) {
    void checkListingActuality(item.id, { silent: true });
  }
}

function closeListingDetails() {
  state.selectedListingId = null;
  state.modalGalleryIndex = 0;
  elements.detailModal.hidden = true;
  if (elements.compareModal.hidden) {
    document.body.style.overflow = "";
  }
}

function render() {
  const listings = getFilteredListings();
  state.renderedListings = listings;
  renderProfiles();
  populateCities();
  populateMarks();
  populateModels();
  populateAttributeFilters();
  renderStats(listings);
  renderFavorites();
  renderHistory();
  renderTopLists(listings);
  renderBars(listings);
  renderTable(listings);
}

function resetFilters() {
  elements.searchInput.value = "";
  elements.yearFromInput.value = "";
  elements.yearToInput.value = "";
  elements.priceFromInput.value = "";
  elements.priceToInput.value = "";
  elements.mileageFromInput.value = "";
  elements.mileageToInput.value = "";
  elements.maxMonthlyPaymentInput.value = "";
  elements.citySelect.value = "";
  elements.markFilterSelect.value = "";
  elements.modelFilterSelect.value = "";
  elements.creditFilterSelect.value = "";
  elements.repairFilterSelect.value = "";
  elements.fuelFilterSelect.value = "";
  elements.transmissionFilterSelect.value = "";
  elements.bodyFilterSelect.value = "";
  elements.driveFilterSelect.value = "";
  elements.steeringFilterSelect.value = "";
  elements.colorFilterSelect.value = "";
  elements.sellerFilterSelect.value = "";
  elements.optionSearchInput.value = "";
  elements.sortSelect.value = "score";
  elements.showInactiveToggle.checked = false;
  render();
}

async function saveListingsToServer(listings) {
  try {
    const response = await fetch("/api/listings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(listings)
    });

    if (!response.ok) {
      throw new Error("Save failed");
    }

    setStatus("Источник: данные сохранены на сервере");
  } catch (error) {
    setStatus("Источник: локальный файл загружен, но сервер недоступен");
  }
}

function pruneUserStateToCurrentListings() {
  const validIds = new Set(state.listings.map(item => item.id));
  state.favoriteIds = state.favoriteIds.filter(id => validIds.has(id));
  state.comparisonHistory = state.comparisonHistory
    .map(entry => ({
      ...entry,
      compareIds: Array.isArray(entry.compareIds) ? entry.compareIds.filter(id => validIds.has(id)).slice(0, 3) : []
    }))
    .filter(entry => entry.compareIds.length >= 2);
}

async function loadListingsFromServer() {
  try {
    const response = await fetch("/api/listings");
    if (!response.ok) {
      throw new Error("Load failed");
    }

    const payload = await response.json();
    const rows = Array.isArray(payload.items) ? payload.items : [];
    if (rows.length) {
      state.listings = normalizeRows(rows);
      state.compareIds = [];
      state.compareWinnerId = null;
      pruneUserStateToCurrentListings();
      populateCities();
      updateComparePanel();
      render();
      if (isAuthenticated()) {
        void saveAppState();
      }
      setStatus(`Источник: серверные данные, активных ${getActualListingsCount()} из ${state.listings.length}`);
      return;
    }
  } catch (error) {
    setStatus("Источник: демо-данные");
  }
}

async function importFromKolesa() {
  const url = elements.kolesaUrlInput.value.trim() || buildImportUrlFromFilters();
  await importFromKolesaUrl(url, getImportLimit());
}

async function importFromKolesaUrl(url, limit = getImportLimit()) {
  const trimmedUrl = String(url || "").trim();
  if (!trimmedUrl) {
    window.alert("Вставь ссылку Kolesa.");
    return;
  }

  setImportBusy(true);
  setStatus(`Источник: импорт с Kolesa, цель ${limit} объявлений...`);

  try {
    const response = await fetch("/api/import/kolesa", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ url: trimmedUrl, save: true, limit })
    });

    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Import failed");
    }

    state.listings = normalizeRows(payload.items || []);
    state.compareIds = [];
    state.compareWinnerId = null;
    pruneUserStateToCurrentListings();
    populateCities();
    updateComparePanel();
    resetFilters();
    if (isAuthenticated()) {
      void saveAppState();
    }
    const pagesLoaded = Number(payload.pagesLoaded) || 1;
    setStatus(`Источник: Kolesa, активных ${getActualListingsCount()} из ${state.listings.length}, ${pagesLoaded} стр.`);
  } catch (error) {
    window.alert(error.message || "Не удалось импортировать данные.");
    setStatus("Источник: ошибка импорта");
  } finally {
    setImportBusy(false);
  }
}

async function handleFileUpload(event) {
  const [file] = event.target.files;
  if (!file) {
    return;
  }

  const reader = new FileReader();
  reader.onload = async () => {
    try {
      const parsed = JSON.parse(String(reader.result));
      const rows = Array.isArray(parsed) ? parsed : parsed.items;
      if (!Array.isArray(rows)) {
        throw new Error("Некорректный формат файла.");
      }

      state.listings = normalizeRows(rows);
      state.compareIds = [];
      state.compareWinnerId = null;
      pruneUserStateToCurrentListings();
      populateCities();
      updateComparePanel();
      resetFilters();
      await saveListingsToServer(state.listings);
      await saveAppState();
    } catch (error) {
      window.alert("Не удалось загрузить файл JSON.");
    }
  };
  reader.readAsText(file, "utf-8");
}

[
  elements.searchInput,
  elements.yearFromInput,
  elements.yearToInput,
  elements.priceFromInput,
  elements.priceToInput,
  elements.mileageFromInput,
  elements.mileageToInput,
  elements.maxMonthlyPaymentInput,
  elements.citySelect,
  elements.modelFilterSelect,
  elements.creditFilterSelect,
  elements.repairFilterSelect,
  elements.fuelFilterSelect,
  elements.transmissionFilterSelect,
  elements.bodyFilterSelect,
  elements.driveFilterSelect,
  elements.steeringFilterSelect,
  elements.colorFilterSelect,
  elements.sellerFilterSelect,
  elements.optionSearchInput,
  elements.sortSelect,
  elements.showInactiveToggle
].forEach(element => {
  element.addEventListener("input", render);
  element.addEventListener("change", render);
});

elements.markFilterSelect.addEventListener("change", () => {
  populateModels();
  if (![...elements.modelFilterSelect.options].some(option => option.value === elements.modelFilterSelect.value)) {
    elements.modelFilterSelect.value = "";
  }
  render();
});

elements.loginBtn.addEventListener("click", loginUser);
elements.registerBtn.addEventListener("click", registerUser);
elements.logoutBtn.addEventListener("click", logoutUser);
elements.resetBtn.addEventListener("click", resetFilters);
elements.createProfileBtn.addEventListener("click", createProfile);
elements.profileSelect.addEventListener("change", event => {
  void switchProfile(event.target.value);
});
elements.fileInput.addEventListener("change", handleFileUpload);
elements.importKolesaBtn.addEventListener("click", importFromKolesa);
elements.importAktauBtn.addEventListener("click", () => {
  elements.importCitySelect.value = "aktau";
  elements.kolesaUrlInput.dataset.manual = "false";
  syncImportUrlPreview();
  void importFromKolesaUrl(buildImportUrlFromFilters(), getImportLimit());
});
[
  elements.importCitySelect,
  elements.importMarkSelect,
  elements.importBodySelect,
  elements.importTransmissionSelect,
  elements.importCustomSelect,
  elements.importNeedRepairSelect,
  elements.importPriceFromInput,
  elements.importPriceToInput
].forEach(element => {
  element.addEventListener("input", () => {
    elements.kolesaUrlInput.dataset.manual = "false";
    syncImportUrlPreview();
  });
  element.addEventListener("change", () => {
    elements.kolesaUrlInput.dataset.manual = "false";
    syncImportUrlPreview();
  });
});
elements.kolesaUrlInput.addEventListener("input", () => {
  elements.kolesaUrlInput.dataset.manual = elements.kolesaUrlInput.value.trim() ? "true" : "false";
});
elements.modalCloseBtn.addEventListener("click", closeListingDetails);
elements.modalBackdrop.addEventListener("click", closeListingDetails);
elements.modalMedia.addEventListener("click", event => {
  const thumb = event.target.closest("[data-gallery-index]");
  if (thumb) {
    state.modalGalleryIndex = Number(thumb.dataset.galleryIndex) || 0;
    const item = getListingById(state.selectedListingId);
    if (item) {
      renderModalGallery(item);
    }
    return;
  }

  const nav = event.target.closest("[data-gallery-step]");
  if (nav) {
    stepModalGallery(Number(nav.dataset.galleryStep) || 0);
  }
});
elements.modalFavoriteBtn.addEventListener("click", () => {
  if (state.selectedListingId) {
    toggleFavorite(state.selectedListingId);
  }
});
elements.modalCompareBtn.addEventListener("click", () => {
  if (state.selectedListingId) {
    toggleCompare(state.selectedListingId);
  }
});
elements.modalCheckBtn.addEventListener("click", () => {
  if (state.selectedListingId) {
    void checkListingActuality(state.selectedListingId);
  }
});
elements.modalVinSaveBtn.addEventListener("click", () => {
  if (state.selectedListingId) {
    void saveVinForSelectedListing();
  }
});
elements.modalVinCopyBtn.addEventListener("click", () => {
  void copySelectedVin();
});
elements.bulkCheckBtn.addEventListener("click", () => {
  void bulkCheckRenderedListings();
});
elements.pickBestCompareBtn.addEventListener("click", () => {
  const winner = pickBestComparedListing();
  if (winner && !elements.compareModal.hidden) {
    openCompareModal();
  }
});
elements.exportCompareCsvBtn.addEventListener("click", exportComparedAsCsv);
elements.exportComparePdfBtn.addEventListener("click", exportComparedAsPdf);
elements.openCompareBtn.addEventListener("click", openCompareModal);
elements.clearCompareBtn.addEventListener("click", () => {
  state.compareIds = [];
  state.compareWinnerId = null;
  updateComparePanel();
});
elements.compareCloseBtn.addEventListener("click", closeCompareModal);
elements.compareBackdrop.addEventListener("click", closeCompareModal);
elements.resultsHead.addEventListener("click", event => {
  const button = event.target.closest("[data-sort-key]");
  if (!button) {
    return;
  }

  const nextKey = button.dataset.sortKey;
  if (state.tableSort.key === nextKey) {
    state.tableSort.direction = state.tableSort.direction === "asc" ? "desc" : "asc";
  } else {
    state.tableSort.key = nextKey;
    state.tableSort.direction = "asc";
  }

  render();
});
elements.resultsBody.addEventListener("click", event => {
  if (event.target.closest("a")) {
    return;
  }

  const row = event.target.closest("tr[data-id]");
  if (row) {
    openListingDetails(row.dataset.id);
  }
});
[
  elements.topScoreList,
  elements.topDealList,
  elements.topFreshList,
  elements.topBadList,
  elements.topCreditList
].forEach(container => {
  container.addEventListener("click", event => {
    const item = event.target.closest("[data-id]");
    if (item) {
      openListingDetails(item.dataset.id);
    }
  });
});
elements.compareChips.addEventListener("click", event => {
  const button = event.target.closest("[data-remove-id]");
  if (button) {
    toggleCompare(button.dataset.removeId);
  }
});
elements.favoritesList.addEventListener("click", event => {
  const removeButton = event.target.closest("[data-remove-favorite]");
  if (removeButton) {
    toggleFavorite(removeButton.dataset.removeFavorite);
    return;
  }

  const item = event.target.closest("[data-id]");
  if (item) {
    openListingDetails(item.dataset.id);
  }
});
elements.historyList.addEventListener("click", event => {
  const restoreButton = event.target.closest("[data-restore-history]");
  if (restoreButton) {
    restoreHistory(Number(restoreButton.dataset.restoreHistory));
  }
});
document.addEventListener("keydown", event => {
  if (event.key === "Escape" && !elements.detailModal.hidden) {
    closeListingDetails();
  } else if (event.key === "Escape" && !elements.compareModal.hidden) {
    closeCompareModal();
  } else if (!elements.detailModal.hidden && (event.key === "ArrowLeft" || event.key === "ArrowRight")) {
    const activeTag = document.activeElement?.tagName || "";
    if (["INPUT", "TEXTAREA", "SELECT"].includes(activeTag)) {
      return;
    }

    event.preventDefault();
    stepModalGallery(event.key === "ArrowRight" ? 1 : -1);
  }
});

populateCities();
populateImportFilters();
elements.kolesaUrlInput.dataset.manual = "false";
syncImportUrlPreview();
updateComparePanel();
Promise.all([loadAppState(), loadListingsFromServer()]).finally(() => {
  render();
});
