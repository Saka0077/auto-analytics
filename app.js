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

const IMPORT_MODELS_BY_MARK = {
  audi: [
    { value: "a6", label: "A6" },
    { value: "a4", label: "A4" },
    { value: "q5", label: "Q5" },
    { value: "q7", label: "Q7" }
  ],
  bmw: [
    { value: "5-series", label: "5 Series" },
    { value: "3-series", label: "3 Series" },
    { value: "x5", label: "X5" },
    { value: "x6", label: "X6" }
  ],
  chevrolet: [
    { value: "cobalt", label: "Cobalt" },
    { value: "cruze", label: "Cruze" },
    { value: "malibu", label: "Malibu" },
    { value: "onix", label: "Onix" },
    { value: "captiva", label: "Captiva" }
  ],
  daewoo: [
    { value: "nexia", label: "Nexia" },
    { value: "gentra", label: "Gentra" },
    { value: "matiz", label: "Matiz" }
  ],
  honda: [
    { value: "accord", label: "Accord" },
    { value: "civic", label: "Civic" },
    { value: "cr-v", label: "CR-V" }
  ],
  hyundai: [
    { value: "accent", label: "Accent" },
    { value: "elantra", label: "Elantra" },
    { value: "sonata", label: "Sonata" },
    { value: "tucson", label: "Tucson" },
    { value: "santa-fe", label: "Santa Fe" }
  ],
  infiniti: [
    { value: "fx35", label: "FX35" },
    { value: "qx56", label: "QX56" },
    { value: "qx80", label: "QX80" }
  ],
  kia: [
    { value: "rio", label: "Rio" },
    { value: "k5", label: "K5" },
    { value: "sportage", label: "Sportage" },
    { value: "cerato", label: "Cerato" },
    { value: "sorento", label: "Sorento" }
  ],
  lexus: [
    { value: "rx-350", label: "RX 350" },
    { value: "lx-570", label: "LX 570" },
    { value: "es-250", label: "ES 250" },
    { value: "gx-460", label: "GX 460" }
  ],
  mazda: [
    { value: "6", label: "Mazda 6" },
    { value: "cx-5", label: "CX-5" },
    { value: "cx-7", label: "CX-7" }
  ],
  "mercedes-benz": [
    { value: "e-class", label: "E-Class" },
    { value: "s-class", label: "S-Class" },
    { value: "c-class", label: "C-Class" },
    { value: "gle", label: "GLE" }
  ],
  mitsubishi: [
    { value: "outlander", label: "Outlander" },
    { value: "pajero", label: "Pajero" },
    { value: "montero-sport", label: "Montero Sport" }
  ],
  nissan: [
    { value: "almera", label: "Almera" },
    { value: "teana", label: "Teana" },
    { value: "patrol", label: "Patrol" },
    { value: "x-trail", label: "X-Trail" }
  ],
  opel: [
    { value: "vectra", label: "Vectra" },
    { value: "omega", label: "Omega" },
    { value: "astra", label: "Astra" }
  ],
  renault: [
    { value: "logan", label: "Logan" },
    { value: "duster", label: "Duster" },
    { value: "sandero", label: "Sandero" }
  ],
  subaru: [
    { value: "legacy", label: "Legacy" },
    { value: "outback", label: "Outback" },
    { value: "forester", label: "Forester" }
  ],
  toyota: [
    { value: "camry", label: "Camry" },
    { value: "corolla", label: "Corolla" },
    { value: "land-cruiser-prado", label: "Land Cruiser Prado" },
    { value: "land-cruiser", label: "Land Cruiser" },
    { value: "rav4", label: "RAV4" }
  ],
  volkswagen: [
    { value: "polo", label: "Polo" },
    { value: "passat", label: "Passat" },
    { value: "caddy", label: "Caddy" },
    { value: "touareg", label: "Touareg" }
  ],
  vaz: [
    { value: "2114", label: "2114" },
    { value: "priora", label: "Priora" },
    { value: "granta", label: "Granta" },
    { value: "vesta", label: "Vesta" },
    { value: "niva", label: "Niva" }
  ],
  gaz: [
    { value: "gazel", label: "Газель" },
    { value: "sobol", label: "Соболь" }
  ],
  skoda: [
    { value: "octavia", label: "Octavia" },
    { value: "superb", label: "Superb" },
    { value: "kodiaq", label: "Kodiaq" }
  ],
  ford: [
    { value: "focus", label: "Focus" },
    { value: "mondeo", label: "Mondeo" },
    { value: "explorer", label: "Explorer" }
  ],
  uaz: [
    { value: "patriot", label: "Patriot" },
    { value: "hunter", label: "Hunter" }
  ],
  changan: [
    { value: "cs55-plus", label: "CS55 Plus" },
    { value: "uni-k", label: "UNI-K" },
    { value: "alsvin", label: "Alsvin" }
  ],
  zaz: [
    { value: "chance", label: "Chance" },
    { value: "sens", label: "Sens" }
  ],
  chery: [
    { value: "tiggo-7-pro", label: "Tiggo 7 Pro" },
    { value: "tiggo-8-pro", label: "Tiggo 8 Pro" },
    { value: "tiggo-2", label: "Tiggo 2" }
  ],
  geely: [
    { value: "coolray", label: "Coolray" },
    { value: "atlas", label: "Atlas" },
    { value: "emgrand", label: "Emgrand" }
  ],
  ravon: [
    { value: "nexia-r3", label: "Nexia R3" },
    { value: "r4", label: "R4" },
    { value: "r2", label: "R2" }
  ],
  suzuki: [
    { value: "grand-vitara", label: "Grand Vitara" },
    { value: "sx4", label: "SX4" },
    { value: "jimny", label: "Jimny" }
  ],
  "ssang-yong": [
    { value: "kyron", label: "Kyron" },
    { value: "actyon", label: "Actyon" }
  ],
  jeep: [
    { value: "grand-cherokee", label: "Grand Cherokee" },
    { value: "cherokee", label: "Cherokee" }
  ],
  exeed: [
    { value: "txl", label: "TXL" },
    { value: "vx", label: "VX" }
  ],
  byd: [
    { value: "song-plus", label: "Song Plus" },
    { value: "han", label: "Han" },
    { value: "destroyer-05", label: "Destroyer 05" }
  ],
  gac: [
    { value: "gs8", label: "GS8" },
    { value: "empow", label: "Empow" }
  ],
  dodge: [
    { value: "charger", label: "Charger" },
    { value: "durango", label: "Durango" }
  ],
  faw: [
    { value: "bestune-b70", label: "Bestune B70" },
    { value: "bestune-t77", label: "Bestune T77" }
  ],
  jetour: [
    { value: "x70-plus", label: "X70 Plus" },
    { value: "dashing", label: "Dashing" }
  ],
  "land-rover": [
    { value: "range-rover-sport", label: "Range Rover Sport" },
    { value: "discovery", label: "Discovery" }
  ],
  saab: [
    { value: "9-5", label: "9-5" },
    { value: "9-3", label: "9-3" }
  ],
  cadillac: [
    { value: "escalade", label: "Escalade" },
    { value: "cts", label: "CTS" }
  ]
};

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

const HISTORY_BATCH_LIMIT = 400;
const SMART_SCOPE_THRESHOLD = 300;
const TABLE_PAGE_SIZE = 50;
let lazyImageObserver = null;

const state = {
  listings: defaultListings.map(normalizeRow),
  archivedListings: [],
  renderedListings: [],
  analysisMode: "buyer",
  currentPage: 1,
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
  showHiddenArchived: false,
  smartScopeBrand: "",
  smartScopeEnabled: true,
  activeProfileId: "default",
  profiles: [{ id: "default", name: "Основной" }],
  authToken: localStorage.getItem(AUTH_TOKEN_KEY) || "",
  currentUser: null,
  collectorStatus: null,
  listingInsights: {},
  listingGalleries: {},
  listingSnapshots: {},
  importModelOptionsCache: {},
  importModelRequestKey: "",
  importPreview: {
    status: "idle",
    url: "",
    availableCount: null,
    totalCount: null,
    exactTotalKnown: false,
    hasMore: false,
    note: ""
  }
};

const elements = {
  modeBuyerBtn: document.getElementById("mode-buyer-btn"),
  modeResellerBtn: document.getElementById("mode-reseller-btn"),
  modeCaption: document.getElementById("mode-caption"),
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
  maintenanceFilterSelect: document.getElementById("maintenance-filter-select"),
  optionSearchInput: document.getElementById("option-search-input"),
  sortSelect: document.getElementById("sort-select"),
  showInactiveToggle: document.getElementById("show-inactive-toggle"),
  importCitySelect: document.getElementById("import-city-select"),
  importMarkSelect: document.getElementById("import-mark-select"),
  importModelSelect: document.getElementById("import-model-select"),
  importBodySelect: document.getElementById("import-body-select"),
  importTransmissionSelect: document.getElementById("import-transmission-select"),
  importCustomSelect: document.getElementById("import-custom-select"),
  importNeedRepairSelect: document.getElementById("import-need-repair-select"),
  importPriceFromInput: document.getElementById("import-price-from-input"),
  importPriceToInput: document.getElementById("import-price-to-input"),
  kolesaUrlInput: document.getElementById("kolesa-url-input"),
  importLimitSelect: document.getElementById("import-limit-select"),
  importPreviewBox: document.getElementById("import-preview-box"),
  importPreviewStatus: document.getElementById("import-preview-status"),
  importPreviewNote: document.getElementById("import-preview-note"),
  importPreviewBtn: document.getElementById("import-preview-btn"),
  importKolesaBtn: document.getElementById("import-kolesa-btn"),
  importAktauBtn: document.getElementById("import-aktau-btn"),
  toggleSmartScopeBtn: document.getElementById("toggle-smart-scope-btn"),
  smartScopeBadge: document.getElementById("smart-scope-badge"),
  toggleHiddenBtn: document.getElementById("toggle-hidden-btn"),
  hiddenCountBadge: document.getElementById("hidden-count-badge"),
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
  collectHistoryBtn: document.getElementById("collect-history-btn"),
  collectAllHistoryBtn: document.getElementById("collect-all-history-btn"),
  pickBestCompareBtn: document.getElementById("pick-best-compare-btn"),
  openCompareBtn: document.getElementById("open-compare-btn"),
  clearCompareBtn: document.getElementById("clear-compare-btn"),
  bars: document.getElementById("bars"),
  resultsHead: document.getElementById("results-head"),
  resultsBody: document.getElementById("results-body"),
  resultsCount: document.getElementById("results-count"),
  resultsPageMeta: document.getElementById("results-page-meta"),
  resultsPrevBtn: document.getElementById("results-prev-btn"),
  resultsNextBtn: document.getElementById("results-next-btn"),
  bulkCheckBtn: document.getElementById("bulk-check-btn"),
  topScoreList: document.getElementById("top-score-list"),
  topDealList: document.getElementById("top-deal-list"),
  topFreshList: document.getElementById("top-fresh-list"),
  topBadList: document.getElementById("top-bad-list"),
  topCreditList: document.getElementById("top-credit-list"),
  topMaintenanceList: document.getElementById("top-maintenance-list"),
  kolesaBrandMeta: document.getElementById("kolesa-brand-meta"),
  kolesaBrandList: document.getElementById("kolesa-brand-list"),
  kolesaModelTitle: document.getElementById("kolesa-model-title"),
  kolesaModelMeta: document.getElementById("kolesa-model-meta"),
  kolesaModelList: document.getElementById("kolesa-model-list"),
  kolesaAktauMeta: document.getElementById("kolesa-aktau-meta"),
  kolesaAktauList: document.getElementById("kolesa-aktau-list"),
  archiveModelMeta: document.getElementById("archive-model-meta"),
  archiveModelList: document.getElementById("archive-model-list"),
  archiveSummaryMeta: document.getElementById("archive-summary-meta"),
  archiveSummaryList: document.getElementById("archive-summary-list"),
  archiveSellerMeta: document.getElementById("archive-seller-meta"),
  archiveSellerList: document.getElementById("archive-seller-list"),
  market30Meta: document.getElementById("market-30-meta"),
  market30PopularList: document.getElementById("market-30-popular-list"),
  market30FastMeta: document.getElementById("market-30-fast-meta"),
  market30FastList: document.getElementById("market-30-fast-list"),
  market30StuckMeta: document.getElementById("market-30-stuck-meta"),
  market30StuckList: document.getElementById("market-30-stuck-list"),
  market30DropMeta: document.getElementById("market-30-drop-meta"),
  market30DropList: document.getElementById("market-30-drop-list"),
  market30CityMeta: document.getElementById("market-30-city-meta"),
  market30CityList: document.getElementById("market-30-city-list"),
  archiveCityFilter: document.getElementById("archive-city-filter"),
  archiveBrandFilter: document.getElementById("archive-brand-filter"),
  archiveSellerFilter: document.getElementById("archive-seller-filter"),
  bestMetricLabel: document.getElementById("best-metric-label"),
  bestTitle: document.getElementById("best-title"),
  bestPrice: document.getElementById("best-price"),
  bestScoreLabel: document.getElementById("best-score-label"),
  bestScore: document.getElementById("best-score"),
  collectorLastRun: document.getElementById("collector-last-run"),
  collectorLastResult: document.getElementById("collector-last-result"),
  collectorLastMode: document.getElementById("collector-last-mode"),
  syncStatus: document.getElementById("sync-status"),
  sortScoreOption: document.getElementById("sort-score-option"),
  topScoreTitle: document.getElementById("top-score-title"),
  topDealTitle: document.getElementById("top-deal-title"),
  topBadTitle: document.getElementById("top-bad-title"),
  chartTitle: document.getElementById("chart-title"),
  resultsScoreLabel: document.getElementById("results-score-label"),
  detailModal: document.getElementById("detail-modal"),
  modalBackdrop: document.getElementById("modal-backdrop"),
  modalCloseBtn: document.getElementById("modal-close-btn"),
  modalMedia: document.getElementById("modal-media"),
  modalSource: document.getElementById("modal-source"),
  modalTitle: document.getElementById("modal-title"),
  modalPrice: document.getElementById("modal-price"),
  modalCity: document.getElementById("modal-city"),
  modalFacts: document.getElementById("modal-facts"),
  modalBreakdownTitle: document.getElementById("modal-breakdown-title"),
  modalBreakdown: document.getElementById("modal-breakdown"),
  modalSignals: document.getElementById("modal-signals"),
  modalMaintenance: document.getElementById("modal-maintenance"),
  modalPriceHistory: document.getElementById("modal-price-history"),
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
      return getPrimaryScore(item);
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
  return String(
    item.listing_uid ||
    item.listingUid ||
    item.advert_id ||
    item.advertId ||
    item.url ||
    [
      item.title || "",
      item.price || "",
      item.year || "",
      item.city || ""
    ].join("|")
  ).trim();
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

function parseMoneyInput(value) {
  const source = String(value ?? "").trim().toLowerCase().replace(/,/g, ".");
  if (!source) {
    return null;
  }

  const compact = source.replace(/\s+/g, "");
  let multiplier = 1;
  let numericText = compact;

  if (/(млн|million|kk|m)$/.test(compact)) {
    multiplier = 1000000;
    numericText = compact.replace(/(млн|million|kk|m)$/g, "");
  } else if (/(тыс|thousand|k)$/.test(compact)) {
    multiplier = 1000;
    numericText = compact.replace(/(тыс|thousand|k)$/g, "");
  }

  if (!numericText) {
    return null;
  }

  const normalizedNumericText = multiplier === 1
    ? numericText.replace(/[^\d]/g, "")
    : numericText.replace(/[^\d.]/g, "");

  if (!normalizedNumericText) {
    return null;
  }

  const parsed = Number(normalizedNumericText);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return Math.round(parsed * multiplier);
}

function formatMoneyInputValue(value) {
  const parsed = parseMoneyInput(value);
  return parsed ? formatInteger(parsed) : "";
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

function nonNegativeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
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

function serializeRowForServer(item) {
  return {
    title: item.title,
    price: item.price,
    year: item.year,
    mileage: item.mileage,
    owners: item.owners,
    city: item.city,
    url: item.url,
    image: item.image,
    photo_gallery: item.photoGallery,
    description: item.description,
    source: item.source,
    brand: item.brand,
    model: item.model,
    fuel_type: item.fuelType,
    transmission: item.transmission,
    body_type: item.bodyType,
    drive_type: item.driveType,
    steering_side: item.steeringSide,
    color: item.color,
    options: item.options,
    vin: item.vin,
    vin_note: item.vinNote,
    repair_state: item.repairState,
    advert_id: item.advertId,
    listing_uid: item.listingUid,
    engine_volume: item.engineVolume,
    publication_date: item.publicationDate,
    last_update: item.lastUpdate,
    first_seen_at: item.firstSeenAt,
    last_seen_at: item.lastSeenAt,
    last_checked_at: item.lastCheckedAt,
    last_status_change_at: item.lastStatusChangeAt,
    actuality_status: item.actualityStatus,
    hidden_after_import: item.hiddenAfterImport,
    hidden_reason: item.hiddenReason,
    hidden_at: item.hiddenAt,
    import_batch_id: item.importBatchId,
    import_scope_label: item.importScopeLabel,
    archived_at: item.archivedAt,
    archive_reason: item.archiveReason,
    photo_count: item.photoCount,
    phone_count: item.phoneCount,
    phone_prefix: item.phonePrefix,
    credit_available: item.creditAvailable,
    paid_services: item.paidServices,
    credit_monthly_payment: item.creditMonthlyPayment,
    credit_down_payment: item.creditDownPayment,
    seller_user_id: item.sellerUserId,
    seller_type_id: item.sellerTypeId,
    is_verified_dealer: item.isVerifiedDealer,
    is_used_car_dealer: item.isUsedCarDealer,
    public_history_available: item.publicHistoryAvailable,
    history_summary: item.historySummary,
    risk_score: item.riskScore,
    risk_flags: item.riskFlags,
    avg_price: item.avgPrice,
    market_difference: item.marketDifference,
    market_difference_percent: item.marketDifferencePercent
  };
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

function normalizeAutopartsProfile(profile) {
  if (!profile || typeof profile !== "object") {
    return null;
  }

  const normalizedProfile = {
    id: String(profile.id || "").trim(),
    modelLabel: String(profile.model_label ?? profile.modelLabel ?? "").trim(),
    brand: String(profile.brand || "").trim(),
    primaryModel: String(profile.primary_model ?? profile.primaryModel ?? "").trim(),
    modelAliases: Array.isArray(profile.model_aliases ?? profile.modelAliases)
      ? (profile.model_aliases ?? profile.modelAliases).map(value => String(value || "").trim()).filter(Boolean)
      : [],
    segment: String(profile.segment || "").trim(),
    years: String(profile.years || "").trim(),
    frontPadsPriceKzt: optionalPositiveNumber(profile.front_pads_price_kzt ?? profile.frontPadsPriceKzt),
    frontPadsStock: optionalPositiveNumber(profile.front_pads_stock ?? profile.frontPadsStock),
    frontDiscPriceKzt: optionalPositiveNumber(profile.front_disc_price_kzt ?? profile.frontDiscPriceKzt),
    frontDiscStock: optionalPositiveNumber(profile.front_disc_stock ?? profile.frontDiscStock),
    serviceBasketKzt: optionalPositiveNumber(profile.service_basket_kzt ?? profile.serviceBasketKzt),
    avgStock: number(profile.avg_stock ?? profile.avgStock),
    priceScore: number(profile.price_score ?? profile.priceScore),
    cheapnessScore: number(profile.cheapness_score ?? profile.cheapnessScore),
    priority: optionalPositiveNumber(profile.priority),
    maintenanceBand: String(profile.maintenance_band ?? profile.maintenanceBand ?? "unknown").trim(),
    maintenanceLabel: String(profile.maintenance_label ?? profile.maintenanceLabel ?? "").trim(),
    comment: String(profile.comment || "").trim(),
    marketSourceUrl: String(profile.market_source_url ?? profile.marketSourceUrl ?? "").trim(),
    padsSourceUrl: String(profile.pads_source_url ?? profile.padsSourceUrl ?? "").trim(),
    discSourceUrl: String(profile.disc_source_url ?? profile.discSourceUrl ?? "").trim(),
    matchScore: number(profile.match_score ?? profile.matchScore),
    matchReason: String(profile.match_reason ?? profile.matchReason ?? "").trim(),
    matchConfidence: String(profile.match_confidence ?? profile.matchConfidence ?? "none").trim(),
    matchConfidenceLabel: String(profile.match_confidence_label ?? profile.matchConfidenceLabel ?? "").trim(),
    modelTokenMatch: booleanValue(profile.model_token_match ?? profile.modelTokenMatch)
  };

  const coverageSignals = [
    Number.isFinite(normalizedProfile.cheapnessScore) || Number.isFinite(normalizedProfile.priceScore),
    Number.isFinite(normalizedProfile.serviceBasketKzt),
    Number.isFinite(normalizedProfile.avgStock) || Number.isFinite(normalizedProfile.frontPadsStock) || Number.isFinite(normalizedProfile.frontDiscStock),
    Number.isFinite(normalizedProfile.frontPadsPriceKzt) || Number.isFinite(normalizedProfile.frontDiscPriceKzt)
  ];
  const coverageCount = coverageSignals.filter(Boolean).length;
  const coverageRatio = coverageSignals.length ? coverageCount / coverageSignals.length : 0;

  let coverageLabel = "Нет данных";
  let coverageLevel = "missing";
  if (coverageCount >= 4) {
    coverageLabel = "Полные данные";
    coverageLevel = "full";
  } else if (coverageCount >= 2) {
    coverageLabel = "Частичные данные";
    coverageLevel = "partial";
  } else if (coverageCount >= 1) {
    coverageLabel = "Базовые данные";
    coverageLevel = "basic";
  }

  return {
    ...normalizedProfile,
    coverageLevel,
    coverageLabel,
    coverageRatio: Number(coverageRatio.toFixed(2))
  };
}

function normalizeSellerAnalysis(analysis) {
  if (!analysis || typeof analysis !== "object") {
    return null;
  }

  return {
    sellerUserId: String(analysis.seller_user_id ?? analysis.sellerUserId ?? "").trim(),
    profileType: String(analysis.profile_type ?? analysis.profileType ?? "").trim(),
    profileLabel: String(analysis.profile_label ?? analysis.profileLabel ?? "").trim(),
    traderScore: nonNegativeNumber(analysis.trader_score ?? analysis.traderScore),
    totalListingCount: nonNegativeNumber(analysis.total_listing_count ?? analysis.totalListingCount),
    activeListingCount: nonNegativeNumber(analysis.active_listing_count ?? analysis.activeListingCount),
    brandCount: nonNegativeNumber(analysis.brand_count ?? analysis.brandCount),
    modelCount: nonNegativeNumber(analysis.model_count ?? analysis.modelCount),
    cityCount: nonNegativeNumber(analysis.city_count ?? analysis.cityCount),
    dominantBrand: String(analysis.dominant_brand ?? analysis.dominantBrand ?? "").trim(),
    dominantBrandCount: nonNegativeNumber(analysis.dominant_brand_count ?? analysis.dominantBrandCount),
    dominantModel: String(analysis.dominant_model ?? analysis.dominantModel ?? "").trim(),
    dominantModelCount: nonNegativeNumber(analysis.dominant_model_count ?? analysis.dominantModelCount),
    belowMarketCount: nonNegativeNumber(analysis.below_market_count ?? analysis.belowMarketCount),
    staleCount: nonNegativeNumber(analysis.stale_count ?? analysis.staleCount),
    promotedCount: nonNegativeNumber(analysis.promoted_count ?? analysis.promotedCount),
    relistedCount: nonNegativeNumber(analysis.relisted_count ?? analysis.relistedCount),
    averageRisk: number(analysis.average_risk ?? analysis.averageRisk),
    note: String(analysis.note || "").trim()
  };
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
    listingUid: item.listing_uid || item.listingUid || createListingId(item),
    hiddenAfterImport: Boolean(item.hidden_after_import ?? item.hiddenAfterImport),
    hiddenReason: item.hidden_reason || item.hiddenReason || "",
    hiddenAt: item.hidden_at || item.hiddenAt || "",
    importBatchId: item.import_batch_id || item.importBatchId || "",
    importScopeLabel: item.import_scope_label || item.importScopeLabel || "",
    archivedAt: item.archived_at || item.archivedAt || "",
    archiveReason: item.archive_reason || item.archiveReason || "",
    snapshotCount: nonNegativeNumber(item.snapshot_count ?? item.snapshotCount),
    firstPrice: optionalPositiveNumber(item.first_price ?? item.firstPrice),
    lastPrice: optionalPositiveNumber(item.last_price ?? item.lastPrice),
    priceChangeCount: nonNegativeNumber(item.price_change_count ?? item.priceChangeCount),
    priceDropTotal: nonNegativeNumber(item.price_drop_total ?? item.priceDropTotal),
    lastPriceChangeAt: item.last_price_change_at || item.lastPriceChangeAt || "",
    statusChangeCount: nonNegativeNumber(item.status_change_count ?? item.statusChangeCount),
    wasRelisted: booleanValue(item.was_relisted ?? item.wasRelisted),
    daysOnMarket: nonNegativeNumber(item.days_on_market ?? item.daysOnMarket),
    historyFirstSeenAt: item.history_first_seen_at || item.historyFirstSeenAt || "",
    historyLastSeenAt: item.history_last_seen_at || item.historyLastSeenAt || "",
    historyAvailable: booleanValue(item.history_available ?? item.historyAvailable),
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
    sellerAnalysis: normalizeSellerAnalysis(item.seller_analysis ?? item.sellerAnalysis),
    publicHistoryAvailable: booleanValue(item.public_history_available ?? item.publicHistoryAvailable),
    historySummary: item.history_summary || item.historySummary || "",
    riskScore: number(item.risk_score ?? item.riskScore),
    riskFlags: Array.isArray(item.risk_flags ?? item.riskFlags)
      ? (item.risk_flags ?? item.riskFlags).map(value => String(value || "").trim()).filter(Boolean)
      : [],
    avgPrice: optionalPositiveNumber(item.avg_price ?? item.avgPrice),
    marketDifference: number(item.market_difference ?? item.marketDifference),
    marketDifferencePercent: number(item.market_difference_percent ?? item.marketDifferencePercent),
    autopartsProfile: normalizeAutopartsProfile(item.autoparts_profile ?? item.autopartsProfile)
  };
}

function normalizeRows(rows) {
  return rows.map(normalizeRow).filter(item => item.price > 0);
}

function getArchiveAwareListings() {
  return state.showHiddenArchived
    ? [...state.listings, ...state.archivedListings]
    : state.listings;
}

function getFilterPoolListings() {
  return getArchiveAwareListings().filter(item => state.showHiddenArchived || !item.hiddenAfterImport);
}

function chooseSmartScopeBrand() {
  const brands = [...new Set(
    state.listings
      .filter(item => isKolesaListing(item) && !item.hiddenAfterImport && item.brand)
      .map(item => item.brand)
  )];

  if (brands.length === 0) {
    state.smartScopeBrand = "";
    return;
  }

  const randomIndex = Math.floor(Math.random() * brands.length);
  state.smartScopeBrand = brands[randomIndex];
}

function hasExplicitPrimaryFilters() {
  return Boolean(
    elements.searchInput.value.trim() ||
    elements.citySelect.value ||
    elements.markFilterSelect.value ||
    elements.modelFilterSelect.value
  );
}

function shouldApplySmartScope() {
  return state.smartScopeEnabled &&
    Boolean(state.smartScopeBrand) &&
    state.listings.length > SMART_SCOPE_THRESHOLD &&
    !hasExplicitPrimaryFilters();
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

function normalizeMarketLookupPart(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-zа-я0-9]+/gi, " ")
    .trim();
}

function buildMarketKey(parts) {
  const normalized = parts.map(normalizeMarketLookupPart).filter(Boolean);
  return normalized.length ? normalized.join("::") : "";
}

function formatDaysOnMarket(value) {
  return Number.isFinite(Number(value)) ? `${formatInteger(value)} дн.` : "-";
}

function getListingSnapshotCacheKey(item) {
  return item?.advertId || item?.url || item?.id || "";
}

function getListingSnapshotRows(item) {
  const key = getListingSnapshotCacheKey(item);
  const cached = state.listingSnapshots[key];
  return Array.isArray(cached?.items) ? cached.items : [];
}

function formatSnapshotEventDate(value) {
  if (!value) {
    return "Дата неизвестна";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Дата неизвестна";
  }
  return date.toLocaleString("ru-RU", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit"
  });
}

function getSnapshotPriceDelta(snapshot, previousSnapshot) {
  if (!previousSnapshot || !Number.isFinite(snapshot?.price) || !Number.isFinite(previousSnapshot?.price)) {
    return null;
  }

  const delta = snapshot.price - previousSnapshot.price;
  if (!delta) {
    return null;
  }

  return {
    value: Math.abs(delta),
    direction: delta < 0 ? "down" : "up",
    label: delta < 0 ? `-${formatPrice(Math.abs(delta))}` : `+${formatPrice(Math.abs(delta))}`
  };
}

function normalizeAnalyticsText(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/ё/g, "е")
    .replace(/\s+/g, " ");
}

function isKolesaListing(item) {
  return normalizeAnalyticsText(item?.source).includes("kolesa");
}

function isAktauCity(value) {
  const normalized = normalizeAnalyticsText(value);
  return normalized === "актау" || normalized === "aktau" || normalized === "aqtau";
}

function getBrandAnalyticsLabel(item) {
  return item.brand || String(item.title || "").split(/\s+/).filter(Boolean)[0] || "";
}

function getModelAnalyticsLabel(item) {
  const label = [item.brand, item.model].filter(Boolean).join(" ").trim();
  return label || item.title || "";
}

function buildAnalyticsGroups(items, getLabel, limit = 10) {
  const groups = new Map();

  items.forEach(item => {
    const label = String(getLabel(item) || "").trim();
    const key = normalizeAnalyticsText(label);
    if (!key) {
      return;
    }

    if (!groups.has(key)) {
      groups.set(key, {
        label,
        count: 0,
        totalPrice: 0,
        priceCount: 0
      });
    }

    const group = groups.get(key);
    group.count += 1;
    if (Number.isFinite(item.price)) {
      group.totalPrice += item.price;
      group.priceCount += 1;
    }
  });

  return [...groups.values()]
    .map(group => ({
      ...group,
      averagePrice: group.priceCount ? group.totalPrice / group.priceCount : null
    }))
    .sort((left, right) => right.count - left.count || (left.averagePrice ?? Infinity) - (right.averagePrice ?? Infinity) || left.label.localeCompare(right.label, "ru"))
    .slice(0, limit);
}

function getAnalysisModeConfig() {
  return state.analysisMode === "reseller"
    ? {
        scoreKey: "resellerOpportunityScore",
        metricLabel: "Лучший вариант для перепродажи",
        scoreLabel: "Перекуп",
        sortLabel: "Лучший для перекупа",
        topScoreTitle: "Топ 5 для перепродажи",
        topDealTitle: "Топ 5 ликвидных",
        topBadTitle: "Слабые для перепродажи",
        chartTitle: "Рейтинг перекупа",
        caption: "Режим перекупа: ищем связку из маржи, ликвидности и низкого риска вложений.",
        breakdownTitle: "Почему это интересно для перепродажи",
        topSecondaryKey: "liquidityScore",
        topSecondaryLabel: "ликвидность"
      }
    : {
        scoreKey: "buyerScore",
        metricLabel: "Лучший вариант для покупки",
        scoreLabel: "Покупка",
        sortLabel: "Лучший для покупки",
        topScoreTitle: "Топ 5 для покупки",
        topDealTitle: "Топ 5 выгодных",
        topBadTitle: "Плохие объявления",
        chartTitle: "Рейтинг покупки",
        caption: "Режим покупки: ищем лучший вариант для себя по рынку, риску и качеству объявления.",
        breakdownTitle: "Почему это хороший вариант",
        topSecondaryKey: "dealScore",
        topSecondaryLabel: "выгода"
      };
}

function getPrimaryScore(item) {
  const { scoreKey } = getAnalysisModeConfig();
  return Number(item?.[scoreKey] ?? item?.score ?? 0);
}

function updateAnalysisModeUI() {
  const config = getAnalysisModeConfig();
  elements.modeBuyerBtn?.classList.toggle("is-active", state.analysisMode === "buyer");
  elements.modeResellerBtn?.classList.toggle("is-active", state.analysisMode === "reseller");
  if (elements.modeCaption) {
    elements.modeCaption.textContent = config.caption;
  }
  if (elements.bestMetricLabel) {
    elements.bestMetricLabel.textContent = config.metricLabel;
  }
  if (elements.bestScoreLabel) {
    elements.bestScoreLabel.textContent = config.scoreLabel;
  }
  if (elements.sortScoreOption) {
    elements.sortScoreOption.textContent = config.sortLabel;
  }
  if (elements.topScoreTitle) {
    elements.topScoreTitle.textContent = config.topScoreTitle;
  }
  if (elements.topDealTitle) {
    elements.topDealTitle.textContent = config.topDealTitle;
  }
  if (elements.topBadTitle) {
    elements.topBadTitle.textContent = config.topBadTitle;
  }
  if (elements.chartTitle) {
    elements.chartTitle.textContent = config.chartTitle;
  }
  if (elements.resultsScoreLabel) {
    elements.resultsScoreLabel.textContent = config.scoreLabel;
  }
  if (elements.modalBreakdownTitle) {
    elements.modalBreakdownTitle.textContent = config.breakdownTitle;
  }
}

function setAnalysisMode(mode) {
  const nextMode = mode === "reseller" ? "reseller" : "buyer";
  if (state.analysisMode === nextMode) {
    return;
  }
  state.analysisMode = nextMode;
  updateAnalysisModeUI();
  render();
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

  if (!item.driveType || !item.steeringSide) {
    return true;
  }

  if (item.creditAvailable && (!item.creditMonthlyPayment || !item.creditDownPayment)) {
    return true;
  }

  if (item.photoCount && item.photoGallery.length < item.photoCount) {
    return true;
  }

  if (!item.phoneCount) {
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

function getHistorySignalMeta(item) {
  const signals = [];
  const snapshotCount = Number(item?.snapshotCount || 0);
  const daysOnMarket = Number(item?.daysOnMarket || 0);
  const priceChangeCount = Number(item?.priceChangeCount || 0);
  const priceDropTotal = Number(item?.priceDropTotal || 0);

  if (priceChangeCount > 0 && priceDropTotal > 0) {
    signals.push({ label: "Цена падала", className: "status-badge status-badge--history" });
  }

  if (daysOnMarket >= 21) {
    signals.push({ label: "Висит долго", className: "status-badge status-badge--stale" });
  }

  if (snapshotCount >= 3 && daysOnMarket >= 14 && priceChangeCount === 0) {
    signals.push({ label: "Цена не двигается", className: "status-badge status-badge--thin" });
  }

  if (item?.wasRelisted) {
    signals.push({ label: "Переопубликовано", className: "status-badge status-badge--history" });
  }

  return signals;
}

function getHistorySummaryLabel(item) {
  const signals = getHistorySignalMeta(item);
  if (signals.length) {
    return signals.map(signal => signal.label).join(", ");
  }
  if (Number(item?.daysOnMarket || 0) <= 3) {
    return "Свежее объявление";
  }
  return "Без сильных сигналов";
}

function getMarketBadge(item) {
  if (!Number.isFinite(item.marketDifferencePercent)) {
    return item.marketConfidence === "insufficient" ? "Мало рынка" : "";
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

function hasTrustedAutopartsProfile(item) {
  const confidence = String(item?.autopartsProfile?.matchConfidence || "").trim();
  return ["exact", "generation_match", "model_only"].includes(confidence);
}

function getAutopartsBadgeMeta(item) {
  const profile = hasTrustedAutopartsProfile(item) ? item?.autopartsProfile : null;
  if (!profile) {
    return null;
  }

  switch (profile.maintenanceBand) {
    case "cheap":
      return { label: "Деш. запчасти", className: "status-badge status-badge--parts-cheap" };
    case "medium":
      return { label: "Средний ремонт", className: "status-badge status-badge--parts-medium" };
    case "expensive":
      return { label: "Дорогой ремонт", className: "status-badge status-badge--parts-expensive" };
    default:
      return null;
  }
}

function getBuyerDecisionMeta(item) {
  const score = Number(item?.buyerScore ?? item?.score ?? 0);
  const risk = Number.isFinite(item?.riskScore) ? item.riskScore / 100 : 0.35;
  const sellerSignal = Number.isFinite(item?.sellerSignalScore) ? item.sellerSignalScore : 0.35;
  const marketDifference = Number.isFinite(item?.marketDifferencePercent) ? item.marketDifferencePercent : 0;
  const age = daysSince(item?.publicationDate || item?.lastUpdate || item?.lastCheckedAt);
  const isStale = age !== null && age >= 30;
  const longOnMarket = Number(item?.daysOnMarket || 0) >= 21;
  const priceDropped = Number(item?.priceChangeCount || 0) > 0 && Number(item?.priceDropTotal || 0) > 0;
  const staticPrice = Number(item?.snapshotCount || 0) >= 3 && Number(item?.daysOnMarket || 0) >= 14 && Number(item?.priceChangeCount || 0) === 0;
  const marketWeak = item?.marketConfidence === "insufficient";
  const hasAutoparts = hasTrustedAutopartsProfile(item);
  const maintenanceScore = Number(item?.maintenanceScore ?? 0.5);
  const maintenanceCoverage = item?.autopartsProfile?.coverageLevel || "missing";
  const maintenanceWeak = hasAutoparts && maintenanceScore <= 0.42;
  const maintenancePartial = hasAutoparts && maintenanceCoverage !== "full";

  if (maintenanceWeak && score < 0.56) {
    return {
      label: "Не брать",
      className: "status-badge status-badge--skip",
      note: maintenanceCoverage === "full"
        ? "обслуживание выглядит дорогим, и это портит сделку даже без других рисков"
        : "по запчастям уже есть дорогие сигналы, а данных пока недостаточно для уверенной покупки"
    };
  }

  if (
    score >= 0.74 &&
    risk <= 0.35 &&
    marketDifference >= -6 &&
    sellerSignal <= 0.55 &&
    !isStale &&
    !maintenanceWeak &&
    !maintenancePartial
  ) {
    return {
      label: "Брать",
      className: "status-badge status-badge--buy",
      note: marketDifference >= 0
        ? "цена выглядит в рынке или ниже рынка, риск низкий"
        : marketWeak
          ? "данных рынка мало, но по продавцу, риску и свежести вариант всё равно сильный"
          : "риск низкий, а карточка выглядит уверенно"
    };
  }

  if (longOnMarket && !priceDropped && (score < 0.66 || staticPrice)) {
    return {
      label: "Осторожно",
      className: "status-badge status-badge--careful",
      note: "объявление висит долго и цена почти не двигается, поэтому вариант лучше перепроверить"
    };
  }

  if (score >= 0.55 && risk <= 0.62 && marketDifference >= -14 && sellerSignal <= 0.78) {
    return {
      label: "Осторожно",
      className: "status-badge status-badge--careful",
      note: maintenanceWeak
        ? "вариант рабочий, но обслуживание выглядит дорогим и может съесть выгоду"
        : maintenancePartial
          ? "по запчастям данные пока неполные, поэтому покупку лучше перепроверить"
          : longOnMarket && priceDropped
            ? "цена уже падала, но объявление всё ещё висит долго"
            : marketWeak
              ? "данных рынка мало, поэтому покупку лучше проверить руками"
          : isStale
            ? "объявление висит давно, проверь историю цены и состояние"
            : "вариант рабочий, но цену и детали лучше перепроверить"
    };
  }

  return {
    label: "Не брать",
    className: "status-badge status-badge--skip",
    note: maintenanceWeak
      ? "цена обслуживания и другие риски вместе делают покупку слабой"
      : marketDifference <= -12
        ? "цена выше рынка или риск по объявлению слишком высокий"
        : "слишком много красных флагов по продавцу, риску или свежести"
  };
}

function getResellerDecisionMeta(item) {
  const score = Number(item?.resellerOpportunityScore ?? 0);
  const liquidity = Number(item?.liquidityScore ?? 0);
  const deal = Number(item?.dealScore ?? 0);
  const risk = Number.isFinite(item?.riskScore) ? item.riskScore / 100 : 0.35;
  const sellerSignal = Number.isFinite(item?.sellerSignalScore) ? item.sellerSignalScore : 0.35;
  const marketDifference = Number.isFinite(item?.marketDifferencePercent) ? item.marketDifferencePercent : 0;
  const age = daysSince(item?.publicationDate || item?.lastUpdate || item?.lastCheckedAt);
  const isStale = age !== null && age >= 21;
  const longOnMarket = Number(item?.daysOnMarket || 0) >= 21;
  const priceDropped = Number(item?.priceChangeCount || 0) > 0 && Number(item?.priceDropTotal || 0) > 0;
  const staticPrice = Number(item?.snapshotCount || 0) >= 3 && Number(item?.daysOnMarket || 0) >= 14 && Number(item?.priceChangeCount || 0) === 0;
  const marketWeak = item?.marketConfidence === "insufficient";
  const hasAutoparts = hasTrustedAutopartsProfile(item);
  const maintenanceScore = Number(item?.maintenanceScore ?? 0.5);
  const maintenanceCoverage = item?.autopartsProfile?.coverageLevel || "missing";
  const maintenanceWeak = hasAutoparts && maintenanceScore <= 0.4;
  const maintenancePartial = hasAutoparts && maintenanceCoverage !== "full";

  if (maintenanceWeak && deal < 0.58) {
    return {
      label: "Риск",
      className: "status-badge status-badge--risk",
      note: "дорогой ремонт или слабая база по запчастям съедает потенциальную маржу"
    };
  }

  if (
    score >= 0.72 &&
    liquidity >= 0.58 &&
    deal >= 0.58 &&
    marketDifference >= 5 &&
    risk <= 0.45 &&
    sellerSignal <= 0.68 &&
    !maintenanceWeak &&
    !maintenancePartial
  ) {
    return {
      label: "Есть маржа",
      className: "status-badge status-badge--profit",
      note: marketWeak
        ? "рынок посчитан грубо, но по ликвидности и риску вариант выглядит живым"
        : "есть дисконт к рынку, нормальная ликвидность и умеренный риск вложений"
    };
  }

  if (longOnMarket && !priceDropped && (deal < 0.64 || staticPrice)) {
    return {
      label: "Слабая маржа",
      className: "status-badge status-badge--thin",
      note: "объявление висит долго без движения цены, значит вход есть, а выход уже выглядит слабее"
    };
  }

  if (
    score >= 0.5 &&
    liquidity >= 0.42 &&
    deal >= 0.45 &&
    marketDifference >= -5 &&
    risk <= 0.68
  ) {
    return {
      label: "Слабая маржа",
      className: "status-badge status-badge--thin",
      note: maintenanceWeak
        ? "запчасти выглядят дорогими, поэтому прибыль легко потерять на ремонте"
        : maintenancePartial
          ? "по запчастям база неполная, поэтому маржу лучше считать с запасом"
          : longOnMarket && priceDropped
            ? "цена уже падала, но объявление всё ещё не ушло, поэтому маржа выглядит тоньше"
            : marketWeak
              ? "данных рынка мало, поэтому маржу лучше считать с запасом"
          : isStale
            ? "можно смотреть только после сильного торга или проверки истории"
            : "сделка возможна, но запас по прибыли пока слабый"
    };
  }

  return {
    label: "Риск",
    className: "status-badge status-badge--risk",
    note: maintenanceWeak
      ? "дорогие запчасти и риски по объявлению делают перепродажу слабой"
      : marketDifference < 0
        ? "цена уже выше рынка или ликвидность слишком слабая для перепродажи"
        : "риск вложений и зависания в продаже слишком высокий"
  };
}

function getCurrentDecisionMeta(item) {
  return state.analysisMode === "reseller"
    ? getResellerDecisionMeta(item)
    : getBuyerDecisionMeta(item);
}

function renderListingBadges(item) {
  const decision = getCurrentDecisionMeta(item);
  const badges = [
    renderActualityBadge(item),
    `<span class="${decision.className}">${escapeHtml(decision.label)}</span>`
  ];
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

  const autopartsBadge = getAutopartsBadgeMeta(item);
  if (autopartsBadge) {
    badges.push(`<span class="${autopartsBadge.className}">${escapeHtml(autopartsBadge.label)}</span>`);
  }

  const sellerLabel = getSellerLabel(item);
  if (sellerLabel && sellerLabel !== "Продавец") {
    badges.push(`<span class="status-badge status-badge--seller">${escapeHtml(sellerLabel)}</span>`);
  }

  getHistorySignalMeta(item).slice(0, 2).forEach(signal => {
    badges.push(`<span class="${signal.className}">${escapeHtml(signal.label)}</span>`);
  });

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
  if (item.sellerAnalysis?.profileLabel) {
    return item.sellerAnalysis.profileLabel;
  }

  if (item.isVerifiedDealer || item.isUsedCarDealer) {
    return "Дилер";
  }

  if (item.sellerUserId || item.sellerTypeId) {
    return "Продавец";
  }

  return "-";
}

function getSellerProfileSummary(item) {
  const analysis = item?.sellerAnalysis;
  if (!analysis) {
    return {
      label: getSellerLabel(item),
      total: null,
      active: null,
      traderScore: null,
      note: ""
    };
  }

  return {
    label: analysis.profileLabel || getSellerLabel(item),
    total: Number.isFinite(analysis.totalListingCount) ? analysis.totalListingCount : null,
    active: Number.isFinite(analysis.activeListingCount) ? analysis.activeListingCount : null,
    traderScore: Number.isFinite(analysis.traderScore) ? analysis.traderScore : null,
    note: analysis.note || ""
  };
}

function setStatus(text) {
  elements.syncStatus.textContent = text;
}

function wait(ms) {
  return new Promise(resolve => window.setTimeout(resolve, ms));
}

async function waitForServerJob(jobId, { onProgress } = {}) {
  const normalizedJobId = String(jobId || "").trim();
  if (!normalizedJobId) {
    throw new Error("Не удалось запустить задачу.");
  }

  while (true) {
    const response = await fetch(`/api/jobs/${encodeURIComponent(normalizedJobId)}`);
    const payload = await response.json();
    if (!response.ok || !payload.job) {
      throw new Error(payload.error || "Не удалось получить статус задачи.");
    }

    const job = payload.job;
    if (typeof onProgress === "function") {
      onProgress(job);
    }

    if (job.status === "completed") {
      return job;
    }

    if (job.status === "paused") {
      throw new Error(job.error || job.message || "Задача поставлена на паузу.");
    }

    if (job.status === "failed") {
      throw new Error(job.error || "Задача завершилась с ошибкой.");
    }

    await wait(1500);
  }
}

function setImportBusy(isBusy) {
  elements.importKolesaBtn.disabled = isBusy;
  elements.importAktauBtn.disabled = isBusy;
  elements.importPreviewBtn.disabled = isBusy;
  elements.importLimitSelect.disabled = isBusy;
  elements.kolesaUrlInput.disabled = isBusy;
  elements.importCitySelect.disabled = isBusy;
  elements.importMarkSelect.disabled = isBusy;
  elements.importModelSelect.disabled = isBusy || !elements.importMarkSelect.value;
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
  const previewCap = state.importPreview.availableCount && !state.importPreview.hasMore
    ? state.importPreview.availableCount
    : 1000;
  if (!Number.isFinite(value) || value <= 0) {
    return 100;
  }
  return Math.min(Math.max(Math.round(value), 1), Math.max(1, previewCap));
}

function renderImportPreview() {
  if (!elements.importPreviewStatus || !elements.importPreviewNote) {
    return;
  }

  const preview = state.importPreview;
  elements.importPreviewBox?.classList.remove("is-loading", "is-ready", "is-error");
  if (elements.importPreviewBtn) {
    elements.importPreviewBtn.disabled = preview.status === "loading";
    elements.importPreviewBtn.textContent = preview.status === "loading"
      ? "Считаю..."
      : "Показать сколько авто";
  }

  if (preview.status === "loading") {
    elements.importPreviewBox?.classList.add("is-loading");
    elements.importPreviewStatus.textContent = "Считаю...";
    elements.importPreviewNote.textContent = "Запрашиваю точное количество с первой страницы Kolesa.";
    return;
  }

  if (preview.status === "error") {
    elements.importPreviewBox?.classList.add("is-error");
    elements.importPreviewStatus.textContent = "Не удалось получить";
    elements.importPreviewNote.textContent = preview.note || "Kolesa сейчас не ответил. Можно импортировать вручную по ссылке.";
    return;
  }

  if (preview.status === "ready" && Number.isFinite(preview.availableCount)) {
    elements.importPreviewBox?.classList.add("is-ready");
    const displayCount = preview.exactTotalKnown && Number.isFinite(preview.totalCount)
      ? preview.totalCount
      : preview.availableCount;
    const totalText = preview.exactTotalKnown
      ? `${formatInteger(displayCount)} авто`
      : preview.hasMore
        ? `${formatInteger(preview.availableCount)}+ авто`
        : `${formatInteger(preview.availableCount)} авто`;
    elements.importPreviewStatus.textContent = totalText;
    const baseNote = preview.exactTotalKnown
      ? preview.totalCount > 1000
        ? `По текущему поиску найдено ${formatInteger(preview.totalCount)} объявлений. За один раз импортируем до 1000, чтобы не грузить Kolesa.`
        : `По текущему поиску найдено ${formatInteger(preview.totalCount)} объявлений. Теперь выбери, сколько импортировать.`
      : preview.hasMore
        ? `По текущему поиску найдено больше ${formatInteger(preview.availableCount)} объявлений. Выбери, сколько импортировать сейчас.`
        : `По текущему поиску найдено ${formatInteger(preview.availableCount)} объявлений. Теперь выбери, сколько импортировать.`;
    elements.importPreviewNote.textContent = preview.note
      ? `${baseNote} ${preview.note}`
      : baseNote;
    return;
  }

  elements.importPreviewStatus.textContent = "Пока не проверено";
  elements.importPreviewNote.textContent = "Измени фильтры и нажми кнопку, чтобы посчитать объявления без лишней нагрузки.";
}

function markImportPreviewDirty() {
    state.importPreview = {
      status: "idle",
      url: elements.kolesaUrlInput.value.trim() || buildImportUrlFromFilters(),
      availableCount: null,
      totalCount: null,
      exactTotalKnown: false,
      hasMore: false,
      note: ""
    };
  elements.importLimitSelect.max = "1000";
  renderImportPreview();
}

async function loadImportPreview(url) {
  const trimmedUrl = String(url || "").trim();
  if (!trimmedUrl) {
    state.importPreview = {
      status: "idle",
      url: "",
      availableCount: null,
      totalCount: null,
      exactTotalKnown: false,
      hasMore: false,
      note: ""
    };
    renderImportPreview();
    return;
  }

  state.importPreview = {
    status: "loading",
    url: trimmedUrl,
    availableCount: null,
    totalCount: null,
    exactTotalKnown: false,
    hasMore: false,
    note: ""
  };
  renderImportPreview();

  try {
    const response = await fetch("/api/import/kolesa/preview", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ url: trimmedUrl })
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Preview failed");
    }

    if (state.importPreview.url !== trimmedUrl) {
      return;
    }

    state.importPreview = {
      status: "ready",
      url: trimmedUrl,
      availableCount: Number(payload.availableCount) || 0,
      totalCount: Number.isFinite(Number(payload.totalCount)) ? Number(payload.totalCount) : null,
      exactTotalKnown: Boolean(payload.exactTotalKnown),
      hasMore: Boolean(payload.hasMore),
      note: ""
    };

    if (!state.importPreview.hasMore) {
      const maxAvailable = Math.max(1, state.importPreview.availableCount);
      if (Number(elements.importLimitSelect.value) > maxAvailable) {
        elements.importLimitSelect.value = String(maxAvailable);
      }
      elements.importLimitSelect.max = String(maxAvailable);
    } else {
      elements.importLimitSelect.max = "1000";
    }

    renderImportPreview();
  } catch (error) {
    if (state.importPreview.url !== trimmedUrl) {
      return;
    }

    state.importPreview = {
      status: "error",
      url: trimmedUrl,
      availableCount: null,
      totalCount: null,
      exactTotalKnown: false,
      hasMore: false,
      note: error.message || "Не удалось получить количество объявлений."
    };
    elements.importLimitSelect.max = "1000";
    renderImportPreview();
  }
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
  fillSelectOptions(elements.importModelSelect, [{ value: "", label: "Сначала выбери марку" }], "");
  elements.importModelSelect.disabled = true;
  fillSelectOptions(elements.importBodySelect, IMPORT_BODIES, "");
  fillSelectOptions(elements.importTransmissionSelect, IMPORT_TRANSMISSIONS, "");
}

async function loadImportModelOptions(selectedValue = "") {
  const mark = elements.importMarkSelect.value;
  const city = elements.importCitySelect.value;
  const requestKey = `${city}::${mark}`;

  if (!mark) {
    fillSelectOptions(elements.importModelSelect, [{ value: "", label: "Сначала выбери марку" }], "");
    elements.importModelSelect.disabled = true;
    return;
  }

  const cached = state.importModelOptionsCache[requestKey];
  if (cached) {
    fillSelectOptions(
      elements.importModelSelect,
      [{ value: "", label: cached.length ? "Все модели" : "Нет моделей на Kolesa" }, ...cached],
      selectedValue
    );
    elements.importModelSelect.disabled = false;
    return;
  }

  state.importModelRequestKey = requestKey;
  fillSelectOptions(elements.importModelSelect, [{ value: "", label: "Загрузка моделей..." }], "");
  elements.importModelSelect.disabled = true;

  try {
    const response = await fetch("/api/import/kolesa/models", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ city, mark })
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Models load failed");
    }
    if (state.importModelRequestKey !== requestKey) {
      return;
    }

    const items = Array.isArray(payload.items) ? payload.items : [];
    state.importModelOptionsCache[requestKey] = items;
    fillSelectOptions(
      elements.importModelSelect,
      [{ value: "", label: items.length ? "Все модели" : "Нет моделей на Kolesa" }, ...items],
      selectedValue
    );
    elements.importModelSelect.disabled = false;
  } catch (error) {
    if (state.importModelRequestKey !== requestKey) {
      return;
    }
    fillSelectOptions(
      elements.importModelSelect,
      [{ value: "", label: error.message || "Не удалось загрузить модели" }],
      ""
    );
    elements.importModelSelect.disabled = true;
  }
}

function syncImportUrlPreview() {
  if (elements.kolesaUrlInput.dataset.manual === "true") {
    return;
  }
  elements.kolesaUrlInput.value = buildImportUrlFromFilters();
}

function handleImportPriceFocus(event) {
  const parsed = parseMoneyInput(event.target.value);
  event.target.value = parsed ? String(parsed) : "";
}

function handleImportPriceBlur(event) {
  event.target.value = formatMoneyInputValue(event.target.value);
}

function requestImportPreview() {
  const url = elements.kolesaUrlInput.value.trim() || buildImportUrlFromFilters();
  void loadImportPreview(url);
}

function buildImportUrlFromFilters() {
  const city = elements.importCitySelect.value;
  const mark = elements.importMarkSelect.value;
  const model = elements.importModelSelect.value;
  const body = elements.importBodySelect.value;
  const transmission = elements.importTransmissionSelect.value;
  const custom = elements.importCustomSelect.value;
  const needRepair = elements.importNeedRepairSelect.value;
  const priceFrom = parseMoneyInput(elements.importPriceFromInput.value);
  const priceTo = parseMoneyInput(elements.importPriceToInput.value);

  let url = "https://kolesa.kz/cars/";
  if (city) {
    url += `${city}/`;
  }
  if (mark) {
    url += `${mark}/`;
  }
  if (model) {
    url += `${model}/`;
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

function buildDerivedMarketMap(listings) {
  const byCity = new Map();
  const byModel = new Map();

  listings.forEach(item => {
    if (!item || !Number.isFinite(item.price) || item.price <= 0 || !item.brand || !item.model) {
      return;
    }

    const modelKey = buildMarketKey([item.brand, item.model]);
    const cityKey = buildMarketKey([item.brand, item.model, item.city]);

    if (modelKey) {
      const bucket = byModel.get(modelKey) || [];
      bucket.push(item);
      byModel.set(modelKey, bucket);
    }

    if (cityKey) {
      const bucket = byCity.get(cityKey) || [];
      bucket.push(item);
      byCity.set(cityKey, bucket);
    }
  });

  const derived = new Map();

  listings.forEach(item => {
    if (!item || !Number.isFinite(item.price) || item.price <= 0 || !item.brand || !item.model) {
      derived.set(item.id, {
        avgPrice: item.avgPrice ?? null,
        marketDifference: item.marketDifference ?? null,
        marketDifferencePercent: item.marketDifferencePercent ?? null,
        marketScope: item.avgPrice ? "kolesa" : "insufficient",
        marketSampleSize: null,
        marketSourceLabel: item.avgPrice ? "Kolesa" : "",
        marketConfidence: item.avgPrice ? "provider" : "insufficient"
      });
      return;
    }

    const candidates = [
      {
        key: buildMarketKey([item.brand, item.model, item.city]),
        store: byCity,
        minSize: 3,
        scope: "brand_model_city",
        sourceLabel: "Локальный рынок"
      },
      {
        key: buildMarketKey([item.brand, item.model]),
        store: byModel,
        minSize: 5,
        scope: "brand_model",
        sourceLabel: "Рынок модели"
      }
    ];

    let resolved = null;
    candidates.some(candidate => {
      if (!candidate.key) {
        return false;
      }
      const bucket = (candidate.store.get(candidate.key) || []).filter(other => Number.isFinite(other.price) && other.price > 0);
      if (bucket.length < candidate.minSize) {
        return false;
      }
      const pool = bucket.filter(other => other.id !== item.id);
      if (!pool.length) {
        return false;
      }

      const avgPrice = pool.reduce((sum, other) => sum + other.price, 0) / pool.length;
      const marketDifference = avgPrice - item.price;
      resolved = {
        avgPrice: Math.round(avgPrice),
        marketDifference: Math.round(marketDifference),
        marketDifferencePercent: avgPrice ? Number(((marketDifference / avgPrice) * 100).toFixed(2)) : null,
        marketScope: candidate.scope,
        marketSampleSize: bucket.length,
        marketSourceLabel: candidate.sourceLabel,
        marketConfidence: "local"
      };
      return true;
    });

    if (!resolved) {
      resolved = item.avgPrice
        ? {
            avgPrice: item.avgPrice,
            marketDifference: item.marketDifference ?? (item.avgPrice - item.price),
            marketDifferencePercent: Number.isFinite(item.marketDifferencePercent)
              ? item.marketDifferencePercent
              : item.avgPrice
                ? Number((((item.marketDifference ?? (item.avgPrice - item.price)) / item.avgPrice) * 100).toFixed(2))
                : null,
            marketScope: "kolesa",
            marketSampleSize: null,
            marketSourceLabel: "Kolesa",
            marketConfidence: "provider"
          }
        : {
            avgPrice: null,
            marketDifference: null,
            marketDifferencePercent: null,
            marketScope: "insufficient",
            marketSampleSize: null,
            marketSourceLabel: "",
            marketConfidence: "insufficient"
          };
    }

    derived.set(item.id, resolved);
  });

  return derived;
}

function scoreListings(listings) {
  const derivedMarketMap = buildDerivedMarketMap(listings);
  const prices = listings.map(item => item.price).filter(Boolean);
  const years = listings.map(item => item.year).filter(item => item !== null && item !== undefined);
  const mileages = listings.map(item => item.mileage).filter(item => item !== null && item !== undefined);
  const owners = listings.map(item => item.owners).filter(item => item !== null && item !== undefined);
  const photoCounts = listings.map(item => item.photoCount).filter(item => item !== null && item !== undefined);
  const risks = listings.map(item => item.riskScore).filter(item => item !== null && item !== undefined);
  const monthlyPayments = listings.map(item => item.creditMonthlyPayment).filter(item => item !== null && item !== undefined);
  const maintenanceCheapnessValues = listings
    .map(item => hasTrustedAutopartsProfile(item) ? item.autopartsProfile?.cheapnessScore : null)
    .filter(item => item !== null && item !== undefined && Number.isFinite(Number(item)));
  const maintenanceBasketValues = listings
    .map(item => hasTrustedAutopartsProfile(item) ? item.autopartsProfile?.serviceBasketKzt : null)
    .filter(item => item !== null && item !== undefined && Number.isFinite(Number(item)));
  const maintenanceStockValues = listings
    .map(item => hasTrustedAutopartsProfile(item) ? (item.autopartsProfile?.avgStock ?? item.autopartsProfile?.frontPadsStock) : null)
    .filter(item => item !== null && item !== undefined && Number.isFinite(Number(item)));
  const maintenancePadsValues = listings
    .map(item => hasTrustedAutopartsProfile(item) ? item.autopartsProfile?.frontPadsPriceKzt : null)
    .filter(item => item !== null && item !== undefined && Number.isFinite(Number(item)));
  const maintenanceDiscValues = listings
    .map(item => hasTrustedAutopartsProfile(item) ? item.autopartsProfile?.frontDiscPriceKzt : null)
    .filter(item => item !== null && item !== undefined && Number.isFinite(Number(item)));
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
    maintenanceCheapnessMin: maintenanceCheapnessValues.length ? Math.min(...maintenanceCheapnessValues) : null,
    maintenanceCheapnessMax: maintenanceCheapnessValues.length ? Math.max(...maintenanceCheapnessValues) : null,
    maintenanceBasketMin: maintenanceBasketValues.length ? Math.min(...maintenanceBasketValues) : null,
    maintenanceBasketMax: maintenanceBasketValues.length ? Math.max(...maintenanceBasketValues) : null,
    maintenanceStockMin: maintenanceStockValues.length ? Math.min(...maintenanceStockValues) : null,
    maintenanceStockMax: maintenanceStockValues.length ? Math.max(...maintenanceStockValues) : null,
    maintenancePadsMin: maintenancePadsValues.length ? Math.min(...maintenancePadsValues) : null,
    maintenancePadsMax: maintenancePadsValues.length ? Math.max(...maintenancePadsValues) : null,
    maintenanceDiscMin: maintenanceDiscValues.length ? Math.min(...maintenanceDiscValues) : null,
    maintenanceDiscMax: maintenanceDiscValues.length ? Math.max(...maintenanceDiscValues) : null,
    freshMin: freshnessValues.length ? Math.min(...freshnessValues) : null,
    freshMax: freshnessValues.length ? Math.max(...freshnessValues) : null
  };

  return listings.map(item => {
    const marketReference = derivedMarketMap.get(item.id) || {
      avgPrice: item.avgPrice ?? null,
      marketDifference: item.marketDifference ?? null,
      marketDifferencePercent: item.marketDifferencePercent ?? null,
      marketScope: item.avgPrice ? "kolesa" : "insufficient",
      marketSampleSize: null,
      marketSourceLabel: item.avgPrice ? "Kolesa" : "",
      marketConfidence: item.avgPrice ? "provider" : "insufficient"
    };
    const trustedAutopartsProfile = hasTrustedAutopartsProfile(item) ? item.autopartsProfile : null;
    const priceScore = normalize(item.price, bounds.priceMin, bounds.priceMax, true);
    const yearScore = normalize(item.year, bounds.yearMin, bounds.yearMax);
    const mileageScore = normalize(item.mileage, bounds.mileageMin, bounds.mileageMax, true);
    const ownersScore = normalize(item.owners, bounds.ownersMin, bounds.ownersMax, true);
    const freshAge = daysSince(getListingDateMeta(item).value);
    const freshnessScore = normalize(freshAge === null ? null : Math.min(freshAge, 60), bounds.freshMin, bounds.freshMax, true);
    const photoScore = normalize(item.photoCount, bounds.photoMin, bounds.photoMax);
    const riskSafetyScore = normalize(item.riskScore, bounds.riskMin, bounds.riskMax, true);
    const marketScore = Number.isFinite(marketReference.marketDifferencePercent)
      ? Math.max(0, Math.min(1, (marketReference.marketDifferencePercent + 15) / 30))
      : 0.5;
    const maintenanceCheapnessScore = trustedAutopartsProfile && Number.isFinite(trustedAutopartsProfile.cheapnessScore)
      ? normalize(trustedAutopartsProfile.cheapnessScore, bounds.maintenanceCheapnessMin, bounds.maintenanceCheapnessMax)
      : null;
    const maintenanceBasketScore = trustedAutopartsProfile && Number.isFinite(trustedAutopartsProfile.serviceBasketKzt)
      ? normalize(trustedAutopartsProfile.serviceBasketKzt, bounds.maintenanceBasketMin, bounds.maintenanceBasketMax, true)
      : null;
    const maintenanceStockValue = trustedAutopartsProfile?.avgStock ?? trustedAutopartsProfile?.frontPadsStock;
    const maintenanceStockScore = trustedAutopartsProfile && Number.isFinite(maintenanceStockValue)
      ? normalize(maintenanceStockValue, bounds.maintenanceStockMin, bounds.maintenanceStockMax)
      : null;
    const maintenancePadScore = trustedAutopartsProfile && Number.isFinite(trustedAutopartsProfile.frontPadsPriceKzt)
      ? normalize(trustedAutopartsProfile.frontPadsPriceKzt, bounds.maintenancePadsMin, bounds.maintenancePadsMax, true)
      : null;
    const maintenanceDiscScore = trustedAutopartsProfile && Number.isFinite(trustedAutopartsProfile.frontDiscPriceKzt)
      ? normalize(trustedAutopartsProfile.frontDiscPriceKzt, bounds.maintenanceDiscMin, bounds.maintenanceDiscMax, true)
      : null;
    const maintenancePartsScores = [maintenancePadScore, maintenanceDiscScore].filter(score => score !== null);
    const maintenancePartsScore = maintenancePartsScores.length
      ? maintenancePartsScores.reduce((sum, score) => sum + score, 0) / maintenancePartsScores.length
      : null;
    const maintenanceComponents = [];
    if (maintenanceCheapnessScore !== null) {
      maintenanceComponents.push({ score: maintenanceCheapnessScore, weight: 0.45 });
    }
    if (maintenanceBasketScore !== null) {
      maintenanceComponents.push({ score: maintenanceBasketScore, weight: 0.25 });
    }
    if (maintenanceStockScore !== null) {
      maintenanceComponents.push({ score: maintenanceStockScore, weight: 0.15 });
    }
    if (maintenancePartsScore !== null) {
      maintenanceComponents.push({ score: maintenancePartsScore, weight: 0.15 });
    }
    const maintenanceBaseScore = maintenanceComponents.length
      ? maintenanceComponents.reduce((sum, component) => sum + component.score * component.weight, 0) /
        maintenanceComponents.reduce((sum, component) => sum + component.weight, 0)
      : 0.5;
    const maintenanceCoverageFactor = trustedAutopartsProfile
      ? 0.7 + (trustedAutopartsProfile.coverageRatio ?? 0) * 0.3
      : 1;
    const maintenanceScore = Math.max(0, Math.min(1, maintenanceBaseScore * maintenanceCoverageFactor));
    const sellerPenalty = item.isUsedCarDealer ? 0.2 : 0;
    const sellerBaseSignal = Math.max(
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
    const sellerAnalysis = item.sellerAnalysis;
    const sellerProfileSignal = sellerAnalysis
      ? Math.max(
          0,
          Math.min(
            1,
            (sellerAnalysis.profileType === "dealer"
              ? 0.62
              : sellerAnalysis.profileType === "reseller"
                ? 0.54
                : sellerAnalysis.profileType === "multi_listing"
                  ? 0.38
                  : 0.14) +
            Math.min(Number(sellerAnalysis.traderScore || 0) / 100, 1) * 0.18 +
            ((sellerAnalysis.totalListingCount || 0) >= 6 ? 0.08 : (sellerAnalysis.totalListingCount || 0) >= 3 ? 0.04 : 0) +
            ((sellerAnalysis.brandCount || 0) >= 3 ? 0.05 : 0) +
            ((sellerAnalysis.cityCount || 0) >= 2 ? 0.04 : 0) -
            (sellerAnalysis.profileType === "private" && (sellerAnalysis.totalListingCount || 0) <= 2 ? 0.06 : 0)
          )
        )
      : null;
    const sellerSignalScore = sellerProfileSignal === null
      ? sellerBaseSignal
      : sellerAnalysis.profileType === "private" && (sellerAnalysis.totalListingCount || 0) <= 2 && !item.isUsedCarDealer
        ? Math.max(0, Math.min(1, sellerBaseSignal * 0.82))
        : Math.max(sellerBaseSignal, Math.min(1, sellerBaseSignal * 0.7 + sellerProfileSignal * 0.3));
    const sellerTrustScore = Math.max(0, 1 - sellerSignalScore);
    const priceMileageScore =
      priceScore * 0.5 +
      mileageScore * 0.5;
    const qualityScore =
      yearScore * 0.2 +
      mileageScore * 0.18 +
      ownersScore * 0.07 +
      freshnessScore * 0.1 +
      photoScore * 0.08 +
      riskSafetyScore * 0.2 +
      sellerTrustScore * 0.07 +
      maintenanceScore * 0.1;
    const valueScore =
      priceScore * 0.43 +
      marketScore * 0.29 +
      freshnessScore * 0.08 +
      sellerTrustScore * 0.08 +
      maintenanceScore * 0.12;
    const score =
      valueScore * 0.56 +
      qualityScore * 0.44 -
      sellerPenalty * 0.35;
    const buyerScore = Math.max(0, Math.min(1, score));
    const dealScore =
      priceScore * 0.34 +
      marketScore * 0.29 +
      freshnessScore * 0.08 +
      yearScore * 0.09 +
      mileageScore * 0.05 +
      maintenanceScore * 0.15;
    const liquidityScore =
      marketScore * 0.26 +
      freshnessScore * 0.22 +
      photoScore * 0.1 +
      yearScore * 0.13 +
      mileageScore * 0.09 +
      riskSafetyScore * 0.1 +
      maintenanceScore * 0.1;
    const creditScore = item.creditAvailable && item.creditMonthlyPayment
      ? (
          normalize(item.creditMonthlyPayment, bounds.monthlyPaymentMin, bounds.monthlyPaymentMax, true) * 0.55 +
          marketScore * 0.2 +
          freshnessScore * 0.1 +
          riskSafetyScore * 0.1 +
          yearScore * 0.05
        )
      : 0;
    const resellerOpportunityScore = Math.max(
      0,
      Math.min(
        1,
        dealScore * 0.34 +
        liquidityScore * 0.26 +
        marketScore * 0.18 +
        freshnessScore * 0.08 +
        riskSafetyScore * 0.08 +
        priceScore * 0.06 -
        (item.riskScore !== null ? item.riskScore / 100 : 0.35) * 0.1 -
        sellerSignalScore * 0.04
      )
    );
    const badScore =
      (item.riskScore !== null ? item.riskScore / 100 : 0.35) * 0.45 +
      sellerSignalScore * 0.2 +
      (photoScore ? 1 - photoScore : 0.4) * 0.15 +
      (Number.isFinite(marketReference.marketDifferencePercent) && marketReference.marketDifferencePercent < 0
        ? Math.min(Math.abs(marketReference.marketDifferencePercent) / 20, 1)
        : 0) * 0.2;

    return {
      ...item,
      avgPrice: marketReference.avgPrice,
      marketDifference: marketReference.marketDifference,
      marketDifferencePercent: marketReference.marketDifferencePercent,
      marketScope: marketReference.marketScope,
      marketSampleSize: marketReference.marketSampleSize,
      marketSourceLabel: marketReference.marketSourceLabel,
      marketConfidence: marketReference.marketConfidence,
      autopartsProfile: trustedAutopartsProfile,
      score: Number(buyerScore.toFixed(4)),
      buyerScore: Number(buyerScore.toFixed(4)),
      resellerOpportunityScore: Number(resellerOpportunityScore.toFixed(4)),
      maintenanceScore: Number(maintenanceScore.toFixed(4)),
      priceMileageScore: Number(priceMileageScore.toFixed(4)),
      qualityScore: Number(qualityScore.toFixed(4)),
      valueScore: Number(valueScore.toFixed(4)),
      dealScore: Number(dealScore.toFixed(4)),
      liquidityScore: Number(liquidityScore.toFixed(4)),
      creditScore: Number(creditScore.toFixed(4)),
      freshnessScore: Number(freshnessScore.toFixed(4)),
      sellerSignalScore: Number(sellerSignalScore.toFixed(4)),
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
        maintenance: Number(maintenanceScore.toFixed(4)),
        photos: Number(photoScore.toFixed(4)),
        seller: Number(sellerTrustScore.toFixed(4))
      }
    };
  });
}

function populateCities() {
  const current = elements.citySelect.value;
  const sourceListings = getFilterPoolListings();
  const cities = [...new Set(sourceListings.map(item => item.city).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
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
  const sourceListings = getFilterPoolListings();
  const marks = [...new Set(sourceListings.map(item => item.brand).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
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
  const sourceListings = getFilterPoolListings();
  const models = [...new Set(
    sourceListings
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
  const sourceListings = getFilterPoolListings();
  const fuels = [...new Set(sourceListings.map(item => item.fuelType).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const transmissions = [...new Set(sourceListings.map(item => item.transmission).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const bodyTypes = [...new Set(sourceListings.map(item => item.bodyType).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const drives = [...new Set(sourceListings.map(item => item.driveType).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const steerings = [...new Set(sourceListings.map(item => item.steeringSide).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const colors = [...new Set(sourceListings.map(item => item.color).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));

  populateAttributeSelect(elements.fuelFilterSelect, fuels, "Любое");
  populateAttributeSelect(elements.transmissionFilterSelect, transmissions, "Любая");
  populateAttributeSelect(elements.bodyFilterSelect, bodyTypes, "Любой");
  populateAttributeSelect(elements.driveFilterSelect, drives, "Любой");
  populateAttributeSelect(elements.steeringFilterSelect, steerings, "Любой");
  populateAttributeSelect(elements.colorFilterSelect, colors, "Любой");
}

function getFilteredListings() {
  const { scoreKey } = getAnalysisModeConfig();
  const sourceListings = getArchiveAwareListings();
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
  const maintenance = elements.maintenanceFilterSelect.value;
  const optionSearch = elements.optionSearchInput.value.trim().toLowerCase();
  const sort = elements.sortSelect.value;
  const showInactive = elements.showInactiveToggle.checked;
  const smartScopeBrand = shouldApplySmartScope() ? state.smartScopeBrand : "";

  const results = scoreListings(sourceListings).filter(item => {
    const hiddenMatch = state.showHiddenArchived || !item.hiddenAfterImport;
    const smartScopeMatch = !smartScopeBrand || normalizeAnalyticsText(item.brand) === normalizeAnalyticsText(smartScopeBrand);
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
    const maintenanceMatch =
      !maintenance ||
      (maintenance === "known"
        ? hasTrustedAutopartsProfile(item)
        : hasTrustedAutopartsProfile(item) && item.autopartsProfile?.maintenanceBand === maintenance);
    const optionMatch = !optionSearch || item.options.some(option => option.toLowerCase().includes(optionSearch));
    return smartScopeMatch && hiddenMatch && actualityMatch && titleMatch && yearMatch && priceMatch && mileageMatch && cityMatch && markMatch && modelMatch && creditMatch && monthlyPaymentMatch && repairMatch && fuelMatch && transmissionMatch && bodyTypeMatch && driveMatch && steeringMatch && colorMatch && sellerMatch && maintenanceMatch && optionMatch;
  });

  switch (sort) {
    case "credit-desc":
      results.sort((a, b) => b.creditScore - a.creditScore || getPrimaryScore(b) - getPrimaryScore(a) || a.creditMonthlyPayment - b.creditMonthlyPayment);
      break;
    case "liquidity-desc":
      results.sort((a, b) => b.liquidityScore - a.liquidityScore || getPrimaryScore(b) - getPrimaryScore(a));
      break;
    case "maintenance-desc":
      results.sort((a, b) => (b.maintenanceScore ?? 0) - (a.maintenanceScore ?? 0) || getPrimaryScore(b) - getPrimaryScore(a) || a.price - b.price);
      break;
    case "fresh-desc":
      results.sort((a, b) => b.freshnessScore - a.freshnessScore || getPrimaryScore(b) - getPrimaryScore(a));
      break;
    case "risk-asc":
      results.sort((a, b) => (a.riskScore ?? 50) - (b.riskScore ?? 50) || getPrimaryScore(b) - getPrimaryScore(a));
      break;
    case "price-mileage-desc":
      results.sort((a, b) =>
        (b.priceMileageScore ?? 0) - (a.priceMileageScore ?? 0) ||
        a.price - b.price ||
        (a.mileage ?? Number.MAX_SAFE_INTEGER) - (b.mileage ?? Number.MAX_SAFE_INTEGER)
      );
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
      results.sort((a, b) => (b[scoreKey] ?? b.score ?? 0) - (a[scoreKey] ?? a.score ?? 0) || a.price - b.price);
      break;
  }

  return applyTableSort(results);
}

function renderStats(listings) {
  const config = getAnalysisModeConfig();
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
  const best = [...listings].sort((a, b) => getPrimaryScore(b) - getPrimaryScore(a))[0];

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
  elements.bestScore.textContent = formatScore(best[config.scoreKey] ?? best.score);
}

function renderAnalyticsList(target, items, emptyText) {
  target.innerHTML = "";

  if (!items.length) {
    target.innerHTML = `<div class="analytics-empty">${escapeHtml(emptyText)}</div>`;
    return;
  }

  items.forEach(item => {
    const row = document.createElement("div");
    row.className = "analytics-item";
    row.innerHTML = `
      <div>
        <div class="analytics-item-title">${escapeHtml(item.label)}</div>
        <div class="analytics-item-meta">${formatInteger(item.count)} объявл.${item.averagePrice ? ` · ср. цена ${escapeHtml(formatPrice(item.averagePrice))}` : ""}</div>
      </div>
      <div class="analytics-item-value">${formatInteger(item.count)}</div>
    `;
    target.append(row);
  });
}

function renderArchiveSummaryList(target, items, emptyText) {
  target.innerHTML = "";

  if (!items.length) {
    target.innerHTML = `<div class="analytics-empty">${escapeHtml(emptyText)}</div>`;
    return;
  }

  items.forEach(item => {
    const row = document.createElement("div");
    row.className = "analytics-item analytics-item--summary";
    row.innerHTML = `
      <div>
        <div class="analytics-item-title">${escapeHtml(item.label)}</div>
        <div class="analytics-item-meta">${escapeHtml(item.meta || "")}</div>
      </div>
      <div class="analytics-item-value">${escapeHtml(item.value || "0")}</div>
    `;
    target.append(row);
  });
}

function renderArchiveSellerList(target, items, emptyText) {
  target.innerHTML = "";

  if (!items.length) {
    target.innerHTML = `<div class="analytics-empty">${escapeHtml(emptyText)}</div>`;
    return;
  }

  items.forEach(item => {
    const row = document.createElement("div");
    row.className = "analytics-item";
    row.innerHTML = `
      <div>
        <div class="analytics-item-title">${escapeHtml(item.label)}</div>
        <div class="analytics-item-meta">${escapeHtml(item.meta || "")}</div>
      </div>
      <div class="analytics-item-value">${escapeHtml(item.value || "0")}</div>
    `;
    target.append(row);
  });
}

function populateArchiveFilterSelect(element, values, placeholder) {
  if (!element) {
    return;
  }
  const current = element.value;
  element.innerHTML = "";
  const defaultOption = document.createElement("option");
  defaultOption.value = "";
  defaultOption.textContent = placeholder;
  element.append(defaultOption);

  values.forEach(value => {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = value;
    element.append(option);
  });

  if ([...element.options].some(option => option.value === current)) {
    element.value = current;
  } else {
    element.value = "";
  }
}

function populateArchiveFilters() {
  const archived = state.archivedListings.filter(isKolesaListing);
  const cities = [...new Set(archived.map(item => item.city).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const brands = [...new Set(archived.map(item => item.brand).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));
  const sellers = [...new Set(archived.map(item => getSellerProfileLabel(item)).filter(Boolean))].sort((a, b) => a.localeCompare(b, "ru"));

  populateArchiveFilterSelect(elements.archiveCityFilter, cities, "Все города");
  populateArchiveFilterSelect(elements.archiveBrandFilter, brands, "Все марки");
  populateArchiveFilterSelect(elements.archiveSellerFilter, sellers, "Все продавцы");
}

function renderKolesaAnalytics(listings) {
  const allKolesaListings = state.listings.filter(isKolesaListing);
  const filteredKolesaListings = listings.filter(isKolesaListing);
  const selectedCity = elements.citySelect.value;
  const cityScopedListings = selectedCity
    ? allKolesaListings.filter(item => normalizeAnalyticsText(item.city) === normalizeAnalyticsText(selectedCity))
    : filteredKolesaListings;
  const aktauListings = allKolesaListings.filter(item => isAktauCity(item.city));

  const topBrands = buildAnalyticsGroups(filteredKolesaListings, getBrandAnalyticsLabel, 10);
  const topModels = buildAnalyticsGroups(cityScopedListings, getModelAnalyticsLabel, 10);
  const topAktauModels = buildAnalyticsGroups(aktauListings, getModelAnalyticsLabel, 10);

  elements.kolesaBrandMeta.textContent = filteredKolesaListings.length
    ? `${formatInteger(filteredKolesaListings.length)} объявл. Kolesa в текущей выборке.`
    : "По текущим фильтрам объявлений Kolesa нет.";
  elements.kolesaModelTitle.textContent = selectedCity
    ? `Топ моделей: ${selectedCity}`
    : "Топ моделей Kolesa";
  elements.kolesaModelMeta.textContent = selectedCity
    ? `${formatInteger(cityScopedListings.length)} объявл. Kolesa по городу ${selectedCity}.`
    : `${formatInteger(cityScopedListings.length)} объявл. Kolesa по текущей выборке.`;
  elements.kolesaAktauMeta.textContent = aktauListings.length
    ? `${formatInteger(aktauListings.length)} объявл. Kolesa из Актау в базе.`
    : "Актау пока не загружен. Сначала импортируй этот город.";

  renderAnalyticsList(elements.kolesaBrandList, topBrands, "Нет импортированных объявлений Kolesa по текущим фильтрам.");
  renderAnalyticsList(elements.kolesaModelList, topModels, selectedCity ? `По городу ${selectedCity} пока нет данных Kolesa.` : "Нет данных Kolesa по текущей выборке.");
  renderAnalyticsList(elements.kolesaAktauList, topAktauModels, "По Актау пока нет объявлений Kolesa. Сначала импортируй Актау.");
}

function renderArchiveAnalytics() {
  const allArchived = state.archivedListings.filter(isKolesaListing);
  const selectedCity = elements.archiveCityFilter?.value || "";
  const selectedBrand = elements.archiveBrandFilter?.value || "";
  const selectedSeller = elements.archiveSellerFilter?.value || "";
  const scopedArchived = allArchived.filter(item => {
    const cityMatch = !selectedCity || normalizeAnalyticsText(item.city) === normalizeAnalyticsText(selectedCity);
    const brandMatch = !selectedBrand || normalizeAnalyticsText(item.brand) === normalizeAnalyticsText(selectedBrand);
    const sellerMatch = !selectedSeller || normalizeAnalyticsText(getSellerProfileLabel(item)) === normalizeAnalyticsText(selectedSeller);
    return cityMatch && brandMatch && sellerMatch;
  });

  const modelGroups = new Map();
  scopedArchived.forEach(item => {
    const label = getModelAnalyticsLabel(item);
    const key = normalizeAnalyticsText(label);
    if (!key) {
      return;
    }
    if (!modelGroups.has(key)) {
      modelGroups.set(key, {
        label,
        count: 0,
        totalDays: 0,
        dayCount: 0,
        fastCount: 0,
        stuckCount: 0
      });
    }
    const entry = modelGroups.get(key);
    const days = Number(item.daysOnMarket || 0);
    entry.count += 1;
    if (days > 0) {
      entry.totalDays += days;
      entry.dayCount += 1;
      if (days <= 7) {
        entry.fastCount += 1;
      }
      if (days >= 21 && Number(item.priceDropTotal || 0) <= 0) {
        entry.stuckCount += 1;
      }
    }
  });
  const archivedTopModels = [...modelGroups.values()]
    .map(item => ({
      label: item.label,
      count: item.count,
      averageDays: item.dayCount ? Math.round(item.totalDays / item.dayCount) : 0,
      fastCount: item.fastCount,
      stuckCount: item.stuckCount
    }))
    .sort((left, right) => right.count - left.count || left.averageDays - right.averageDays || left.label.localeCompare(right.label, "ru"))
    .slice(0, 10);
  const withPriceDrops = scopedArchived.filter(item => item.priceDropTotal > 0 || item.priceChangeCount > 0);
  const longRunning = scopedArchived.filter(item => item.daysOnMarket >= 14);
  const relisted = scopedArchived.filter(item => item.wasRelisted);
  const stalePrice = scopedArchived.filter(item => item.daysOnMarket >= 14 && item.priceDropTotal <= 0);
  const avgDays = scopedArchived.length
    ? Math.round(scopedArchived.reduce((sum, item) => sum + Number(item.daysOnMarket || 0), 0) / scopedArchived.length)
    : 0;
  const avgDrop = withPriceDrops.length
    ? Math.round(withPriceDrops.reduce((sum, item) => sum + Number(item.priceDropTotal || 0), 0) / withPriceDrops.length)
    : 0;
  const archiveFilterLabel = [selectedCity, selectedBrand, selectedSeller].filter(Boolean).join(" / ");

  const sellerGroups = new Map();
  scopedArchived.forEach(item => {
    const analysis = item.sellerAnalysis;
    const label = analysis?.profileLabel || getSellerProfileLabel(item) || "Без сигнала";
    const key = normalizeAnalyticsText(label) || "unknown";
    if (!sellerGroups.has(key)) {
      sellerGroups.set(key, {
        label,
        count: 0,
        totalTraderScore: 0,
        belowMarketCount: 0,
        relistedCount: 0
      });
    }
    const entry = sellerGroups.get(key);
    entry.count += 1;
    entry.totalTraderScore += Number(analysis?.traderScore || 0);
    if (Number(item.marketDifferencePercent || 0) <= -10) {
      entry.belowMarketCount += 1;
    }
    if (item.wasRelisted) {
      entry.relistedCount += 1;
    }
  });

  const sellerItems = [...sellerGroups.values()]
    .sort((left, right) => right.count - left.count || right.totalTraderScore - left.totalTraderScore)
    .slice(0, 8)
    .map(item => ({
      label: item.label,
      meta: `ниже рынка ${formatInteger(item.belowMarketCount)} · переопубл. ${formatInteger(item.relistedCount)}`,
      value: `${formatInteger(item.count)} шт.`
    }));

  const fastLeader = [...archivedTopModels]
    .sort((left, right) => right.fastCount - left.fastCount || left.averageDays - right.averageDays || right.count - left.count)
    .find(item => item.fastCount > 0);
  const stuckLeader = [...archivedTopModels]
    .sort((left, right) => right.stuckCount - left.stuckCount || right.averageDays - left.averageDays || right.count - left.count)
    .find(item => item.stuckCount > 0);

  const summaryItems = [
    {
      label: "Цена падала",
      meta: avgDrop > 0 ? `среднее снижение ${formatPrice(avgDrop)}` : "падений цены почти нет",
      value: `${formatInteger(withPriceDrops.length)}`
    },
    {
      label: "Висели долго",
      meta: avgDays > 0 ? `средний срок ${formatInteger(avgDays)} дн.` : "срок ещё не накоплен",
      value: `${formatInteger(longRunning.length)}`
    },
    {
      label: "Переопубликовано",
      meta: "архивные объявления с повторным выходом",
      value: `${formatInteger(relisted.length)}`
    },
    {
      label: "Цена не двигалась",
      meta: "долго висели без заметного снижения",
      value: `${formatInteger(stalePrice.length)}`
    },
    fastLeader
      ? {
          label: "Быстрее уходят",
          meta: `${fastLeader.label} · ср. ${formatInteger(fastLeader.averageDays)} дн.`,
          value: `${formatInteger(fastLeader.fastCount)}`
        }
      : null,
    stuckLeader
      ? {
          label: "Чаще зависают",
          meta: `${stuckLeader.label} · ср. ${formatInteger(stuckLeader.averageDays)} дн.`,
          value: `${formatInteger(stuckLeader.stuckCount)}`
        }
      : null
  ];

  elements.archiveModelMeta.textContent = scopedArchived.length
    ? `${formatInteger(scopedArchived.length)} архивных объявл.${archiveFilterLabel ? ` · ${archiveFilterLabel}` : ""}.`
    : archiveFilterLabel
      ? `По архиву фильтра ${archiveFilterLabel} данных пока нет.`
      : "Архив пока пуст.";
  elements.archiveSummaryMeta.textContent = scopedArchived.length
    ? "Это отдельный слой анализа старых и скрытых машин."
    : "Нужен архив объявлений для анализа.";
  elements.archiveSellerMeta.textContent = sellerItems.length
    ? "Показывает, как в архиве ведут себя типы продавцов."
    : "Когда архив наполнится, здесь появится срез по продавцам.";

  renderArchiveSellerList(
    elements.archiveModelList,
    archivedTopModels.map(item => ({
      label: item.label,
      meta: `ср. ${formatInteger(item.averageDays)} дн. · быстрых ${formatInteger(item.fastCount)} · зависших ${formatInteger(item.stuckCount)}`,
      value: `${formatInteger(item.count)} шт.`
    })),
    selectedCity ? `В архиве города ${selectedCity} пока нет моделей.` : "Архивных моделей пока нет."
  );
  renderArchiveSummaryList(elements.archiveSummaryList, summaryItems.filter(Boolean).filter(item => Number(item.value) > 0), "Архив ещё не накопил сигналы по срокам и ценам.");
  renderArchiveSellerList(elements.archiveSellerList, sellerItems, "Пока нет архивных продавцов для анализа.");
}

function renderMarket30Analytics() {
  const now = Date.now();
  const windowMs = 30 * 24 * 60 * 60 * 1000;
  const combined = [...state.listings, ...state.archivedListings]
    .filter(isKolesaListing)
    .filter(item => {
      const relevantAt = item.archivedAt || item.historyLastSeenAt || item.lastSeenAt || item.lastCheckedAt || item.historyFirstSeenAt;
      if (!relevantAt) {
        return false;
      }
      const time = Date.parse(relevantAt);
      return Number.isFinite(time) && now - time <= windowMs;
    });

  const uniqueItems = [...new Map(
    combined.map(item => [item.listingUid || item.advertId || item.url || item.id, item])
  ).values()];

  const modelGroups = new Map();
  uniqueItems.forEach(item => {
    const label = getModelAnalyticsLabel(item);
    const key = normalizeAnalyticsText(label);
    if (!key) {
      return;
    }

    if (!modelGroups.has(key)) {
      modelGroups.set(key, {
        label,
        count: 0,
        totalDays: 0,
        daysCount: 0,
        fastCount: 0,
        stuckCount: 0,
        dropCount: 0,
        totalDrop: 0
      });
    }

    const entry = modelGroups.get(key);
    const days = Number(item.daysOnMarket || 0);
    const dropTotal = Number(item.priceDropTotal || 0);
    const priceChangeCount = Number(item.priceChangeCount || 0);

    entry.count += 1;
    if (days > 0) {
      entry.totalDays += days;
      entry.daysCount += 1;
      if (days <= 7) {
        entry.fastCount += 1;
      }
      if (days >= 21 && priceChangeCount === 0) {
        entry.stuckCount += 1;
      }
    }
    if (priceChangeCount > 0 || dropTotal > 0) {
      entry.dropCount += 1;
      entry.totalDrop += Math.max(0, dropTotal);
    }
  });

  const modelStats = [...modelGroups.values()]
    .map(item => ({
      ...item,
      averageDays: item.daysCount ? Math.round(item.totalDays / item.daysCount) : 0,
      averageDrop: item.dropCount ? Math.round(item.totalDrop / item.dropCount) : 0
    }))
    .filter(item => item.count > 0);

  const cityModelGroups = new Map();
  uniqueItems.forEach(item => {
    const city = String(item.city || "").trim();
    const model = getModelAnalyticsLabel(item);
    if (!city || !model) {
      return;
    }
    const key = `${normalizeAnalyticsText(city)}|${normalizeAnalyticsText(model)}`;
    if (!cityModelGroups.has(key)) {
      cityModelGroups.set(key, {
        label: `${city} · ${model}`,
        count: 0,
        totalDays: 0,
        daysCount: 0,
        fastCount: 0
      });
    }
    const entry = cityModelGroups.get(key);
    const days = Number(item.daysOnMarket || 0);
    entry.count += 1;
    if (days > 0) {
      entry.totalDays += days;
      entry.daysCount += 1;
      if (days <= 7) {
        entry.fastCount += 1;
      }
    }
  });

  const popularItems = [...modelStats]
    .sort((left, right) => right.count - left.count || left.averageDays - right.averageDays || left.label.localeCompare(right.label, "ru"))
    .slice(0, 8)
    .map(item => ({
      label: item.label,
      meta: `ср. ${formatInteger(item.averageDays)} дн. · падений ${formatInteger(item.dropCount)}`,
      value: `${formatInteger(item.count)} шт.`
    }));

  const fastItems = [...modelStats]
    .filter(item => item.fastCount > 0)
    .sort((left, right) => right.fastCount - left.fastCount || left.averageDays - right.averageDays || right.count - left.count)
    .slice(0, 8)
    .map(item => ({
      label: item.label,
      meta: `ср. ${formatInteger(item.averageDays)} дн. · в базе ${formatInteger(item.count)}`,
      value: `${formatInteger(item.fastCount)} быстрых`
    }));

  const stuckItems = [...modelStats]
    .filter(item => item.stuckCount > 0)
    .sort((left, right) => right.stuckCount - left.stuckCount || right.averageDays - left.averageDays || right.count - left.count)
    .slice(0, 8)
    .map(item => ({
      label: item.label,
      meta: `ср. ${formatInteger(item.averageDays)} дн. · в базе ${formatInteger(item.count)}`,
      value: `${formatInteger(item.stuckCount)} зависших`
    }));

  const dropItems = [...modelStats]
    .filter(item => item.dropCount > 0)
    .sort((left, right) => right.dropCount - left.dropCount || right.averageDrop - left.averageDrop || right.count - left.count)
    .slice(0, 8)
    .map(item => ({
      label: item.label,
      meta: item.averageDrop > 0 ? `ср. снижение ${formatPrice(item.averageDrop)}` : `в базе ${formatInteger(item.count)}`,
      value: `${formatInteger(item.dropCount)} падений`
    }));

  const cityItems = [...cityModelGroups.values()]
    .filter(item => item.fastCount > 0)
    .map(item => ({
      ...item,
      averageDays: item.daysCount ? Math.round(item.totalDays / item.daysCount) : 0
    }))
    .sort((left, right) => right.fastCount - left.fastCount || left.averageDays - right.averageDays || right.count - left.count)
    .slice(0, 8)
    .map(item => ({
      label: item.label,
      meta: `ср. ${formatInteger(item.averageDays)} дн. · в базе ${formatInteger(item.count)}`,
      value: `${formatInteger(item.fastCount)} быстрых`
    }));

  elements.market30Meta.textContent = uniqueItems.length
    ? `${formatInteger(uniqueItems.length)} уникальных авто за последние 30 дней в локальной базе.`
    : "Локальная база пока не накопила данные за 30 дней.";
  elements.market30FastMeta.textContent = fastItems.length
    ? "Модели, которые чаще уходят быстро."
    : "Пока нет данных по быстрым продажам.";
  elements.market30StuckMeta.textContent = stuckItems.length
    ? "Модели, которые чаще зависают без движения цены."
    : "Пока нет данных по зависшим моделям.";
  elements.market30DropMeta.textContent = dropItems.length
    ? "Модели, где чаще встречается снижение цены."
    : "Пока нет данных по падению цены.";
  elements.market30CityMeta.textContent = cityItems.length
    ? "Комбинации город + модель, которые чаще уходят быстро."
    : "Пока нет городов с быстрыми повторяющимися продажами.";

  renderArchiveSellerList(elements.market30PopularList, popularItems, "Нужно накопить локальную базу хотя бы за несколько дней.");
  renderArchiveSellerList(elements.market30FastList, fastItems, "Пока нет быстрых моделей в окне 30 дней.");
  renderArchiveSellerList(elements.market30StuckList, stuckItems, "Пока нет зависших моделей в окне 30 дней.");
  renderArchiveSellerList(elements.market30DropList, dropItems, "Пока нет моделей с заметным падением цены.");
  renderArchiveSellerList(elements.market30CityList, cityItems, "Пока нет повторяющихся быстрых сочетаний город + модель.");
}

function renderBars(listings) {
  const config = getAnalysisModeConfig();
  elements.bars.innerHTML = "";

  listings.slice(0, 6).forEach(item => {
    const score = item[config.scoreKey] ?? item.score ?? 0;
    const row = document.createElement("div");
    row.className = "bar-row";
    row.innerHTML = `
      <div class="bar-label">${escapeHtml(item.title)}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${Math.max(score * 100, 5)}%"></div></div>
      <div>${formatScore(score)}</div>
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
    return `<div class="${className}">${renderLazyImageTag(proxied, "Фото авто")}</div>`;
  }

  return `<div class="${className} thumb--empty">нет фото</div>`;
}

function renderLazyImageTag(src, alt, { eager = false, className = "lazy-image" } = {}) {
  const safeSrc = escapeHtml(src || "");
  const safeAlt = escapeHtml(alt || "");
  if (!safeSrc) {
    return "";
  }

  if (eager) {
    return `<img class="${className} is-loaded" src="${safeSrc}" alt="${safeAlt}" loading="eager" decoding="async">`;
  }

  return `<img class="${className}" data-lazy-src="${safeSrc}" alt="${safeAlt}" loading="lazy" decoding="async">`;
}

function ensureLazyImageObserver() {
  if (lazyImageObserver || typeof window === "undefined" || typeof IntersectionObserver === "undefined") {
    return;
  }

  lazyImageObserver = new IntersectionObserver(entries => {
    entries.forEach(entry => {
      if (!entry.isIntersecting) {
        return;
      }
      const image = entry.target;
      const lazySrc = image.dataset.lazySrc;
      if (lazySrc && !image.src) {
        image.src = lazySrc;
      }
      image.classList.add("is-loaded");
      image.removeAttribute("data-lazy-src");
      lazyImageObserver.unobserve(image);
    });
  }, {
    rootMargin: "250px 0px"
  });
}

function hydrateLazyImages(root = document) {
  ensureLazyImageObserver();
  const images = root.querySelectorAll ? root.querySelectorAll("img[data-lazy-src]") : [];
  images.forEach(image => {
    if (!lazyImageObserver) {
      image.src = image.dataset.lazySrc;
      image.classList.add("is-loaded");
      image.removeAttribute("data-lazy-src");
      return;
    }
    lazyImageObserver.observe(image);
  });
}

function renderTopLists(listings) {
  const config = getAnalysisModeConfig();
  elements.topScoreList.innerHTML = "";
  elements.topDealList.innerHTML = "";
  elements.topFreshList.innerHTML = "";
  elements.topBadList.innerHTML = "";
  elements.topCreditList.innerHTML = "";
  elements.topMaintenanceList.innerHTML = "";

  const topByScore = [...listings]
    .sort((a, b) => getPrimaryScore(b) - getPrimaryScore(a) || a.price - b.price)
    .slice(0, 5);
  const topByDeal = [...listings]
    .sort((a, b) => (b[config.topSecondaryKey] ?? 0) - (a[config.topSecondaryKey] ?? 0) || getPrimaryScore(b) - getPrimaryScore(a))
    .slice(0, 5);
  const topByFresh = [...listings]
    .sort((a, b) => b.freshnessScore - a.freshnessScore || getPrimaryScore(b) - getPrimaryScore(a))
    .slice(0, 5);
  const topBad = [...listings]
    .sort((a, b) => b.badScore - a.badScore || getPrimaryScore(a) - getPrimaryScore(b))
    .slice(0, 5);
  const topCredit = [...listings]
    .filter(item => item.creditAvailable && item.creditMonthlyPayment)
    .sort((a, b) => b.creditScore - a.creditScore || getPrimaryScore(b) - getPrimaryScore(a) || a.creditMonthlyPayment - b.creditMonthlyPayment)
    .slice(0, 5);
  const topMaintenance = [...listings]
    .filter(item => hasTrustedAutopartsProfile(item))
    .sort((a, b) => (b.maintenanceScore ?? 0) - (a.maintenanceScore ?? 0) || getPrimaryScore(b) - getPrimaryScore(a) || a.price - b.price)
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

  renderList(elements.topScoreList, topByScore, config.scoreKey, config.scoreLabel.toLowerCase());
  renderList(elements.topDealList, topByDeal, config.topSecondaryKey, config.topSecondaryLabel);
  renderList(elements.topFreshList, topByFresh, "freshnessScore", "fresh");
  renderList(elements.topBadList, topBad, "badScore", "bad");
  renderList(elements.topCreditList, topCredit, "creditScore", "credit");
  renderList(elements.topMaintenanceList, topMaintenance, "maintenanceScore", "содерж.");
}

function renderTable(listings) {
  renderTableSortHeaders();
  elements.resultsBody.innerHTML = "";
  const totalPages = Math.max(1, Math.ceil(listings.length / TABLE_PAGE_SIZE));
  state.currentPage = Math.min(Math.max(state.currentPage, 1), totalPages);
  const startIndex = (state.currentPage - 1) * TABLE_PAGE_SIZE;
  const pageItems = listings.slice(startIndex, startIndex + TABLE_PAGE_SIZE);
  const endIndex = Math.min(listings.length, startIndex + pageItems.length);
  elements.resultsCount.textContent = listings.length
    ? `${formatInteger(listings.length)} результатов · показано ${formatInteger(startIndex + 1)}-${formatInteger(endIndex)}`
    : "0 результатов";
  if (elements.resultsPageMeta) {
    elements.resultsPageMeta.textContent = `Страница ${formatInteger(state.currentPage)} из ${formatInteger(totalPages)}`;
  }
  if (elements.resultsPrevBtn) {
    elements.resultsPrevBtn.disabled = state.currentPage <= 1;
  }
  if (elements.resultsNextBtn) {
    elements.resultsNextBtn.disabled = state.currentPage >= totalPages;
  }

  if (!listings.length) {
    const row = document.createElement("tr");
    row.innerHTML = `<td colspan="9">Ничего не найдено по текущим фильтрам.</td>`;
    elements.resultsBody.append(row);
    return;
  }

  pageItems.forEach(item => {
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
      <td><span class="score-badge">${formatScore(getPrimaryScore(item))}</span></td>
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
    const marketTitle = item.marketSourceLabel === "Локальный рынок"
      ? `Средняя цена по городу (${formatInteger(item.marketSampleSize || 0)})`
      : item.marketSourceLabel === "Рынок модели"
        ? `Средняя цена по модели (${formatInteger(item.marketSampleSize || 0)})`
        : "Средняя цена Kolesa";

    return [
      renderFact(marketTitle, formatPrice(item.avgPrice)),
      renderFact(label, value)
    ];
  }

  if (item.marketConfidence === "insufficient") {
    return [renderFact("Рынок", "данных рынка мало")];
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

function getMaintenanceNarrative(item) {
  const autoparts = hasTrustedAutopartsProfile(item) ? item?.autopartsProfile : null;
  const score = Number(item?.maintenanceScore ?? 0.5);

  if (!autoparts) {
    return {
      tone: "neutral",
      title: "Нет базы по запчастям",
      note: "Оценка содержания пока нейтральная: цены на запчасти для этой машины ещё не собраны."
    };
  }

  if (score >= 0.72) {
    return {
      tone: "good",
      title: "Обслуживание выглядит недорогим",
      note: autoparts.coverageLevel === "full"
        ? "По базе запчастей это сильная сторона машины."
        : "Даже по неполной базе видно, что обслуживание выглядит скорее дешёвым."
    };
  }

  if (score >= 0.45) {
    return {
      tone: "neutral",
      title: "Обслуживание выглядит средним",
      note: autoparts.coverageLevel === "full"
        ? "По запчастям сильного минуса нет, но и особой экономии тоже."
        : "Данных пока не хватает для уверенного вывода, но явной проблемы не видно."
    };
  }

  return {
    tone: "bad",
    title: "Обслуживание выглядит дорогим",
    note: autoparts.coverageLevel === "full"
      ? "По базе запчастей это уже заметный минус для владения."
      : "Даже по неполной базе уже видно, что ремонт может выйти дорогим."
  };
}

function renderMaintenancePanel(item) {
  if (!elements.modalMaintenance) {
    return;
  }

  const autoparts = hasTrustedAutopartsProfile(item) ? item.autopartsProfile : null;
  const buyerDecision = getBuyerDecisionMeta(item);
  const resellerDecision = getResellerDecisionMeta(item);
  const maintenance = getMaintenanceNarrative(item);

  if (!autoparts) {
    elements.modalMaintenance.innerHTML = `
      <div class="maintenance-summary maintenance-summary--neutral">
        <strong>${escapeHtml(maintenance.title)}</strong>
        <p>${escapeHtml(maintenance.note)}</p>
      </div>
      <div class="maintenance-grid">
        <div class="maintenance-item">
          <span>Содержание</span>
          <strong>${formatScore(item.maintenanceScore)}</strong>
        </div>
        <div class="maintenance-item">
          <span>Покрытие</span>
          <strong>Нет данных</strong>
        </div>
      </div>
    `;
    return;
  }

  const buyerImpact = buyerDecision.label === "Брать"
    ? "не мешает покупке"
    : buyerDecision.label === "Осторожно"
      ? "требует перепроверки перед покупкой"
      : "ухудшает решение по покупке";
  const resellerImpact = resellerDecision.label === "Есть маржа"
    ? "не ломает маржу"
    : resellerDecision.label === "Слабая маржа"
      ? "съедает часть маржи"
      : "делает перепродажу рискованнее";

  elements.modalMaintenance.innerHTML = `
    <div class="maintenance-summary maintenance-summary--${escapeHtml(maintenance.tone)}">
      <strong>${escapeHtml(maintenance.title)}</strong>
      <p>${escapeHtml(maintenance.note)}</p>
    </div>
    <div class="maintenance-grid">
      <div class="maintenance-item">
        <span>Содержание</span>
        <strong>${formatScore(item.maintenanceScore)}</strong>
      </div>
      <div class="maintenance-item">
        <span>Покрытие</span>
        <strong>${escapeHtml(autoparts.coverageLabel || "Есть данные")}</strong>
      </div>
      <div class="maintenance-item">
        <span>Колодки / перед</span>
        <strong>${autoparts.frontPadsPriceKzt ? formatPrice(autoparts.frontPadsPriceKzt) : "-"}</strong>
      </div>
      <div class="maintenance-item">
        <span>Диск / перед</span>
        <strong>${autoparts.frontDiscPriceKzt ? formatPrice(autoparts.frontDiscPriceKzt) : "-"}</strong>
      </div>
      <div class="maintenance-item">
        <span>Сервисная корзина</span>
        <strong>${autoparts.serviceBasketKzt ? formatPrice(autoparts.serviceBasketKzt) : "Пока нет"}</strong>
      </div>
      <div class="maintenance-item">
        <span>Запчасти</span>
        <strong>${escapeHtml(autoparts.maintenanceLabel || "Есть данные")}</strong>
      </div>
    </div>
    <div class="maintenance-impact-list">
      <div class="maintenance-impact">
        <span>Для покупки</span>
        <strong>${escapeHtml(buyerImpact)}</strong>
      </div>
      <div class="maintenance-impact">
        <span>Для перекупа</span>
        <strong>${escapeHtml(resellerImpact)}</strong>
      </div>
    </div>
  `;
}

function renderPriceHistoryPanel(item) {
  if (!elements.modalPriceHistory) {
    return;
  }

  const key = getListingSnapshotCacheKey(item);
  const snapshotState = state.listingSnapshots[key];
  const snapshots = getListingSnapshotRows(item)
    .slice()
    .sort((left, right) => new Date(right.captured_at || right.capturedAt || 0).getTime() - new Date(left.captured_at || left.capturedAt || 0).getTime());

  if (snapshotState?.status === "loading") {
    elements.modalPriceHistory.innerHTML = `<div class="price-history-empty">Загружаю историю цены...</div>`;
    return;
  }

  if (snapshotState?.status === "error") {
    elements.modalPriceHistory.innerHTML = `<div class="price-history-empty">Не удалось загрузить историю цены.</div>`;
    return;
  }

  if (!snapshots.length) {
    elements.modalPriceHistory.innerHTML = `<div class="price-history-empty">Истории пока нет. Нажми "Собрать историю" или проверь объявление позже.</div>`;
    return;
  }

  const summary = [
    { label: "Наблюдений", value: formatInteger(item.snapshotCount ?? snapshots.length) },
    { label: "Дней в продаже", value: formatDaysOnMarket(item.daysOnMarket) },
    { label: "Изм. цены", value: formatInteger(item.priceChangeCount ?? 0) },
    { label: "Снижение", value: Number.isFinite(item.priceDropTotal) ? formatPrice(item.priceDropTotal) : "-" },
    { label: "Сигнал", value: getHistorySummaryLabel(item) }
  ];

  const timeline = snapshots
    .slice(0, 12)
    .map((snapshot, index) => {
      const previous = snapshots[index + 1];
      const delta = getSnapshotPriceDelta(snapshot, previous);
      const isLatest = index === 0;
      const price = Number.isFinite(snapshot.price) ? formatPrice(snapshot.price) : "-";
      const status = String(snapshot.actuality_status || snapshot.actualityStatus || "").trim();
      const credit = snapshot.credit_available || snapshot.creditAvailable ? "кредит" : "";
      const meta = [status, credit, snapshot.photo_count || snapshot.photoCount ? `фото ${snapshot.photo_count || snapshot.photoCount}` : ""]
        .filter(Boolean)
        .join(" · ");

      return `
        <div class="price-history-item${isLatest ? " is-latest" : ""}">
          <div>
            <div class="price-history-item-head">
              <span class="price-history-date">${escapeHtml(formatSnapshotEventDate(snapshot.captured_at || snapshot.capturedAt))}</span>
              ${delta ? `<span class="price-history-badge price-history-badge--${escapeHtml(delta.direction)}">${escapeHtml(delta.label)}</span>` : ""}
              ${isLatest ? `<span class="price-history-badge">последний</span>` : ""}
            </div>
            <div class="price-history-meta">${escapeHtml(meta || "без доп. изменений")}</div>
          </div>
          <div class="price-history-price">${escapeHtml(price)}</div>
        </div>
      `;
    })
    .join("");

  elements.modalPriceHistory.innerHTML = `
    <div class="price-history-summary">
      ${summary.map(itemSummary => `
        <div class="price-history-stat">
          <span>${escapeHtml(itemSummary.label)}</span>
          <strong>${escapeHtml(itemSummary.value)}</strong>
        </div>
      `).join("")}
    </div>
    <div class="price-history-timeline">${timeline}</div>
  `;
}

async function loadListingSnapshots(item, { force = false } = {}) {
  const key = getListingSnapshotCacheKey(item);
  if (!key || (!force && state.listingSnapshots[key]?.status === "loaded")) {
    renderPriceHistoryPanel(item);
    return;
  }

  state.listingSnapshots[key] = {
    status: "loading",
    items: getListingSnapshotRows(item)
  };
  if (state.selectedListingId === item.id) {
    renderPriceHistoryPanel(item);
  }

  try {
    const query = item.advertId
      ? `advertId=${encodeURIComponent(item.advertId)}`
      : `url=${encodeURIComponent(item.url)}`;
    const response = await fetch(`/api/listing-snapshots?${query}`);
    if (!response.ok) {
      throw new Error("Snapshots load failed");
    }
    const payload = await response.json();
    const items = Array.isArray(payload.items) ? payload.items : [];
    state.listingSnapshots[key] = {
      status: "loaded",
      items
    };
  } catch (error) {
    state.listingSnapshots[key] = {
      status: "error",
      items: []
    };
  }

  if (state.selectedListingId === item.id) {
    const selected = getListingById(item.id) || item;
    renderPriceHistoryPanel(selected);
  }
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

  if (label === "Рынок" && item.marketConfidence === "insufficient") {
    return "данных рынка пока мало, поэтому влияние на решение нейтральное";
  }

  if (label === "Цена") {
    return `текущая цена ${formatPrice(item.price)}`;
  }

  if (label === "Качество") {
    return value >= 0.7 ? "сильное общее состояние по данным объявления" : value >= 0.45 ? "средний уровень по качеству" : "есть слабые сигналы по качеству";
  }

  if (label === "Выгода") {
    return value >= 0.7 ? "есть запас ниже рынка и шанс на хорошую сделку" : value >= 0.45 ? "сделка выглядит средней" : "запас по выгоде слабый";
  }

  if (label === "Ликвидность") {
    return value >= 0.7 ? "модель выглядит быстрой для перепродажи" : value >= 0.45 ? "ликвидность средняя" : "может продаваться долго";
  }

  if (label === "Содержание") {
    if (!hasTrustedAutopartsProfile(item)) {
      return "по запчастям пока нет базы, оценка нейтральная";
    }

    const basket = item.autopartsProfile.serviceBasketKzt
      ? formatPrice(item.autopartsProfile.serviceBasketKzt)
      : (item.autopartsProfile.coverageLevel === "partial" || item.autopartsProfile.coverageLevel === "basic"
          ? "частичные данные"
          : "нет корзины");
    return value >= 0.72
      ? `запчасти выглядят дешёвыми, сервисная корзина ${basket}`
      : value >= 0.45
        ? `обслуживание выглядит средним, сервисная корзина ${basket}`
        : `обслуживание выйдет дороже среднего, сервисная корзина ${basket}`;
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
    const summary = getSellerProfileSummary(item);
    if (summary.note) {
      return `${summary.label.toLowerCase()}: ${summary.note}`;
    }
    return item.isUsedCarDealer ? "похоже на автосалон или перекупа" : "ближе к частному продавцу";
  }

  return value >= 0.72 ? "сильная сторона" : value >= 0.45 ? "нейтрально" : "слабое место";
}

function renderListingFacts(item) {
  const actualityMeta = getActualityMeta(item);
  const config = getAnalysisModeConfig();
  const currentDecision = getCurrentDecisionMeta(item);
  const autoparts = hasTrustedAutopartsProfile(item) ? item.autopartsProfile : null;
  const sellerSummary = getSellerProfileSummary(item);
  elements.modalFacts.innerHTML = [
    renderFact("Статус", actualityMeta.label),
    renderFact("Проверено", formatDateTime(item.lastCheckedAt)),
    renderFact("Решение", `${config.scoreLabel}: ${currentDecision.label}`),
    renderFact("Покупка", formatScore(item.buyerScore ?? item.score)),
    renderFact("Перекуп", formatScore(item.resellerOpportunityScore)),
    renderFact("Содержание", formatScore(item.maintenanceScore)),
    renderFact("Качество", formatScore(item.qualityScore)),
    renderFact("Выгода", formatScore(item.dealScore)),
    ...renderMarketFacts(item),
    renderFact("Дата объявления", formatListingDateDetailed(item)),
    renderFact("Дней в продаже", formatDaysOnMarket(item.daysOnMarket)),
    renderFact("Наблюдений", item.snapshotCount ?? "-"),
    renderFact("Изменений цены", item.priceChangeCount ?? "-"),
    renderFact("Первая цена", Number.isFinite(item.firstPrice) ? formatPrice(item.firstPrice) : "-"),
    renderFact("Текущая цена", Number.isFinite(item.lastPrice) ? formatPrice(item.lastPrice) : formatPrice(item.price)),
    renderFact("Снижение цены", Number.isFinite(item.priceDropTotal) ? formatPrice(item.priceDropTotal) : "-"),
    renderFact("Последнее изм. цены", item.lastPriceChangeAt ? formatDateTime(item.lastPriceChangeAt) : "-"),
    renderFact("Переопубликовано", formatYesNo(item.wasRelisted)),
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
    renderFact("Профиль продавца", sellerSummary.label || "-"),
    renderFact(
      "Объявлений продавца",
      sellerSummary.total !== null
        ? `${sellerSummary.active ?? sellerSummary.total} акт. / ${sellerSummary.total}`
        : "-"
    ),
    renderFact("Поток продавца", sellerSummary.traderScore !== null ? `${Math.round(sellerSummary.traderScore)}%` : "-"),
    renderFact("Анализ продавца", sellerSummary.note || "-"),
    renderFact("Риск", Number.isFinite(item.riskScore) ? `${Math.round(item.riskScore)}/100` : "-"),
    renderFact("Ликвидность", Number.isFinite(item.liquidityScore) ? `${(item.liquidityScore * 100).toFixed(0)}%` : "-"),
    renderFact("Признак перекупа", Number.isFinite(item.sellerSignalScore) ? `${(item.sellerSignalScore * 100).toFixed(0)}%` : "-"),
    renderFact(
      "Запчасти",
      autoparts
        ? `${autoparts.maintenanceLabel || "Есть данные"}${Number.isFinite(autoparts.cheapnessScore) ? ` (${formatScorePercent(autoparts.cheapnessScore / 100)})` : ""}`
        : "-"
    ),
    renderFact("Матч запчастей", autoparts?.matchConfidenceLabel || "-"),
    renderFact("Данные запчастей", autoparts?.coverageLabel || "-"),
    renderFact("Сервисная корзина", autoparts?.serviceBasketKzt ? formatPrice(autoparts.serviceBasketKzt) : "-"),
    renderFact("Колодки / перед", autoparts?.frontPadsPriceKzt ? formatPrice(autoparts.frontPadsPriceKzt) : "-"),
    renderFact("Диск / перед", autoparts?.frontDiscPriceKzt ? formatPrice(autoparts.frontDiscPriceKzt) : "-"),
    renderFact("Цена", formatPrice(item.price)),
    renderFact("Город", item.city || "-")
  ].join("");

  renderMaintenancePanel(item);
}

function renderListingBreakdown(item) {
  const config = getAnalysisModeConfig();
  const parts = item.scoreParts || {
    price: 0.5,
    quality: 0.5,
    year: 0.5,
    mileage: 0.5,
    owners: 0.5,
    market: 0.5,
    freshness: 0.5,
    risk: 0.5,
    maintenance: 0.5,
    photos: 0.5,
    seller: 0.5
  };

  const breakdownItems = state.analysisMode === "reseller"
    ? [
        ["Выгода", item.dealScore],
        ["Ликвидность", item.liquidityScore],
        ["Содержание", item.maintenanceScore],
        ["Рынок", parts.market],
        ["Свежесть", parts.freshness],
        ["Риск", parts.risk],
        ["Фото", parts.photos],
        ["Продавец", parts.seller],
        ["Год", parts.year],
        ["Пробег", parts.mileage]
      ]
    : [
        ["Качество", parts.quality],
        ["Цена", parts.price],
        ["Содержание", item.maintenanceScore],
        ["Рынок", parts.market],
        ["Год", parts.year],
        ["Пробег", parts.mileage],
        ["Владельцы", parts.owners],
        ["Свежесть", parts.freshness],
        ["Риск", parts.risk],
        ["Фото", parts.photos],
        ["Продавец", parts.seller]
      ];

  if (elements.modalBreakdownTitle) {
    elements.modalBreakdownTitle.textContent = config.breakdownTitle;
  }

  elements.modalBreakdown.innerHTML = breakdownItems
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

function getPrimaryDecisionReasons(item) {
  const reasons = [];
  const sellerSummary = getSellerProfileSummary(item);

  if (Number.isFinite(item.marketDifferencePercent)) {
    if (item.marketDifferencePercent >= 10) {
      reasons.push(`ниже рынка на ${formatPercent(item.marketDifferencePercent)}`);
    } else if (item.marketDifferencePercent <= -10) {
      reasons.push(`выше рынка на ${formatPercent(Math.abs(item.marketDifferencePercent))}`);
    } else {
      reasons.push("цена около рынка");
    }
  } else if (item.marketConfidence === "insufficient") {
    reasons.push("данных рынка мало");
  }

  if (hasTrustedAutopartsProfile(item)) {
    const maintenanceTone = Number(item.maintenanceScore || 0) >= 0.72
      ? "обслуживание дешёвое"
      : Number(item.maintenanceScore || 0) >= 0.45
        ? "обслуживание среднее"
        : "обслуживание дорогое";
    reasons.push(maintenanceTone);
  } else {
    reasons.push("по запчастям базы мало");
  }

  if (sellerSummary.label && sellerSummary.label !== "Продавец") {
    reasons.push(sellerSummary.label.toLowerCase());
  }

  const historySignals = getHistorySignalMeta(item);
  if (historySignals.length) {
    historySignals.forEach(signal => reasons.push(signal.label.toLowerCase()));
  } else if (Number(item.daysOnMarket || 0) > 0) {
    reasons.push(`в продаже ${formatDaysOnMarket(item.daysOnMarket).toLowerCase()}`);
  }

  return [...new Set(reasons)].slice(0, 3);
}

function renderSignals(item) {
  const config = getAnalysisModeConfig();
  const currentDecision = getCurrentDecisionMeta(item);
  const buyerDecision = getBuyerDecisionMeta(item);
  const resellerDecision = getResellerDecisionMeta(item);
  const autoparts = hasTrustedAutopartsProfile(item) ? item.autopartsProfile : null;
  const sellerSummary = getSellerProfileSummary(item);
  const primaryReasons = getPrimaryDecisionReasons(item);
  const finance = [];
  const details = [];

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
    details.push(`Контактов в объявлении: ${item.phoneCount}`);
  }
  if (item.phonePrefix) {
    details.push(`Префикс номера: ${item.phonePrefix}`);
  }
  if (item.photoCount) {
    details.push(`Фотографий: ${item.photoCount}`);
  }
  if (item.fuelType) {
    details.push(`Топливо: ${item.fuelType}`);
  }
  if (item.driveType) {
    details.push(`Привод: ${item.driveType}`);
  }
  if (item.steeringSide) {
    details.push(`Руль: ${item.steeringSide}`);
  }
  if (item.color) {
    details.push(`Цвет: ${item.color}`);
  }
  if (item.options.length) {
    details.push(`Опции: ${item.options.join(", ")}`);
  }
  if (item.paidServices.length) {
    details.push(`Продвижение: ${item.paidServices.join(", ")}`);
  }
  if (sellerSummary.label && sellerSummary.label !== "-") {
    const baseLine = sellerSummary.total !== null
      ? `Профиль продавца по базе: ${sellerSummary.label}, ${sellerSummary.active ?? sellerSummary.total} акт. из ${sellerSummary.total}`
      : `Профиль продавца по базе: ${sellerSummary.label}`;
    details.push(baseLine);
  }
  if (sellerSummary.traderScore !== null) {
    details.push(`Поток продавца по базе: ${Math.round(sellerSummary.traderScore)}%`);
  }
  if (sellerSummary.note) {
    details.push(`Анализ продавца: ${sellerSummary.note}`);
  }
  if (item.publicHistoryAvailable) {
    details.push(`Есть публичная история авто на странице`);
  }
  if (autoparts?.maintenanceLabel) {
    const serviceBasket = autoparts.serviceBasketKzt ? `, корзина ${formatPrice(autoparts.serviceBasketKzt)}` : "";
    const coverage = autoparts.coverageLabel ? `, ${autoparts.coverageLabel.toLowerCase()}` : "";
    details.push(`Запчасти: ${autoparts.maintenanceLabel}${coverage}, maintenance ${formatScorePercent(item.maintenanceScore)}${serviceBasket}`);
  }
  if (autoparts?.comment) {
    details.push(`По рынку запчастей: ${autoparts.comment}`);
  }
  if (item.historySummary) {
    details.push(item.historySummary);
  }
  if (item.riskFlags.length) {
    details.push(...item.riskFlags);
  }

  const detailLines = [...finance, ...details];
  if (!primaryReasons.length && !detailLines.length) {
    return `<div class="muted">После проверки карточки здесь появятся кредитные условия и открытые сигналы по объявлению.</div>`;
  }

  return `
    <div class="signal-summary">
      <strong>${escapeHtml(config.scoreLabel)}: ${escapeHtml(currentDecision.label)}</strong>
      <p>${escapeHtml(currentDecision.note)}</p>
      <div class="signal-meta">Покупка: ${escapeHtml(buyerDecision.label)} · Перекуп: ${escapeHtml(resellerDecision.label)}</div>
    </div>
    ${primaryReasons.length ? `<ul class="signal-list">${primaryReasons.map(line => `<li>${escapeHtml(line)}</li>`).join("")}</ul>` : ""}
    ${detailLines.length ? `<div class="signal-subhead">Детали</div><ul class="signal-list">${detailLines.map(line => `<li>${escapeHtml(line)}</li>`).join("")}</ul>` : ""}
  `;
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
    : "";

  state.modalGalleryIndex = currentIndex;
  elements.modalMedia.innerHTML = `
    <div class="modal-gallery">
      <div class="modal-gallery-main">
        ${hasMultiple ? '<button class="modal-gallery-nav modal-gallery-nav--prev" type="button" data-gallery-step="-1" aria-label="Предыдущее фото">‹</button>' : ""}
        <a class="modal-gallery-link" href="${escapeHtml(proxiedImage)}" target="_blank" rel="noreferrer">
          ${renderLazyImageTag(proxiedImage, item.title, { eager: true, className: "lazy-image modal-main-image" })}
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
              ${renderLazyImageTag(toProxiedImageUrl(image), `${item.title} ${index + 1}`)}
            </button>
          `).join("")}
        </div>
      ` : ""}
    </div>
  `;
  hydrateLazyImages(elements.modalMedia);
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
      void loadListingSnapshots(updated, { force: true });
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

async function queueHistoryCollection(targets, {
  button,
  idleText = "Собрать историю",
  queueText = "В очереди...",
  progressPrefix = "История",
  sourceLabel = "текущего списка"
} = {}) {
  if (!targets.length) {
    window.alert(`Нет объявлений Kolesa для истории из ${sourceLabel}.`);
    return;
  }

  const normalizedTargets = [...new Map(
    targets
      .filter(item => item?.url)
      .map(item => [item.listingUid || item.advertId || item.url, item])
  ).values()];
  const batches = [];
  for (let index = 0; index < normalizedTargets.length; index += HISTORY_BATCH_LIMIT) {
    batches.push(normalizedTargets.slice(index, index + HISTORY_BATCH_LIMIT));
  }

  if (button) {
    button.disabled = true;
    button.textContent = queueText;
  }
  setStatus(`Источник: ставим сбор истории в очередь для ${normalizedTargets.length} объявлений из ${sourceLabel}...`);

  try {
    let totalChecked = 0;
    let totalFailed = 0;
    let totalSkippedRecent = 0;
    let processedBeforeBatch = 0;

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex += 1) {
      const batch = batches[batchIndex];
      const response = await fetch("/api/jobs/collect-snapshots", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
      body: JSON.stringify({
        urls: batch.map(item => item.url),
        limit: batch.length,
        mode: sourceLabel === "всей локальной базы" ? "full" : "manual",
        concurrency: 1
      })
    });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Collect history failed");
      }

      const completedJob = await waitForServerJob(payload.job?.id, {
        onProgress: job => {
          const batchProgress = Number(job.progress || 0);
          const totalProgress = Math.min(normalizedTargets.length, processedBeforeBatch + batchProgress);
          if (button) {
            button.textContent = job.status === "queued"
              ? queueText
              : `${progressPrefix} ${totalProgress}/${normalizedTargets.length}`;
          }
          setStatus(`Источник: ${job.message || "идет сбор истории"} · пакет ${batchIndex + 1}/${batches.length} (${totalProgress}/${normalizedTargets.length})`);
        }
      });

      processedBeforeBatch += batch.length;
      totalChecked += Number(completedJob.result?.checked || 0);
      totalFailed += Number(completedJob.result?.failed || 0);
      totalSkippedRecent += Number(completedJob.result?.skipped_recent || 0);
    }

    await loadListingsFromServer();
    state.renderedListings = getFilteredListings();
    state.listingSnapshots = {};
    populateCities();
    render();
    if (state.selectedListingId && !elements.detailModal.hidden) {
      const selected = getListingById(state.selectedListingId);
      if (selected) {
        void loadListingSnapshots(selected, { force: true });
      }
    }

    setStatus(`Источник: история собрана, обновлено ${totalChecked}, ошибок ${totalFailed}, пропущено недавно проверенных ${totalSkippedRecent}.`);
  } catch (error) {
    window.alert(error.message || "Не удалось собрать историю.");
    setStatus("Источник: ошибка сбора истории");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = idleText;
    }
  }
}

async function collectHistoryForRenderedListings() {
  const targets = state.renderedListings.filter(isKolesaListing).filter(item => item.url);
  await queueHistoryCollection(targets, {
    button: elements.collectHistoryBtn,
    idleText: "Собрать историю",
    queueText: "В очереди...",
    progressPrefix: "История",
    sourceLabel: "текущего списка"
  });
}

async function collectHistoryForAllListings() {
  const targets = state.listings.filter(isKolesaListing).filter(item => item.url);
  await queueHistoryCollection(targets, {
    button: elements.collectAllHistoryBtn,
    idleText: "Собрать всё",
    queueText: "Вся база...",
    progressPrefix: "Вся база",
    sourceLabel: "всей локальной базы"
  });
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
            elements.modalSignals.innerHTML = renderSignals(selected);
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
    const scoreLabel = getAnalysisModeConfig().scoreLabel;
    elements.compareWinnerText.textContent = winner
      ? `Лучший ${scoreLabel.toLowerCase()} вариант сейчас: ${winner.title} (${formatScore(getPrimaryScore(winner))})`
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
    const scoreDiff = getPrimaryScore(b) - getPrimaryScore(a);
    if (scoreDiff !== 0) {
      return scoreDiff;
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
      "buyerScore",
      "resellerScore",
      "maintenanceScore",
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
      formatScore(item.buyerScore ?? item.score),
      formatScore(item.resellerOpportunityScore),
      formatScore(item.maintenanceScore),
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
  const modeConfig = getAnalysisModeConfig();
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
        <div><span>${escapeHtml(modeConfig.scoreLabel)}</span><strong>${formatScore(getPrimaryScore(item))}</strong></div>
        <div><span>Содержание</span><strong>${formatScore(item.maintenanceScore)}</strong></div>
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
  const modeConfig = getAnalysisModeConfig();
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
          <div class="compare-spec"><span>${escapeHtml(modeConfig.scoreLabel)}</span><strong>${formatScore(getPrimaryScore(item))}</strong></div>
          <div class="compare-spec"><span>Содержание</span><strong>${formatScore(item.maintenanceScore)}</strong></div>
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
  hydrateLazyImages(elements.compareGrid);
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
  renderPriceHistoryPanel(item);
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
  void loadListingSnapshots(item);
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
  updateHiddenArchiveUi();
  renderCollectorStatus();
  updateSmartScopeUi();
  updateAnalysisModeUI();
  const listings = getFilteredListings();
  state.renderedListings = listings;
  renderProfiles();
  populateCities();
  populateMarks();
  populateModels();
  populateAttributeFilters();
  populateArchiveFilters();
  renderStats(listings);
  renderFavorites();
  renderHistory();
  renderKolesaAnalytics(listings);
  renderArchiveAnalytics();
  renderMarket30Analytics();
  renderTopLists(listings);
  renderBars(listings);
  renderTable(listings);
  hydrateLazyImages(document);
  if (state.selectedListingId && !elements.detailModal.hidden) {
    const selected = getListingById(state.selectedListingId);
    if (selected) {
      elements.modalTitle.textContent = selected.title;
      elements.modalPrice.textContent = formatPrice(selected.price);
      elements.modalCity.textContent = selected.city || "-";
      renderModalGallery(selected);
      renderListingFacts(selected);
      renderListingBreakdown(selected);
      elements.modalSignals.innerHTML = renderSignals(selected);
      renderPriceHistoryPanel(selected);
    }
  }
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
  elements.maintenanceFilterSelect.value = "";
  elements.optionSearchInput.value = "";
  elements.sortSelect.value = "score";
  elements.showInactiveToggle.checked = false;
  if (elements.archiveCityFilter) elements.archiveCityFilter.value = "";
  if (elements.archiveBrandFilter) elements.archiveBrandFilter.value = "";
  if (elements.archiveSellerFilter) elements.archiveSellerFilter.value = "";
  state.smartScopeEnabled = true;
  chooseSmartScopeBrand();
  state.currentPage = 1;
  state.showHiddenArchived = false;
  state.tableSort.key = "";
  state.tableSort.direction = "asc";
  render();
}

async function saveListingsToServer(listings) {
  try {
    const response = await fetch("/api/listings", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(listings.map(serializeRowForServer))
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
  const validIds = new Set([...state.listings, ...state.archivedListings].map(item => item.id));
  state.favoriteIds = state.favoriteIds.filter(id => validIds.has(id));
  state.comparisonHistory = state.comparisonHistory
    .map(entry => ({
      ...entry,
      compareIds: Array.isArray(entry.compareIds) ? entry.compareIds.filter(id => validIds.has(id)).slice(0, 3) : []
    }))
    .filter(entry => entry.compareIds.length >= 2);
}

function updateHiddenArchiveUi() {
  const hiddenCount = state.listings.filter(item => item.hiddenAfterImport).length;
  const archiveCount = state.archivedListings.length;
  if (elements.hiddenCountBadge) {
    elements.hiddenCountBadge.textContent = `Скрыто ${formatInteger(hiddenCount)} · Архив ${formatInteger(archiveCount)}`;
  }
  if (elements.toggleHiddenBtn) {
    elements.toggleHiddenBtn.textContent = state.showHiddenArchived
      ? "Скрыть архив"
      : "Показать скрытые";
  }
}

function updateSmartScopeUi() {
  const active = shouldApplySmartScope();
  if (elements.smartScopeBadge) {
    elements.smartScopeBadge.textContent = active
      ? `Быстрый режим: ${state.smartScopeBrand}`
      : "Показаны все марки";
  }
  if (elements.toggleSmartScopeBtn) {
    elements.toggleSmartScopeBtn.textContent = active
      ? "Показать все марки"
      : "Быстрый режим";
  }
}

function formatCollectorMode(value) {
  switch (String(value || "").trim()) {
    case "daily":
      return "Ежедневный";
    case "full":
      return "Вся база";
    case "manual":
      return "Вручную";
    default:
      return "-";
  }
}

function renderCollectorStatus() {
  const collector = state.collectorStatus;
  if (!collector) {
    elements.collectorLastRun.textContent = "-";
    elements.collectorLastResult.textContent = "-";
    elements.collectorLastMode.textContent = "-";
    return;
  }

  elements.collectorLastRun.textContent = collector.last_run_at ? formatDateTime(collector.last_run_at) : "-";
  elements.collectorLastResult.textContent = collector.last_total
    ? `${formatInteger(collector.last_checked)} / ${formatInteger(collector.last_failed)}`
    : "-";
  elements.collectorLastMode.textContent = collector.last_status
    ? `${formatCollectorMode(collector.last_mode)} · ${collector.last_status}`
    : formatCollectorMode(collector.last_mode);
}

async function loadCollectorStatusFromServer() {
  try {
    const response = await fetch("/api/collector/status");
    if (!response.ok) {
      throw new Error("Collector status failed");
    }
    const payload = await response.json();
    state.collectorStatus = payload.collector || null;
  } catch (error) {
    state.collectorStatus = null;
  }
  renderCollectorStatus();
}

async function loadArchivedListingsFromServer() {
  try {
    const response = await fetch("/api/archive/listings");
    if (!response.ok) {
      throw new Error("Archive load failed");
    }
    const payload = await response.json();
    state.archivedListings = normalizeRows(Array.isArray(payload.items) ? payload.items : []);
  } catch (error) {
    state.archivedListings = [];
  }
  updateHiddenArchiveUi();
}

async function loadListingsFromServer() {
  try {
    const response = await fetch("/api/listings");
    if (!response.ok) {
      throw new Error("Load failed");
    }

    const payload = await response.json();
    await loadCollectorStatusFromServer();
    const rows = Array.isArray(payload.items) ? payload.items : [];
    if (rows.length) {
      state.listings = normalizeRows(rows);
      state.smartScopeEnabled = true;
      chooseSmartScopeBrand();
      state.currentPage = 1;
      await loadArchivedListingsFromServer();
      state.compareIds = [];
      state.compareWinnerId = null;
      pruneUserStateToCurrentListings();
      populateCities();
      updateComparePanel();
      render();
      if (isAuthenticated()) {
        void saveAppState();
      }
      setStatus(`Источник: серверные данные, активных ${getActualListingsCount()} из ${state.listings.length} · скрыто ${formatInteger(state.listings.filter(item => item.hiddenAfterImport).length)} · архив ${formatInteger(state.archivedListings.length)}`);
      return;
    }
  } catch (error) {
    setStatus("Источник: демо-данные");
    state.collectorStatus = null;
    renderCollectorStatus();
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
  setStatus(`Источник: ставим импорт в очередь, цель ${limit} объявлений...`);

  try {
    const response = await fetch("/api/jobs/import/kolesa", {
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

    const completedJob = await waitForServerJob(payload.job?.id, {
      onProgress: job => {
        const progressText = job.total > 0 ? `${job.progress || 0}/${job.total}` : "в очереди";
        elements.importKolesaBtn.textContent = job.status === "queued"
          ? "В очереди..."
          : `Импорт ${progressText}`;
        setStatus(`Источник: ${job.message || "идет мягкий импорт"}${job.status === "queued" ? "" : ` (${progressText})`}`);
      }
    });

    await loadListingsFromServer();
    state.compareIds = [];
    state.compareWinnerId = null;
    pruneUserStateToCurrentListings();
    populateCities();
    updateComparePanel();
    resetFilters();
    if (isAuthenticated()) {
      void saveAppState();
    }
    state.importPreview = {
      status: "ready",
      url: trimmedUrl,
      availableCount: Number(
        completedJob.result?.count
        || state.importPreview.availableCount
        || 0
      ),
      totalCount: state.importPreview.totalCount,
      exactTotalKnown: state.importPreview.exactTotalKnown,
      hasMore: state.importPreview.hasMore && Number(completedJob.result?.count || 0) >= 1000,
      note: (() => {
        const summary = completedJob.result?.summary || {};
        const importedCount = Number(summary.imported_count ?? completedJob.result?.count ?? 0);
        const newCount = Number(summary.new_count ?? 0);
        const updatedCount = Number(summary.updated_count ?? 0);
        const duplicateCount = Number(summary.duplicate_count ?? 0);
        const errorCount = Number(summary.error_count ?? completedJob.result?.detailFailures ?? 0);
        return `Импортировано ${formatInteger(importedCount)} · новых ${formatInteger(newCount)} · обновлено ${formatInteger(updatedCount)} · дубликаты ${formatInteger(duplicateCount)} · ошибок ${formatInteger(errorCount)}.`;
      })()
    };
    renderImportPreview();
    const pagesLoaded = Number(completedJob.result?.pagesLoaded) || 1;
    const summary = completedJob.result?.summary || {};
    setStatus(
      `Источник: импортировано ${formatInteger(summary.imported_count ?? completedJob.result?.count ?? 0)}, новых ${formatInteger(summary.new_count ?? 0)}, обновлено ${formatInteger(summary.updated_count ?? 0)}, дубликаты ${formatInteger(summary.duplicate_count ?? 0)}, ошибок ${formatInteger(summary.error_count ?? completedJob.result?.detailFailures ?? 0)} · активных ${getActualListingsCount()} из ${state.listings.length}, ${pagesLoaded} стр.`
    );
  } catch (error) {
    window.alert(error.message || "Не удалось импортировать данные.");
    setStatus("Источник: ошибка импорта");
  } finally {
    setImportBusy(false);
    elements.importKolesaBtn.textContent = "Импортировать";
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
  elements.maintenanceFilterSelect,
  elements.optionSearchInput,
  elements.sortSelect,
  elements.showInactiveToggle
].forEach(element => {
  element.addEventListener("input", () => {
    state.currentPage = 1;
    render();
  });
  element.addEventListener("change", () => {
    state.currentPage = 1;
    render();
  });
});

elements.markFilterSelect.addEventListener("change", () => {
  state.smartScopeEnabled = false;
  state.currentPage = 1;
  populateModels();
  if (![...elements.modelFilterSelect.options].some(option => option.value === elements.modelFilterSelect.value)) {
    elements.modelFilterSelect.value = "";
  }
  render();
});
if (elements.toggleSmartScopeBtn) {
  elements.toggleSmartScopeBtn.addEventListener("click", () => {
    if (shouldApplySmartScope()) {
      state.smartScopeEnabled = false;
    } else {
      state.smartScopeEnabled = true;
      if (!state.smartScopeBrand) {
        chooseSmartScopeBrand();
      }
    }
    state.currentPage = 1;
    render();
  });
}
elements.modeBuyerBtn.addEventListener("click", () => {
  setAnalysisMode("buyer");
});
elements.modeResellerBtn.addEventListener("click", () => {
  setAnalysisMode("reseller");
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
elements.importPreviewBtn.addEventListener("click", requestImportPreview);
elements.collectAllHistoryBtn.addEventListener("click", collectHistoryForAllListings);
elements.importAktauBtn.addEventListener("click", () => {
  elements.importCitySelect.value = "aktau";
  elements.importModelSelect.value = "";
  elements.kolesaUrlInput.dataset.manual = "false";
  syncImportUrlPreview();
  markImportPreviewDirty();
  void importFromKolesaUrl(buildImportUrlFromFilters(), getImportLimit());
});
if (elements.toggleHiddenBtn) {
  elements.toggleHiddenBtn.addEventListener("click", () => {
    state.showHiddenArchived = !state.showHiddenArchived;
    state.currentPage = 1;
    render();
  });
}
if (elements.resultsPrevBtn) {
  elements.resultsPrevBtn.addEventListener("click", () => {
    state.currentPage = Math.max(1, state.currentPage - 1);
    render();
  });
}
if (elements.resultsNextBtn) {
  elements.resultsNextBtn.addEventListener("click", () => {
    state.currentPage += 1;
    render();
  });
}
[
  elements.archiveCityFilter,
  elements.archiveBrandFilter,
  elements.archiveSellerFilter
].filter(Boolean).forEach(element => {
  element.addEventListener("change", () => {
    state.currentPage = 1;
    render();
  });
});
[
  elements.importCitySelect,
  elements.importMarkSelect,
  elements.importModelSelect,
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
    markImportPreviewDirty();
  });
  element.addEventListener("change", () => {
    elements.kolesaUrlInput.dataset.manual = "false";
    syncImportUrlPreview();
    markImportPreviewDirty();
  });
});
elements.importMarkSelect.addEventListener("change", () => {
  void loadImportModelOptions("");
});
elements.importCitySelect.addEventListener("change", () => {
  if (elements.importMarkSelect.value) {
    void loadImportModelOptions("");
  }
});
elements.kolesaUrlInput.addEventListener("input", () => {
  elements.kolesaUrlInput.dataset.manual = elements.kolesaUrlInput.value.trim() ? "true" : "false";
  markImportPreviewDirty();
});
[
  elements.importPriceFromInput,
  elements.importPriceToInput
].forEach(element => {
  element.addEventListener("focus", handleImportPriceFocus);
  element.addEventListener("blur", handleImportPriceBlur);
  element.value = formatMoneyInputValue(element.value);
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
elements.collectHistoryBtn.addEventListener("click", () => {
  void collectHistoryForRenderedListings();
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
  elements.topCreditList,
  elements.topMaintenanceList
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
updateAnalysisModeUI();
elements.kolesaUrlInput.dataset.manual = "false";
syncImportUrlPreview();
markImportPreviewDirty();
updateComparePanel();
Promise.all([loadAppState(), loadListingsFromServer()]).finally(() => {
  render();
});
