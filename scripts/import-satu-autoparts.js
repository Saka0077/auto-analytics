const fs = require("fs");
const path = require("path");
const cheerio = require("cheerio");

const ROOT = path.resolve(__dirname, "..");
const DEFAULT_SEEDS = path.join(ROOT, "catalog", "satu-autoparts-seeds.json");
const DEFAULT_OUTPUT = path.join(ROOT, "catalog", "satu-autoparts-market.json");
const TEMPLATE_PATH = path.join(ROOT, "catalog", "satu-autoparts-template.json");

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function normalizeLookup(value) {
  return normalizeWhitespace(value)
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim();
}

function slugify(value) {
  return normalizeLookup(value).replace(/\s+/g, "-");
}

function toNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const numeric = Number(String(value).replace(",", ".").replace(/[^\d.-]/g, ""));
  return Number.isFinite(numeric) ? numeric : null;
}

function dedupe(values) {
  return [...new Set(values.filter(Boolean))];
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf-8"));
}

function createEmptyPayload(template) {
  return {
    meta: {
      source: normalizeWhitespace(template?.meta?.source || "satu.kz"),
      version: Number.isFinite(Number(template?.meta?.version)) ? Number(template.meta.version) : 1,
      currency: normalizeWhitespace(template?.meta?.currency || "KZT"),
      captured_at: new Date().toISOString(),
      notes: normalizeWhitespace(template?.meta?.notes)
    },
    part_types: Array.isArray(template.part_types) ? template.part_types : [],
    offers: [],
    fitment_map: [],
    market_summary: []
  };
}

function extractJsonLd($) {
  const items = [];
  $('script[type="application/ld+json"]').each((_, element) => {
    const raw = $(element).html();
    if (!raw) {
      return;
    }

    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        items.push(...parsed);
      } else if (parsed && typeof parsed === "object") {
        items.push(parsed);
      }
    } catch (_) {
      // Ignore invalid blocks.
    }
  });
  return items;
}

function flattenJsonLd(items) {
  const flattened = [];
  for (const item of items) {
    if (!item || typeof item !== "object") {
      continue;
    }

    if (Array.isArray(item["@graph"])) {
      flattened.push(...flattenJsonLd(item["@graph"]));
      continue;
    }

    flattened.push(item);
  }
  return flattened;
}

function findJsonLdEntity(items, typeName) {
  const lowered = String(typeName || "").toLowerCase();
  return items.find(item => {
    const typeValue = item?.["@type"];
    if (Array.isArray(typeValue)) {
      return typeValue.some(type => String(type || "").toLowerCase() === lowered);
    }
    return String(typeValue || "").toLowerCase() === lowered;
  }) || null;
}

function firstText($, selectors) {
  for (const selector of selectors) {
    const value = normalizeWhitespace($(selector).first().text());
    if (value) {
      return value;
    }
  }
  return "";
}

function firstAttr($, selectors, attrName) {
  for (const selector of selectors) {
    const value = normalizeWhitespace($(selector).first().attr(attrName));
    if (value) {
      return value;
    }
  }
  return "";
}

function extractArticle(text) {
  const match = String(text || "").match(/\b(?:артикул|article|арт\.?|sku|код)\s*[:#]?\s*([A-Z0-9._/-]{3,})/i);
  return match?.[1] ? normalizeWhitespace(match[1]) : "";
}

function parseAvailability(value) {
  const normalized = normalizeLookup(value);
  if (!normalized) {
    return "unknown";
  }
  if (/instock|в наличии|есть в наличии|available/.test(normalized)) {
    return "in_stock";
  }
  if (/outofstock|нет в наличии|под заказ|ожидается/.test(normalized)) {
    return "out_of_stock";
  }
  return "unknown";
}

function extractCity(text) {
  const match = String(text || "").match(/\b(Алматы|Астана|Актау|Актобе|Шымкент|Караганда|Костанай|Павлодар|Уральск|Атырау|Тараз|Кызылорда|Семей|Талдыкорган|Туркестан|Петропавловск|Кокшетау|Экибастуз)\b/i);
  return match ? normalizeWhitespace(match[1]) : "";
}

function buildOfferId(seed, offer) {
  const brandBits = [
    seed.make,
    seed.model,
    seed.generation,
    offer.article || offer.sku || offer.brand_name,
    offer.seller_name || "seller"
  ].filter(Boolean).map(slugify);

  return `satu:${seed.part_type}:${brandBits.join(":")}`;
}

function parseOfferPage(html, url, seed) {
  const $ = cheerio.load(html);
  const jsonLd = flattenJsonLd(extractJsonLd($));
  const product = findJsonLdEntity(jsonLd, "Product");
  const organization = findJsonLdEntity(jsonLd, "Organization");
  const offerNode = Array.isArray(product?.offers) ? product.offers[0] : product?.offers || null;

  const metaTitle = firstAttr($, ['meta[property="og:title"]', 'meta[name="twitter:title"]'], "content");
  const metaImage = firstAttr($, ['meta[property="og:image"]', 'meta[name="twitter:image"]'], "content");
  const metaDescription = firstAttr($, ['meta[property="og:description"]', 'meta[name="description"]'], "content");

  const title = normalizeWhitespace(
    product?.name ||
    metaTitle ||
    firstText($, ["h1", "[data-qaid='product_name']", ".b-product__title"])
  );

  const priceText = normalizeWhitespace(
    offerNode?.price ||
    firstAttr($, ['meta[property="product:price:amount"]'], "content") ||
    firstText($, [
      "[data-qaid='product_price']",
      ".x-price-primary",
      ".x-price",
      ".b-product__price"
    ])
  );

  const sellerName = normalizeWhitespace(
    organization?.name ||
    firstText($, [
      "[data-qaid='company_name']",
      ".x-company-name",
      ".b-seller-block__title",
      ".b-product-company__name"
    ])
  );

  const availabilityRaw = normalizeWhitespace(
    offerNode?.availability ||
    firstText($, [
      "[data-qaid='product_presence']",
      ".x-product-status",
      ".b-product-availability"
    ])
  );

  const descriptionRaw = normalizeWhitespace(
    metaDescription ||
    product?.description ||
    firstText($, [
      "[data-qaid='product_description']",
      ".b-product__description",
      ".x-product-description"
    ])
  );

  const pageText = normalizeWhitespace($("body").text());
  const article = normalizeWhitespace(
    product?.mpn ||
    product?.sku ||
    extractArticle(pageText)
  );

  const brandName = normalizeWhitespace(
    product?.brand?.name ||
    product?.brand ||
    firstText($, [
      "[data-qaid='product_brand']",
      ".x-brand-name",
      ".b-product__brand"
    ])
  );

  const city = normalizeWhitespace(
    firstText($, [
      "[data-qaid='company_address']",
      ".x-company-address",
      ".b-seller-block__address"
    ]) || extractCity(pageText)
  );

  const image = normalizeWhitespace(
    product?.image?.[0] ||
    product?.image ||
    metaImage ||
    firstAttr($, ["img"], "src")
  );

  const deliveryInfo = normalizeWhitespace(firstText($, [
    "[data-qaid='delivery_block']",
    ".x-delivery",
    ".b-delivery"
  ]));

  const offer = {
    source: "satu.kz",
    url,
    captured_at: new Date().toISOString(),
    title,
    price_kzt: toNumber(priceText),
    currency: normalizeWhitespace(offerNode?.priceCurrency || "KZT") || "KZT",
    seller_name: sellerName,
    seller_type: "shop",
    city,
    availability: parseAvailability(availabilityRaw),
    stock_count: null,
    brand_name: brandName,
    sku: normalizeWhitespace(product?.sku || ""),
    article,
    manufacturer_code: normalizeWhitespace(product?.mpn || ""),
    condition: "new",
    image,
    delivery_info: deliveryInfo,
    fitment_raw: normalizeWhitespace(seed.fitment_raw || descriptionRaw || title),
    description_raw: descriptionRaw,
    part_type: seed.part_type
  };

  offer.id = buildOfferId(seed, offer);
  return offer;
}

function buildFitment(seed, offerId) {
  return {
    offer_id: offerId,
    make: normalizeWhitespace(seed.make),
    model: normalizeWhitespace(seed.model),
    generation: normalizeWhitespace(seed.generation),
    year_from: Number.isFinite(Number(seed.year_from)) ? Number(seed.year_from) : null,
    year_to: Number.isFinite(Number(seed.year_to)) ? Number(seed.year_to) : null,
    engine: normalizeWhitespace(seed.engine),
    body_type: normalizeWhitespace(seed.body_type),
    part_type: normalizeWhitespace(seed.part_type),
    fitment_confidence: normalizeWhitespace(seed.fitment_confidence || "manual")
  };
}

function median(values) {
  if (!values.length) {
    return null;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function summarizeMarket(offers, fitments) {
  const fitmentByOffer = new Map(fitments.map(item => [item.offer_id, item]));
  const groups = new Map();

  for (const offer of offers) {
    const fitment = fitmentByOffer.get(offer.id);
    if (!fitment) {
      continue;
    }

    const key = [
      fitment.make,
      fitment.model,
      fitment.generation,
      fitment.year_from || "",
      fitment.year_to || "",
      fitment.engine,
      fitment.body_type,
      fitment.part_type
    ].join("|");

    if (!groups.has(key)) {
      groups.set(key, {
        ...fitment,
        prices: [],
        cities: [],
        total: 0,
        inStock: 0
      });
    }

    const group = groups.get(key);
    group.total += 1;
    if (Number.isFinite(offer.price_kzt)) {
      group.prices.push(offer.price_kzt);
    }
    if (offer.availability === "in_stock") {
      group.inStock += 1;
    }
    if (offer.city) {
      group.cities.push(offer.city);
    }
  }

  return [...groups.values()].map(group => {
    const prices = group.prices.filter(Number.isFinite);
    const avg = prices.length ? prices.reduce((sum, value) => sum + value, 0) / prices.length : null;
    const min = prices.length ? Math.min(...prices) : null;
    const max = prices.length ? Math.max(...prices) : null;
    const med = median(prices);

    return {
      make: group.make,
      model: group.model,
      generation: group.generation,
      year_from: group.year_from,
      year_to: group.year_to,
      engine: group.engine,
      body_type: group.body_type,
      part_type: group.part_type,
      offers_count: group.total,
      min_price_kzt: min,
      median_price_kzt: med,
      max_price_kzt: max,
      avg_price_kzt: avg === null ? null : Math.round(avg),
      cities: dedupe(group.cities),
      availability_score: group.total ? Number((group.inStock / group.total).toFixed(2)) : null,
      cheapness_score: (min && med) ? Number((min / med).toFixed(2)) : null,
      updated_at: new Date().toISOString()
    };
  });
}

async function fetchHtml(url) {
  const response = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
      "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"
    }
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return response.text();
}

async function main() {
  const seedsPath = path.resolve(process.argv[2] || DEFAULT_SEEDS);
  const outputPath = path.resolve(process.argv[3] || DEFAULT_OUTPUT);

  if (!fs.existsSync(TEMPLATE_PATH)) {
    console.error(`Template not found: ${TEMPLATE_PATH}`);
    process.exit(1);
  }

  if (!fs.existsSync(seedsPath)) {
    console.error(`Seeds not found: ${seedsPath}`);
    process.exit(1);
  }

  const template = readJson(TEMPLATE_PATH);
  const payload = createEmptyPayload(template);
  const seedsData = readJson(seedsPath);
  const seeds = Array.isArray(seedsData.seeds) ? seedsData.seeds : [];

  if (!seeds.length) {
    fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2), "utf-8");
    console.log(`No Satu seeds found in ${seedsPath}. Wrote empty payload to ${outputPath}`);
    return;
  }

  const errors = [];

  for (const seed of seeds) {
    if (!seed?.url || !seed?.part_type || !seed?.make || !seed?.model) {
      errors.push({ url: seed?.url || "", error: "Missing required seed fields" });
      continue;
    }

    try {
      const html = await fetchHtml(seed.url);
      const offer = parseOfferPage(html, seed.url, seed);
      const fitment = buildFitment(seed, offer.id);
      payload.offers.push(offer);
      payload.fitment_map.push(fitment);
      await new Promise(resolve => setTimeout(resolve, 1500));
    } catch (error) {
      errors.push({ url: seed.url, error: error.message });
    }
  }

  payload.meta.captured_at = new Date().toISOString();
  if (errors.length) {
    payload.meta.notes = `${payload.meta.notes} Import completed with ${errors.length} errors.`;
    payload.meta.errors = errors;
  }

  payload.market_summary = summarizeMarket(payload.offers, payload.fitment_map);

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2), "utf-8");
  console.log(`Imported ${payload.offers.length} Satu offers to ${outputPath}`);
  if (errors.length) {
    console.log(`Errors: ${errors.length}`);
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
