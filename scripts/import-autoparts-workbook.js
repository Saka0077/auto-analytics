const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const ROOT = path.resolve(__dirname, "..");
const DEFAULT_INPUT = path.join(process.env.USERPROFILE || "C:\\Users\\Sayat", "Downloads", "kz_autoparts_market_snapshot_2026_03_12(1).xlsx");
const DEFAULT_OUTPUT = path.join(ROOT, "catalog", "autoparts-catalog.json");

function normalizeLookup(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/g, " ");
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

function splitSegmentYears(rawValue) {
  const value = String(rawValue || "").trim();
  const [segment, years] = value.split("/").map(part => String(part || "").trim());
  return {
    segment: segment || "",
    years: years || ""
  };
}

function splitModelLabel(rawLabel) {
  const label = String(rawLabel || "").trim();
  if (!label) {
    return {
      brand: "",
      aliases: [],
      primaryModel: "",
      normalizedBrand: "",
      lookupKeys: []
    };
  }

  const [brandToken, ...restTokens] = label.split(/\s+/);
  const brand = brandToken || "";
  const modelPart = restTokens.join(" ").trim();
  const aliases = modelPart
    .split("/")
    .map(part => String(part || "").trim())
    .filter(Boolean);
  const primaryModel = aliases[0] || modelPart || "";
  const normalizedBrand = normalizeLookup(brand);
  const modelKeys = aliases.map(normalizeLookup).filter(Boolean);
  const lookupKeys = [...new Set([
    ...modelKeys,
    ...aliases.map(alias => normalizeLookup(`${brand} ${alias}`)).filter(Boolean)
  ])];

  return {
    brand,
    aliases,
    primaryModel,
    normalizedBrand,
    lookupKeys
  };
}

function getMaintenanceBand(score) {
  if (!Number.isFinite(score)) {
    return { key: "unknown", label: "Неизвестно" };
  }

  if (score >= 90) {
    return { key: "cheap", label: "Дешёвые запчасти" };
  }

  if (score >= 75) {
    return { key: "medium", label: "Средние запчасти" };
  }

  return { key: "expensive", label: "Дорогие запчасти" };
}

function trimObjectStrings(record) {
  return Object.fromEntries(
    Object.entries(record).map(([key, value]) => [key, typeof value === "string" ? value.trim() : value])
  );
}

function parseSnapshotSheet(sheet) {
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  const title = String(rows[0]?.[0] || "").trim();
  const note = String(rows[1]?.[0] || "").trim();
  const header = rows[3] || [];

  const items = rows
    .slice(4)
    .filter(row => String(row[0] || "").trim())
    .filter(row => !/^короткий вывод/i.test(String(row[0] || "").trim()))
    .filter(row => toNumber(row[6]) !== null || toNumber(row[9]) !== null)
    .map(row => {
      const modelLabel = String(row[0] || "").trim();
      const yearsMeta = splitSegmentYears(row[1]);
      const modelMeta = splitModelLabel(modelLabel);
      const cheapnessScore = toNumber(row[9]);
      const band = getMaintenanceBand(cheapnessScore);

      return trimObjectStrings({
        id: slugify(modelLabel),
        model_label: modelLabel,
        brand: modelMeta.brand,
        brand_key: modelMeta.normalizedBrand,
        primary_model: modelMeta.primaryModel,
        model_aliases: modelMeta.aliases,
        lookup_keys: modelMeta.lookupKeys,
        segment: yearsMeta.segment,
        years: yearsMeta.years,
        front_pads_price_kzt: toNumber(row[2]),
        front_pads_stock: toNumber(row[3]),
        front_disc_price_kzt: toNumber(row[4]),
        front_disc_stock: toNumber(row[5]),
        service_basket_kzt: toNumber(row[6]),
        avg_stock: toNumber(row[7]),
        price_score: toNumber(row[8]),
        cheapness_score: cheapnessScore,
        priority: toNumber(row[10]),
        maintenance_band: band.key,
        maintenance_label: band.label,
        comment: String(row[11] || "").trim(),
        market_source_url: String(row[12] || "").trim(),
        pads_source_url: String(row[13] || "").trim(),
        disc_source_url: String(row[14] || "").trim()
      });
    });

  const conclusions = rows
    .slice(10)
    .map(row => String(row[0] || "").trim())
    .filter(Boolean)
    .filter(line => !/^короткий вывод/i.test(line));

  return { title, note, items, conclusions };
}

function parseMarketContextSheet(sheet) {
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  return rows
    .slice(2)
    .map(row => trimObjectStrings({
      metric: String(row[0] || "").trim(),
      value: String(row[1] || "").trim(),
      source_url: String(row[2] || "").trim()
    }))
    .filter(item => item.metric);
}

function parseTemplateSheet(sheet) {
  const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: "" });
  return rows
    .slice(2)
    .filter(row => String(row[0] || "").trim())
    .filter(row => !/^check_date$/i.test(String(row[0] || "").trim()))
    .filter(row => String(row[3] || "").trim() || toNumber(row[12]) !== null)
    .map(row => {
      const modelLabel = String(row[4] || "").trim();
      const modelMeta = splitModelLabel(modelLabel);
      return trimObjectStrings({
        check_date: String(row[0] || "").trim(),
        city: String(row[1] || "").trim(),
        source_name: String(row[2] || "").trim(),
        source_url: String(row[3] || "").trim(),
        model_label: modelLabel,
        brand: modelMeta.brand,
        brand_key: modelMeta.normalizedBrand,
        primary_model: modelMeta.primaryModel,
        model_aliases: modelMeta.aliases,
        lookup_keys: modelMeta.lookupKeys,
        generation: String(row[5] || "").trim(),
        years: String(row[6] || "").trim(),
        engine: String(row[7] || "").trim(),
        part_group: String(row[8] || "").trim(),
        part_name: String(row[9] || "").trim(),
        part_brand: String(row[10] || "").trim(),
        sku: String(row[11] || "").trim(),
        price_kzt: toNumber(row[12]),
        stock_qty: toNumber(row[13]),
        delivery_days: String(row[14] || "").trim(),
        warranty_days: toNumber(row[15]),
        notes: String(row[16] || "").trim()
      });
    });
}

function main() {
  const inputPath = path.resolve(process.argv[2] || DEFAULT_INPUT);
  const outputPath = path.resolve(process.argv[3] || DEFAULT_OUTPUT);

  if (!fs.existsSync(inputPath)) {
    console.error(`Workbook not found: ${inputPath}`);
    process.exit(1);
  }

  const workbook = XLSX.readFile(inputPath);
  const snapshot = parseSnapshotSheet(workbook.Sheets.snapshot);
  const marketContext = parseMarketContextSheet(workbook.Sheets.market_context);
  const observations = parseTemplateSheet(workbook.Sheets.data_template);

  const payload = {
    generated_at: new Date().toISOString(),
    source_workbook: path.basename(inputPath),
    snapshot_title: snapshot.title,
    snapshot_note: snapshot.note,
    snapshot_items: snapshot.items,
    snapshot_conclusions: snapshot.conclusions,
    market_context: marketContext,
    observations
  };

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2), "utf-8");
  console.log(`Imported ${snapshot.items.length} snapshot rows and ${observations.length} observations to ${outputPath}`);
}

main();
