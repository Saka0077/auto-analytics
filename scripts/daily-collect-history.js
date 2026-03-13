const BASE_URL = String(process.env.AUTO_ANALYTICS_BASE_URL || "http://localhost:3000").replace(/\/+$/, "");
const POLL_INTERVAL_MS = 2000;
const POLL_TIMEOUT_MS = 1000 * 60 * 60 * 6;
const HISTORY_BATCH_LIMIT = 400;

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `HTTP ${response.status}`);
  }
  return payload;
}

async function main() {
  const health = await requestJson(`${BASE_URL}/api/health`);
  if (!health.ok) {
    throw new Error("Локальный сервер не готов.");
  }

  const listings = await requestJson(`${BASE_URL}/api/listings`);
  const targets = Array.isArray(listings.items)
    ? [...new Map(
        listings.items
          .filter(item => String(item.source || "").includes("kolesa") && item.url)
          .map(item => [String(item.listing_uid || item.advert_id || item.url), item.url])
      ).values()]
    : [];

  if (!targets.length) {
    console.log("Нет локальных объявлений Kolesa для ежедневного сбора.");
    return;
  }

  let totalChecked = 0;
  let totalFailed = 0;

  for (let index = 0; index < targets.length; index += HISTORY_BATCH_LIMIT) {
    const batch = targets.slice(index, index + HISTORY_BATCH_LIMIT);
    const created = await requestJson(`${BASE_URL}/api/jobs/collect-snapshots`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        urls: batch,
        limit: batch.length,
        concurrency: 1
      })
    });

    const jobId = created?.job?.id;
    if (!jobId) {
      throw new Error("Не удалось создать задачу сбора истории.");
    }

    console.log(`Запущен daily history job ${jobId} на пакет ${index + 1}-${index + batch.length} из ${targets.length}.`);

    const startedAt = Date.now();
    while (Date.now() - startedAt < POLL_TIMEOUT_MS) {
      await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
      const statusPayload = await requestJson(`${BASE_URL}/api/jobs/${jobId}`);
      const job = statusPayload.job || {};
      const progressText = job.total > 0 ? `${job.progress || 0}/${job.total}` : "queued";
      console.log(`[${job.status}] ${job.message || ""} ${progressText}`.trim());

      if (!["queued", "running"].includes(job.status)) {
        if (job.status !== "completed") {
          throw new Error(job.error || `Job finished with status ${job.status}`);
        }

        totalChecked += Number(job.result?.checked || 0);
        totalFailed += Number(job.result?.failed || 0);
        break;
      }
    }

    if (Date.now() - startedAt >= POLL_TIMEOUT_MS) {
      throw new Error("Daily history job не успел завершиться за отведённое время.");
    }
  }

  console.log(`Готово. Обновлено ${totalChecked}, ошибок ${totalFailed}.`);
}

main().catch(error => {
  console.error(error.message || error);
  process.exitCode = 1;
});
