// src/job/pricing.jobs.js
import cron from "node-cron";
import { getGlobalPriceRules } from "../services/priceRules.provider.js";
import {
  recomputeAllGroups,
  fixAgesAndMaybeReprice,
} from "../controllers/admin.reprice.controller.js";

/**
 * ⏰ Programa el reproceso diario de precios (cuotaIdeal) para TODOS los grupos.
 * Env vars:
 *  - ENABLE_PRICING_CRON=1           (se recomienda iniciar esto en tu bootstrap)
 *  - PRICING_CRON="10 3 * * *"       (default 03:10 Buenos Aires)
 *  - PRICING_TZ="America/Argentina/Buenos_Aires"
 *  - PRICING_CONCURRENCY=8
 *  - PRICING_CRON_DEBUG=0|1
 */
export function scheduleDailyPricingRecompute() {
  // Evita doble registro si se llama dos veces por error
  if (global.__pricingCronTask) return global.__pricingCronTask;

  const expr = process.env.PRICING_CRON || "10 3 * * *";
  const tz = process.env.PRICING_TZ || "America/Argentina/Buenos_Aires";
  const conc = Number(process.env.PRICING_CONCURRENCY || 8);
  const debugCron = process.env.PRICING_CRON_DEBUG === "1";

  if (!cron.validate(expr)) {
    console.warn(
      `⚠️  PRICING_CRON inválido ("${expr}"). Usando default "10 3 * * *".`
    );
  }

  const task = cron.schedule(
    cron.validate(expr) ? expr : "10 3 * * *",
    async () => {
      try {
        const rules = await getGlobalPriceRules().catch(() => null);
        const base = Number(rules?.base || 16000);
        console.log(
          `⏰ [pricing.cron] Reproceso diario (base=${base}, conc=${conc})…`
        );

        await recomputeAllGroups({
          concurrency: conc,
          debug: false,
        });

        console.log("✅ [pricing.cron] Finalizado.");
      } catch (err) {
        console.error("❌ [pricing.cron] Error:", err?.message || err);
      }
    },
    { timezone: tz, scheduled: true }
  );

  if (debugCron) {
    console.log(
      `⏰ [pricing.cron] Programado expr="${expr}" tz="${tz}" conc=${conc}`
    );
  }

  global.__pricingCronTask = task;
  return task;
}

/**
 * 🛠️ Normaliza 'fechaNac' → recalcula 'edad' → re-precia grupos afectados (o todos).
 * Env vars:
 *  - PRICING_MODE=prueba   → fuerza reprice de TODOS para debugging
 *  - AGE_FORCE_REPRICE_ALL=1  → idem
 *  - PRICING_CONCURRENCY=8
 */
export async function fixAgesAndReprice({
  concurrency = Number(process.env.PRICING_CONCURRENCY || 8),
  logEvery = 200,
} = {}) {
  const forceAll =
    (process.env.PRICING_MODE || "").toLowerCase() === "prueba" ||
    process.env.AGE_FORCE_REPRICE_ALL === "1";

  console.log(`[AGE-FIX] start (forceAll=${forceAll}, conc=${concurrency})…`);
  const r = await fixAgesAndMaybeReprice({
    forceAll,
    concurrency,
    logEvery,
    debug: false,
  });
  console.log("[AGE-FIX] done:", r);
  return r;
}

/**
 * 🛠️ Re-precia SOLO los grupos con 'cuotaIdeal' faltante/0.
 * Env vars:
 *  - PRICING_CONCURRENCY=8
 */
export async function fixZeroPricing({
  concurrency = Number(process.env.PRICING_CONCURRENCY || 8),
  logEvery = 200,
} = {}) {
  const { default: Cliente } = await import("../models/client.model.js");
  console.log("[PRICING-FIX] buscando grupos con cuotaIdeal 0/null…");

  const rows = await Cliente.aggregate([
    {
      $match: {
        $or: [
          { cuotaIdeal: { $exists: false } },
          { cuotaIdeal: null },
          { cuotaIdeal: 0 },
        ],
        idCliente: { $ne: null },
      },
    },
    { $group: { _id: "$idCliente" } },
    { $project: { _id: 0, idCliente: "$_id" } },
  ]).allowDiskUse(true);

  if (!rows.length) {
    console.log("[PRICING-FIX] nada para recalcular. ✅");
    return { ok: true, totalGrupos: 0, procesados: 0, errores: 0 };
  }

  const ids = rows.map((r) => r.idCliente);

  // 👇 ahora tomamos recomputeGroupsByIds desde el controller (no desde el service lite)
  const { recomputeGroupsByIds } = await import(
    "../controllers/admin.reprice.controller.js"
  );

  const r = await recomputeGroupsByIds(ids, {
    concurrency,
    logEvery,
    debug: false,
  });

  console.log("[PRICING-FIX] done:", r);
  return { ok: true, totalGrupos: r.total, ...r };
}

export default {
  scheduleDailyPricingRecompute,
  fixAgesAndReprice,
  fixZeroPricing,
};
