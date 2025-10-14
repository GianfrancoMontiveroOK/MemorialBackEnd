// src/job/pricing.cron.js  (ESM)

import cron from "node-cron";
import { recomputeAllGroups } from "../services/pricing.services.js";
import { getGlobalPriceRules } from "../services/priceRules.provider.js";

/**
 * Programa un reproceso diario de precios (cuotaIdeal) para TODOS los grupos.
 *
 * .env relevantes:
 * - ENABLE_PRICING_CRON=1            → (db.js ya lo controla)
 * - PRICING_CRON="10 3 * * *"        → CRON expr (default 03:10)
 * - PRICING_TZ="America/Argentina/Buenos_Aires"
 * - PRICING_CONCURRENCY=8
 * - PRICING_BASE=16000               → override opcional (si no, usa rules.base)
 * - PRICING_CRON_DEBUG=0|1           → logs extra del cron
 */
export default function scheduleDailyPricingRecompute() {
  // Evita doble registro si llamaran dos veces (defensa extra)
  if (global.__pricingCronTask) {
    if (process.env.PRICING_CRON_DEBUG === "1") {
      console.log("⏰ [pricing.cron] tarea ya estaba programada, skip.");
    }
    return global.__pricingCronTask;
  }

  const expr = process.env.PRICING_CRON || "10 3 * * *"; // 03:10 cada día
  const tz = process.env.PRICING_TZ || "America/Argentina/Buenos_Aires";
  const conc = Number(process.env.PRICING_CONCURRENCY || 8);
  const debugCron = process.env.PRICING_CRON_DEBUG === "1";

  // Validación básica de cron expr
  if (!cron.validate(expr)) {
    console.warn(
      `⚠️ [pricing.cron] PRICING_CRON inválido ("${expr}"). Usando default "10 3 * * *".`
    );
  }

  const task = cron.schedule(
    cron.validate(expr) ? expr : "10 3 * * *",
    async () => {
      try {
        const t0 = Date.now();

        // Leemos reglas para tomar base dinámica (salvo override por env)
        const rules = await getGlobalPriceRules().catch(() => null);
        const baseEnv = Number(process.env.PRICING_BASE || NaN);
        const base = Number.isFinite(baseEnv)
          ? baseEnv
          : Number(rules?.base || 16000);

        console.log(
          `⏰ [pricing.cron] Iniciando reproceso diario de precios (base=${base}, conc=${conc})…`
        );

        await recomputeAllGroups({
          base,
          concurrency: conc,
          debug: false,
        });

        const secs = ((Date.now() - t0) / 1000).toFixed(1);
        console.log(`✅ [pricing.cron] Finalizado en ${secs}s.`);
      } catch (err) {
        console.error(
          "❌ [pricing.cron] Error en tarea diaria:",
          err?.message || err
        );
      }
    },
    { timezone: tz, scheduled: true }
  );

  if (debugCron) {
    console.log(
      `⏰ [pricing.cron] Programado con expr="${expr}", tz="${tz}", conc=${conc}`
    );
  }

  // Guardamos referencia global para evitar re-programar
  global.__pricingCronTask = task;

  return task;
}
