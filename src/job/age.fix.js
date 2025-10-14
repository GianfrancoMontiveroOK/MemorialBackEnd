// src/job/age.fix.js
import { fixAgesAndMaybeReprice } from "../services/pricing.services.js";

/**
 * Normaliza fechaNac → edad y re-precia:
 * - Forzar TODOS los grupos: PRICING_MODE=prueba o AGE_FORCE_REPRICE_ALL=1
 */
export default async function fixAgesAndReprice({
  concurrency = Number(process.env.PRICING_CONCURRENCY || 8),
  base = Number(process.env.PRICING_BASE || 16000),
  logEvery = 200,
} = {}) {
  const forceAll =
    (process.env.PRICING_MODE || "").toLowerCase() === "prueba" ||
    process.env.AGE_FORCE_REPRICE_ALL === "1";

  console.log(`[AGE-FIX] start (forceAll=${forceAll})…`);
  const r = await fixAgesAndMaybeReprice({
    forceAll,
    base,
    concurrency,
    logEvery,
    debug: false,
  });
  console.log("[AGE-FIX] done:", r);
  return r;
}
