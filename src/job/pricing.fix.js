// src/job/pricing.fix.js
import Cliente from "../models/client.model.js";
import { recomputeGroupsByIds } from "../services/pricing.services.js";

/**
 * Recalcula cuotaIdeal SOLO para grupos con cuotaIdeal 0/null/ausente.
 */
export default async function fixZeroPricing({
  concurrency = Number(process.env.PRICING_CONCURRENCY || 8),
  base = Number(process.env.PRICING_BASE || 16000),
  logEvery = 200,
} = {}) {
  console.log(`[PRICING-FIX] buscando grupos con cuotaIdeal 0/null…`);
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
  const r = await recomputeGroupsByIds(ids, {
    base,
    concurrency,
    debug: false,
  });
  console.log("[PRICING-FIX] done:", r);
  return { ok: true, totalGrupos: r.total, ...r };
}
