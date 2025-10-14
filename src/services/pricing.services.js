// src/services/pricing.services.js  (ESM)

import Cliente from "../models/client.model.js";
import { getGlobalPriceRules } from "./priceRules.provider.js";
import { computeCuotaIdealWith } from "./pricing.engine.js";

/* ===================== Helpers ===================== */

function ageFromDate(d) {
  if (!d) return undefined;
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return undefined;
  const today = new Date();
  let a = today.getFullYear() - dt.getFullYear();
  const m = today.getMonth() - dt.getMonth();
  if (m < 0 || (m === 0 && today.getDate() < dt.getDate())) a--;
  return a >= 0 ? a : undefined;
}

// Inactivo solo si baja es Date válido o activo es falso explícito
function isActive(member) {
  if (member?.baja) {
    const d = new Date(member.baja);
    if (!Number.isNaN(d.getTime())) return false;
  }
  const v = member?.activo;
  if (
    v === false ||
    v === 0 ||
    v === "0" ||
    String(v).toLowerCase() === "false"
  )
    return false;
  return true;
}

// Filtro robusto por idCliente (num o string de mismo valor)
function buildGroupFilter(idKey) {
  const t = String(idKey).trim();
  const n = Number(t);
  const or = [];
  if (!Number.isNaN(n)) or.push({ idCliente: n });
  or.push({
    $expr: { $eq: [{ $trim: { input: { $toString: "$idCliente" } } }, t] },
  });
  return { $or: or };
}

/* ========================================================================
   1) Núcleo: recalcular 'cuotaIdeal' de TODO el grupo (nuevo modelo)
      - Integrantes activos = count de miembros activos
      - edadMax = máximo de edades válidas de activos
      - cremaciones = count de activos con { cremacion: true }
      - Reglas SIEMPRE desde Settings (getGlobalPriceRules)
      - Nota: el parámetro 'base' se ignora (compat con llamadas viejas)
   ======================================================================== */
export async function recomputeGroupPricing(
  idClienteKey,
  { debug = false } = {}
) {
  const filter = buildGroupFilter(idClienteKey);

  const members = await Cliente.aggregate([
    { $match: filter },
    {
      $project: {
        _id: 1,
        idCliente: 1,
        activo: 1,
        baja: 1,
        fechaNac: 1,
        edad: 1,
        cremacion: 1, // boolean nuevo
        rol: 1,
      },
    },
  ]).allowDiskUse(true);

  const total = members.length;
  const activosArr = members.filter(isActive);
  const integrantes = activosArr.length;

  const edades = activosArr
    .map((m) => {
      const a = ageFromDate(m.fechaNac);
      return typeof a === "number"
        ? a
        : typeof m.edad === "number"
        ? m.edad
        : undefined;
    })
    .filter((a) => typeof a === "number");

  const edadMax = edades.length ? Math.max(...edades) : 0;
  const cremaciones = activosArr.reduce(
    (acc, m) => acc + (m?.cremacion ? 1 : 0),
    0
  );

  // Reglas dinámicas desde Settings
  const rules = await getGlobalPriceRules();
  const BASE = Number(rules.base ?? 16000);

  const cuotaIdeal =
    integrantes > 0
      ? computeCuotaIdealWith(
          { base: BASE, integrantes, edadMax, cremCount: cremaciones },
          rules
        )
      : 0;

  // 1) Actualizar cuotaIdeal en TODO el grupo
  const res = await Cliente.updateMany(filter, {
    $set: { cuotaIdeal, updatedAt: new Date() },
  });
  const matched = res?.matchedCount ?? 0;
  const modified = res?.modifiedCount ?? 0;

  // 2) Actualizar edadMaxPoliza del TITULAR (fallback: todo el grupo si no hay titular)
  const titularFilter = { $and: [filter, { rol: "TITULAR" }] };
  const updTitular = await Cliente.updateMany(titularFilter, {
    $set: { edadMaxPoliza: edadMax, updatedAt: new Date() },
  });

  const titularMatched = updTitular?.matchedCount ?? 0;
  const titularModified = updTitular?.modifiedCount ?? 0;

  if (titularMatched === 0) {
    // No hay titular explícito → escribimos edadMaxPoliza en todos para consistencia
    await Cliente.updateMany(filter, {
      $set: { edadMaxPoliza: edadMax, updatedAt: new Date() },
    });
  }

  if (debug) {
    console.log("[pricing] grupo:", idClienteKey, {
      encontrados: total,
      activos: integrantes,
      edadMax,
      cremaciones,
      cuotaIdeal,
      matched,
      modified,
      titularMatched,
      titularModified,
      sample: members.slice(0, 3),
    });
  }

  return {
    ok: true,
    id: idClienteKey,
    cuotaIdeal,
    integrantes,
    edadMax,
    cremaciones,
    matched,
    modified,
    titularMatched,
    titularModified,
  };
}

/* ========================================================================
   2) Orquestador: recalcular por lote de grupos (ids)
      - Con barra de progreso (TTY) o logs periódicos
      - Solo usa PRICING_MODE=prueba para activar debug verboso
      - NO usa PRICING_BASE ni otras .env de pricing
   ======================================================================== */
export async function recomputeGroupsByIds(
  ids = [],
  {
    concurrency = 8,
    barWidth = 30,
    logEvery = 200,
    debug = (process.env.PRICING_MODE || "").toLowerCase() === "prueba",
  } = {}
) {
  const uniq = [...new Set(ids.map((x) => String(x).trim()).filter(Boolean))];
  const total = uniq.length;

  let cursor = 0;
  let procesados = 0;
  let errores = 0;
  let modifiedTotal = 0;
  let matchedTotal = 0;

  const t0 = Date.now();
  const isTTY = !!(process.stdout && process.stdout.isTTY);
  const fmt = (n) => n.toLocaleString("es-AR");

  function render(final = false) {
    const pct = total ? procesados / total : 1;
    const pctText = (pct * 100).toFixed(1) + "%";
    const elapsed = (Date.now() - t0) / 1000;
    const rate = procesados > 0 ? procesados / elapsed : 0;
    const remain = Math.max(0, total - procesados);
    const etaSec = rate > 0 ? remain / rate : 0;
    const eta =
      etaSec > 3600
        ? `${Math.floor(etaSec / 3600)}h ${Math.floor((etaSec % 3600) / 60)}m`
        : etaSec > 60
        ? `${Math.floor(etaSec / 60)}m ${Math.floor(etaSec % 60)}s`
        : `${Math.floor(etaSec)}s`;

    if (isTTY) {
      const filled = Math.round(barWidth * pct);
      const bar =
        "█".repeat(filled) + "░".repeat(Math.max(0, barWidth - filled));
      const line = ` ${bar} ${pctText} | ${fmt(procesados)}/${fmt(
        total
      )} · mod=${fmt(modifiedTotal)} · err=${fmt(errores)} · ETA ${eta} `;
      process.stdout.clearLine(0);
      process.stdout.cursorTo(0);
      process.stdout.write(line);
      if (final) process.stdout.write("\n");
    } else if (final || procesados % logEvery === 0) {
      console.log(
        `[pricing#byIds] ${procesados}/${total} (${pctText}) · mod=${fmt(
          modifiedTotal
        )} · err=${fmt(errores)} · ETA ${eta}`
      );
    }
  }

  let intervalId = null;
  if (isTTY) intervalId = setInterval(() => render(false), 300);

  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= total) break;
      const id = uniq[idx];
      try {
        const r = await recomputeGroupPricing(id, { debug });
        matchedTotal += r?.matched ?? 0;
        modifiedTotal += r?.modified ?? 0;
      } catch (e) {
        errores++;
        if (debug) console.error("[pricing#byIds] ERROR", id, e?.message || e);
      } finally {
        procesados++;
        if (!isTTY && procesados % logEvery === 0) render(false);
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.max(1, concurrency) }, () => worker())
  );

  if (intervalId) clearInterval(intervalId);
  render(true);

  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  if (debug) {
    console.log(
      `[pricing#byIds] FIN en ${secs}s · total=${fmt(total)} · matched=${fmt(
        matchedTotal
      )} · modified=${fmt(modifiedTotal)} · errores=${fmt(errores)}`
    );
  }

  return { ok: true, total, procesados, matchedTotal, modifiedTotal, errores };
}

/* ========================================================================
   3) Orquestador: recalcular TODOS los grupos
      - distinct idCliente y delega en recomputeGroupsByIds
      - Solo PRICING_MODE=prueba para debug
   ======================================================================== */
export async function recomputeAllGroups(opts = {}) {
  const ids = await Cliente.distinct("idCliente", { idCliente: { $ne: null } });
  const uniq = [...new Set(ids.map((x) => String(x).trim()).filter(Boolean))];

  const concurrency = "concurrency" in opts ? opts.concurrency : 8;
  const barWidth = "barWidth" in opts ? opts.barWidth : 30;
  const logEvery = "logEvery" in opts ? opts.logEvery : 200;
  const debug =
    "debug" in opts
      ? !!opts.debug
      : (process.env.PRICING_MODE || "").toLowerCase() === "prueba";

  console.log(
    `[recomputeAllGroups] Recalculando ${uniq.length.toLocaleString(
      "es-AR"
    )} grupos (conc=${concurrency})…`
  );

  return recomputeGroupsByIds(uniq, { concurrency, barWidth, logEvery, debug });
}

/* ========================================================================
   4) Age fix dentro del servicio (opcional):
      - Normaliza 'fechaNac' (string -> Date) y recalcula 'edad'
      - Re-precia TODOS o SOLO grupos afectados
      - PRICING_MODE=prueba => forceAll=true y debug verboso
      - Reglas SIEMPRE desde Settings (no se pisa base)
   ======================================================================== */
export async function fixAgesAndMaybeReprice({
  forceAll = (process.env.PRICING_MODE || "").toLowerCase() === "prueba",
  concurrency = 8,
  barWidth = 30,
  logEvery = 200,
  debug = (process.env.PRICING_MODE || "").toLowerCase() === "prueba",
} = {}) {
  const now = new Date();
  const updRes = await Cliente.updateMany(
    { fechaNac: { $exists: true, $ne: null } },
    [
      {
        $set: {
          _fecTmp: {
            $switch: {
              branches: [
                {
                  case: { $eq: [{ $type: "$fechaNac" }, "date"] },
                  then: "$fechaNac",
                },
                {
                  case: {
                    $and: [
                      { $eq: [{ $type: "$fechaNac" }, "string"] },
                      {
                        $regexMatch: {
                          input: "$fechaNac",
                          regex: "^\\d{1,2}/\\d{1,2}/\\d{2,4}$",
                        },
                      },
                    ],
                  },
                  then: {
                    $let: {
                      vars: {
                        yyyy: {
                          $cond: [
                            {
                              $gte: [
                                {
                                  $strLenCP: {
                                    $toString: {
                                      $arrayElemAt: [
                                        { $split: ["$fechaNac", "/"] },
                                        2,
                                      ],
                                    },
                                  },
                                },
                                4,
                              ],
                            },
                            {
                              $dateFromString: {
                                dateString: "$fechaNac",
                                format: "%d/%m/%Y",
                              },
                            },
                            {
                              $dateFromString: {
                                dateString: "$fechaNac",
                                format: "%d/%m/%y",
                              },
                            },
                          ],
                        },
                      },
                      in: "$$yyyy",
                    },
                  },
                },
              ],
              default: {
                $convert: {
                  input: "$fechaNac",
                  to: "date",
                  onError: null,
                  onNull: null,
                },
              },
            },
          },
        },
      },
      {
        $set: {
          edad: {
            $let: {
              vars: {
                years: {
                  $cond: [
                    { $ne: ["$_fecTmp", null] },
                    {
                      $dateDiff: {
                        startDate: "$_fecTmp",
                        endDate: now,
                        unit: "year",
                      },
                    },
                    null,
                  ],
                },
              },
              in: {
                $cond: [
                  {
                    $and: [
                      { $ne: ["$$years", null] },
                      { $gte: ["$$years", 0] },
                      { $lte: ["$$years", 120] },
                    ],
                  },
                  "$$years",
                  null,
                ],
              },
            },
          },
        },
      },
      { $unset: "_fecTmp" },
    ]
  );

  if (debug) {
    console.log(
      `[AGE-FIX][svc] matched=${updRes?.matchedCount ?? 0}, modified=${
        updRes?.modifiedCount ?? 0
      }`
    );
  }

  let ids;
  if (forceAll) {
    ids = await Cliente.distinct("idCliente", { idCliente: { $ne: null } });
  } else {
    const groups = await Cliente.aggregate([
      {
        $match: {
          idCliente: { $ne: null },
          fechaNac: { $exists: true, $ne: null },
        },
      },
      { $group: { _id: "$idCliente" } },
      { $project: { _id: 0, idCliente: "$_id" } },
    ]).allowDiskUse(true);
    ids = groups.map((g) => g.idCliente);
  }

  return recomputeGroupsByIds(ids, {
    concurrency,
    barWidth,
    logEvery,
    debug,
  });
}

export default {
  recomputeGroupPricing,
  recomputeGroupsByIds,
  recomputeAllGroups,
  fixAgesAndMaybeReprice,
};
