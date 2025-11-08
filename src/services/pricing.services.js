// src/services/pricing.services.js  (ESM)

import Cliente from "../models/client.model.js";
import { getGlobalPriceRules } from "./priceRules.provider.js";

/* ============================================================================
   ENGINE — helpers puros + cálculo de cuota
   ========================================================================== */

export function round500(x) {
  const n = Number(x) || 0;
  const base = Math.floor(n / 500) * 500;
  return n - base >= 250 ? base + 500 : base;
}

export function membersFactor(n, groupCfg) {
  const m = Math.max(1, Number(n) || 1);
  const { neutralAt = 4, step = 0.25, minMap = {} } = groupCfg || {};
  if (minMap[m] != null) return Number(minMap[m]);
  if (m <= neutralAt) {
    if (m === 1) return 0.5;
    if (m === 2) return 0.75;
    return 1.0; // 3 y 4 → 1.0
  }
  return 1 + step * (m - neutralAt);
}

export function ageCoef(edadMax, tiers) {
  const e = Number(edadMax) || 0;
  for (const t of tiers || []) {
    if (e >= (t?.min ?? 0)) return Number(t?.coef || 1);
  }
  return 1.0;
}

export function computeCuotaIdealWith(
  { base, integrantes, edadMax, cremCount },
  rules
) {
  const B = Number(base ?? rules.base);
  const g = membersFactor(integrantes, rules.group);
  const a = ageCoef(edadMax, rules.age);
  const crem =
    B * Number(rules.cremationCoef) * Math.max(0, Number(cremCount) || 0);
  return round500(B * g * a + crem);
}

export async function computeCuotaIdealAsync({
  base,
  integrantes,
  edadMax,
  cremCount,
}) {
  const rules = await getGlobalPriceRules();
  return computeCuotaIdealWith(
    { base, integrantes, edadMax, cremCount },
    rules
  );
}

/* ============================================================================
   HELPERS DB
   ========================================================================== */

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

/** Recalcula y PERSISTE `edad` para el grupo (usa fechaNac || fechNacimiento). */
async function updateAgesForGroup(filter, { debug = false } = {}) {
  const now = new Date();
  const res = await Cliente.updateMany(
    filter,
    [
      { $set: { _dobRaw: { $ifNull: ["$fechaNac", "$fechNacimiento"] } } },
      {
        $set: {
          _dob: {
            $switch: {
              branches: [
                {
                  case: { $eq: [{ $type: "$_dobRaw" }, "date"] },
                  then: "$_dobRaw",
                },
                {
                  case: {
                    $and: [
                      { $eq: [{ $type: "$_dobRaw" }, "string"] },
                      {
                        $regexMatch: {
                          input: "$_dobRaw",
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
                                        { $split: ["$_dobRaw", "/"] },
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
                                dateString: "$_dobRaw",
                                format: "%d/%m/%Y",
                              },
                            },
                            {
                              $dateFromString: {
                                dateString: "$_dobRaw",
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
                  input: "$_dobRaw",
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
                    { $ne: ["$_dob", null] },
                    {
                      $dateDiff: {
                        startDate: "$_dob",
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
      { $unset: ["_dobRaw", "_dob"] },
    ],
    { strict: false } // por si el schema no tiene alguno de los campos legacy
  );

  if (debug) {
    console.log(
      `[AGE-UPDATE group] matched=${res?.matchedCount ?? 0}, modified=${
        res?.modifiedCount ?? 0
      }`
    );
  }
  return res;
}

/* ============================================================================
   1) Núcleo: recalcular y PERSISTIR todo el grupo
   - Recalcula EDADES por miembro (persistente)
   - Ignora inactivos para el cómputo (baja válida o activo=false)
   - Calcula integrantes, edadMax, cremaciones, cuotaIdeal
   - Persiste cuotaIdeal + edadMaxPoliza en TODO el grupo (y titular)
   ========================================================================== */
export async function recomputeGroupPricing(
  idClienteKey,
  { debug = false } = {}
) {
  const filter = buildGroupFilter(idClienteKey);

  // 0) Aseguramos EDADES actualizadas y persistidas
  await updateAgesForGroup(filter, { debug });

  // 1) Leemos miembros del grupo (ahora con edad recalculada)
  const members = await Cliente.aggregate([
    { $match: filter },
    {
      $project: {
        _id: 1,
        idCliente: 1,
        activo: 1,
        baja: 1,
        edad: 1,
        fechaNac: 1,
        cremacion: 1,
        rol: 1,
      },
    },
  ]).allowDiskUse(true);

  const total = members.length;
  const activosArr = members.filter(isActive);
  const integrantes = activosArr.length;

  // edadMax: priorizamos `edad` ya recalculada; fallback a fechaNac por seguridad
  const edades = activosArr
    .map((m) => {
      if (typeof m?.edad === "number") return m.edad;
      if (m?.fechaNac) {
        const dt = new Date(m.fechaNac);
        if (!Number.isNaN(dt.getTime())) {
          const today = new Date();
          let a = today.getFullYear() - dt.getFullYear();
          const mm = today.getMonth() - dt.getMonth();
          if (mm < 0 || (mm === 0 && today.getDate() < dt.getDate())) a--;
          return a >= 0 && a <= 120 ? a : undefined;
        }
      }
      return undefined;
    })
    .filter((a) => typeof a === "number");

  const edadMax = edades.length ? Math.max(...edades) : 0;
  const cremaciones = activosArr.reduce(
    (acc, m) => acc + (m?.cremacion ? 1 : 0),
    0
  );

  // 2) Reglas → cuotaIdeal (para el grupo)
  const rules = await getGlobalPriceRules();
  const BASE = Number(rules.base ?? 16000);
  const cuotaIdeal =
    integrantes > 0
      ? computeCuotaIdealWith(
          { base: BASE, integrantes, edadMax, cremCount: cremaciones },
          rules
        )
      : 0;

  // 3) Persistimos en TODO el grupo (denormalización útil de lectura)
  const res = await Cliente.updateMany(
    filter,
    { $set: { cuotaIdeal, edadMaxPoliza: edadMax, updatedAt: new Date() } },
    { strict: false }
  );
  const matched = res?.matchedCount ?? 0;
  const modified = res?.modifiedCount ?? 0;

  // 4) Redundancia explícita en titular (por si querés consultar solo titulares)
  const titularFilter = { $and: [filter, { rol: "TITULAR" }] };
  const updTitular = await Cliente.updateMany(
    titularFilter,
    { $set: { edadMaxPoliza: edadMax, updatedAt: new Date() } },
    { strict: false }
  );
  const titularMatched = updTitular?.matchedCount ?? 0;
  const titularModified = updTitular?.modifiedCount ?? 0;

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

/* ============================================================================
   2) Orquestador: por lote de grupos
   ========================================================================== */
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

/* ============================================================================
   3) Recalcular TODOS los grupos
   ========================================================================== */
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

/* ============================================================================
   4) Compat: Age-fix legacy + reprice (solo fechaNac)
   ========================================================================== */
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

  return recomputeGroupsByIds(ids, { concurrency, barWidth, logEvery, debug });
}

/* ============================================================================
   (Opcionales útiles, por si querés reportes)
   ========================================================================== */
export async function auditAges({ sample = 5 } = {}) {
  const now = new Date();

  const rows = await Cliente.aggregate([
    {
      $project: {
        idCliente: 1,
        edad: 1,
        fechaNac: 1,
        fechNacimiento: 1,
        _dobRaw: { $ifNull: ["$fechaNac", "$fechNacimiento"] },
      },
    },
    {
      $addFields: {
        _dob: {
          $switch: {
            branches: [
              {
                case: { $eq: [{ $type: "$_dobRaw" }, "date"] },
                then: "$_dobRaw",
              },
              {
                case: {
                  $and: [
                    { $eq: [{ $type: "$_dobRaw" }, "string"] },
                    {
                      $regexMatch: {
                        input: "$_dobRaw",
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
                                      { $split: ["$_dobRaw", "/"] },
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
                              dateString: "$_dobRaw",
                              format: "%d/%m/%Y",
                            },
                          },
                          {
                            $dateFromString: {
                              dateString: "$_dobRaw",
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
                input: "$_dobRaw",
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
      $addFields: {
        _years: {
          $cond: [
            { $ne: ["$_dob", null] },
            { $dateDiff: { startDate: "$_dob", endDate: now, unit: "year" } },
            null,
          ],
        },
      },
    },
    {
      $addFields: {
        _yearsValid: {
          $cond: [
            {
              $and: [
                { $ne: ["$_years", null] },
                { $gte: ["$_years", 0] },
                { $lte: ["$_years", 120] },
              ],
            },
            "$_years",
            null,
          ],
        },
      },
    },
    {
      $addFields: {
        _mismatch: {
          $cond: [
            {
              $and: [
                { $ne: ["$_yearsValid", null] },
                {
                  $ne: [
                    { $ifNull: ["$edad", null] },
                    { $ifNull: ["$_yearsValid", null] },
                  ],
                },
              ],
            },
            true,
            false,
          ],
        },
      },
    },
    { $match: { _mismatch: true } },
    {
      $facet: {
        stats: [
          { $group: { _id: "$_id", idCliente: { $first: "$idCliente" } } },
          {
            $group: {
              _id: null,
              count: { $sum: 1 },
              grupos: { $addToSet: "$idCliente" },
            },
          },
        ],
        sample: [
          {
            $project: {
              _id: 1,
              idCliente: 1,
              edadActual: "$edad",
              edadCalc: "$_yearsValid",
              dob: "$_dob",
            },
          },
          { $limit: sample },
        ],
      },
    },
  ]).allowDiskUse(true);

  const stat = rows?.[0]?.stats?.[0] || { count: 0, grupos: [] };
  const examples = rows?.[0]?.sample || [];
  return {
    ok: true,
    desalineados: stat.count || 0,
    gruposAfectados: (stat.grupos || []).filter((g) => g != null),
    ejemplos: examples,
  };
}

export default {
  round500,
  membersFactor,
  ageCoef,
  computeCuotaIdealWith,
  computeCuotaIdealAsync,
  recomputeGroupPricing,
  recomputeGroupsByIds,
  recomputeAllGroups,
  fixAgesAndMaybeReprice,
  auditAges,
};
