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
   HELPERS DB internos
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
                // 1) Ya es Date -> la usamos como está
                {
                  case: { $eq: [{ $type: "$_dobRaw" }, "date"] },
                  then: "$_dobRaw",
                },
                // 2) String tipo "dd/mm/yyyy" o "d/m/yy"
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
                            // dd/mm/YYYY
                            {
                              $dateFromString: {
                                dateString: "$_dobRaw",
                                format: "%d/%m/%Y",
                                onError: null,
                                onNull: null,
                              },
                            },
                            // dd/mm/YY
                            {
                              $dateFromString: {
                                dateString: "$_dobRaw",
                                format: "%d/%m/%y",
                                onError: null,
                                onNull: null,
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
              // 3) Fallback genérico: que intente convertir y si no puede → null
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
    { strict: false }
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
   Núcleo: recalcular y PERSISTIR todo el grupo
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

  // 3) Persistimos en TODO el grupo
  const res = await Cliente.updateMany(
    filter,
    { $set: { cuotaIdeal, edadMaxPoliza: edadMax, updatedAt: new Date() } },
    { strict: false }
  );
  const matched = res?.matchedCount ?? 0;
  const modified = res?.modifiedCount ?? 0;

  // 4) Redundancia explícita en titular
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

export default {
  round500,
  membersFactor,
  ageCoef,
  computeCuotaIdealWith,
  computeCuotaIdealAsync,
  recomputeGroupPricing,
};
