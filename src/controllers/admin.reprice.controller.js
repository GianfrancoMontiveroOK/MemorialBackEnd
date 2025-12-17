// src/controllers/admin.reprice.controller.js

import Cliente from "../models/client.model.js";
import {
  recomputeGroupPricing,
  round500,
} from "../services/pricing.services.js";

/* ============================================================================
 * PROGRESO GLOBAL (migrado desde el service)
 * ========================================================================== */

const _globalPricingProgress = {
  running: false,
  mode: null, // "all" | "byIds" | "percent" | etc
  total: 0,
  procesados: 0,
  matchedTotal: 0,
  modifiedTotal: 0,
  errores: 0,
  startedAt: null,
  updatedAt: null,
  // para percent:
  modifiedIdeal: 0,
  modifiedHistorical: 0,
  skippedTotal: 0,
};

function _resetProgress({ mode = null, total = 0 } = {}) {
  const now = new Date();
  _globalPricingProgress.running = total > 0;
  _globalPricingProgress.mode = mode;
  _globalPricingProgress.total = total;
  _globalPricingProgress.procesados = 0;
  _globalPricingProgress.matchedTotal = 0;
  _globalPricingProgress.modifiedTotal = 0;
  _globalPricingProgress.errores = 0;
  _globalPricingProgress.modifiedIdeal = 0;
  _globalPricingProgress.modifiedHistorical = 0;
  _globalPricingProgress.skippedTotal = 0;
  _globalPricingProgress.startedAt = total > 0 ? now : null;
  _globalPricingProgress.updatedAt = now;
}

function _updateProgress(partial = {}) {
  Object.assign(_globalPricingProgress, partial, {
    updatedAt: new Date(),
  });
}

export function getGlobalPricingProgress() {
  const snap = { ..._globalPricingProgress };
  const { total, procesados, running, startedAt, updatedAt } = snap;

  // porcentaje 0–100
  let percent = 0;
  if (Number.isFinite(total) && total > 0 && Number.isFinite(procesados)) {
    percent = (procesados / total) * 100;
  }

  // terminó (ya procesó todo y no está corriendo)
  const finished =
    !running &&
    Number.isFinite(total) &&
    total > 0 &&
    Number.isFinite(procesados) &&
    procesados >= total;

  // ETA opcional en segundos
  let etaSec = null;
  if (
    running &&
    startedAt &&
    updatedAt &&
    Number.isFinite(total) &&
    total > 0 &&
    Number.isFinite(procesados) &&
    procesados > 0
  ) {
    const elapsedMs =
      new Date(updatedAt).getTime() - new Date(startedAt).getTime();
    const rate = elapsedMs > 0 ? procesados / (elapsedMs / 1000) : 0; // grupos/s
    const remaining = Math.max(0, total - procesados);
    etaSec = rate > 0 ? remaining / rate : null;
  }

  return {
    ...snap,
    percent,
    finished,
    etaSec,
  };
}

/* ============================================================================
 * HELPERS que antes estaban en el service (para loops)
 * ========================================================================== */

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

/* ============================================================================
 * 1) Orquestador: por lote de grupos (migrado)
 * ========================================================================== */

export async function recomputeGroupsByIds(
  ids = [],
  {
    concurrency = 8,
    barWidth = 30,
    logEvery = 200,
    debug = (process.env.PRICING_MODE || "").toLowerCase() === "prueba",
    mode = "byIds",
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

  _resetProgress({ mode, total });

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

        _updateProgress({
          running: true,
          total,
          procesados,
          matchedTotal,
          modifiedTotal,
          errores,
        });

        if (!isTTY && procesados % logEvery === 0) render(false);
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.max(1, concurrency) }, () => worker())
  );

  if (intervalId) clearInterval(intervalId);
  render(true);

  _updateProgress({
    running: false,
    total,
    procesados,
    matchedTotal,
    modifiedTotal,
    errores,
  });

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
 * 2) Recalcular TODOS los grupos (migrado)
 * ========================================================================== */

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

  return recomputeGroupsByIds(uniq, {
    concurrency,
    barWidth,
    logEvery,
    debug,
    mode: "all",
  });
}

/* ============================================================================
 * 3) Aumentar precios por porcentaje (migrado)
 * ========================================================================== */

async function increasePricesPercentEngine({
  percent,
  applyToIdeal,
  applyToHistorical,
  debug = (process.env.PRICING_MODE || "").toLowerCase() === "prueba",
} = {}) {
  const p = Number(percent);
  if (!Number.isFinite(p) || p === 0) {
    const err = new Error("percent debe ser un número distinto de cero");
    err.status = 400;
    throw err;
  }

  if (!applyToIdeal && !applyToHistorical) {
    const err = new Error(
      "Debe seleccionar al menos un tipo de precio a actualizar"
    );
    err.status = 400;
    throw err;
  }

  const factor = 1 + p / 100;

  // ids únicos de grupo (idCliente)
  const ids = await Cliente.distinct("idCliente", { idCliente: { $ne: null } });
  const uniq = [...new Set(ids.map((x) => String(x).trim()).filter(Boolean))];
  const total = uniq.length;

  let processed = 0;
  let errores = 0;
  let modifiedIdeal = 0;
  let modifiedHistorical = 0;
  let skipped = 0;

  _resetProgress({ mode: "percent", total });

  for (const id of uniq) {
    try {
      const filter = buildGroupFilter(id);

      const sample = await Cliente.findOne(filter, {
        cuotaIdeal: 1,
        cuota: 1,
      })
        .lean()
        .exec();

      let groupModifiedIdeal = 0;
      let groupModifiedHist = 0;

      // 1) AUMENTO SOBRE cuotaIdeal
      if (applyToIdeal) {
        const oldIdeal = Number(sample?.cuotaIdeal ?? 0);

        if (!oldIdeal || !Number.isFinite(oldIdeal)) {
          if (debug) {
            console.log(
              `[pricing#increasePercent] grupo=${id} SIN cuotaIdeal válida (valor crudo=${sample?.cuotaIdeal}) → NO se actualiza cuotaIdeal`
            );
          }
        } else {
          const updatedIdeal = round500(oldIdeal * factor);

          if (updatedIdeal === oldIdeal) {
            if (debug) {
              console.log(
                `[pricing#increasePercent] grupo=${id} cuotaIdeal sin cambios (${oldIdeal})`
              );
            }
          } else {
            const updResIdeal = await Cliente.updateMany(
              filter,
              {
                $set: {
                  cuotaIdeal: updatedIdeal,
                  updatedAt: new Date(),
                },
              },
              { strict: false }
            );

            groupModifiedIdeal = updResIdeal?.modifiedCount ?? 0;
            modifiedIdeal += groupModifiedIdeal;

            if (debug) {
              console.log(
                `[pricing#increasePercent] grupo=${id} cuotaIdeal ${oldIdeal} → ${updatedIdeal} (docs modificados=${groupModifiedIdeal})`
              );
            }
          }
        }
      }

      // 2) AUMENTO SOBRE cuota (histórica)
      if (applyToHistorical) {
        const oldCuota = Number(sample?.cuota ?? 0);

        if (!oldCuota || !Number.isFinite(oldCuota)) {
          if (debug) {
            console.log(
              `[pricing#increasePercent] grupo=${id} SIN cuota vigente válida (valor crudo=${sample?.cuota}) → NO se actualiza cuota`
            );
          }
        } else {
          const updatedCuota = round500(oldCuota * factor);

          if (updatedCuota === oldCuota) {
            if (debug) {
              console.log(
                `[pricing#increasePercent] grupo=${id} cuota sin cambios (${oldCuota})`
              );
            }
          } else {
            const updResHist = await Cliente.updateMany(
              filter,
              {
                $set: {
                  cuota: updatedCuota,
                  updatedAt: new Date(),
                },
              },
              { strict: false }
            );

            groupModifiedHist = updResHist?.modifiedCount ?? 0;
            modifiedHistorical += groupModifiedHist;

            if (debug) {
              console.log(
                `[pricing#increasePercent] grupo=${id} cuota ${oldCuota} → ${updatedCuota} (docs modificados=${groupModifiedHist})`
              );
            }
          }
        }
      }

      if (!groupModifiedIdeal && !groupModifiedHist) {
        skipped++;
      }
    } catch (e) {
      errores++;
      if (debug) {
        console.error(
          "[pricing#increasePercent] ERROR grupo",
          id,
          e?.message || e
        );
      }
    } finally {
      processed++;

      _updateProgress({
        mode: "percent",
        running: true,
        total,
        procesados: processed,
        matchedTotal: processed,
        modifiedIdeal,
        modifiedHistorical,
        modifiedTotal: modifiedIdeal + modifiedHistorical,
        skippedTotal: skipped,
        errores,
      });
    }
  }

  _updateProgress({
    mode: "percent",
    running: false,
    total,
    procesados: processed,
    matchedTotal: processed,
    modifiedIdeal,
    modifiedHistorical,
    modifiedTotal: modifiedIdeal + modifiedHistorical,
    skippedTotal: skipped,
    errores,
  });

  if (debug) {
    console.log(
      `[pricing#increasePercent] FIN percent=${p}% (factor=${factor}) · total=${total} · procesados=${processed} · modifiedIdeal=${modifiedIdeal} · modifiedHistorical=${modifiedHistorical} · skipped=${skipped} · errores=${errores}`
    );
  }

  return {
    ok: true,
    percent: p,
    factor,
    totalGrupos: total,
    procesados: processed,
    modifiedIdeal,
    modifiedHistorical,
    modifiedTotal: modifiedIdeal + modifiedHistorical,
    skippedTotal: skipped,
    errores,
    historicalImplemented: !!applyToHistorical,
  };
}

/* ============================================================================
 * CONTROLLERS HTTP
 * ========================================================================== */

/**
 * POST /api/admin/reprice/:idCliente
 */
export async function repriceGroupController(req, res) {
  try {
    const { idCliente } = req.params;
    if (!idCliente?.toString().trim()) {
      return res
        .status(400)
        .json({ ok: false, message: "idCliente requerido" });
    }
    const r = await recomputeGroupPricing(idCliente, { debug: false });
    return res.json({ ok: true, ...r });
  } catch (err) {
    const status = err?.status || 500;
    return res
      .status(status)
      .json({ ok: false, message: err?.message || "Error reprice grupo" });
  }
}

/**
 * POST /api/admin/reprice-all
 */
export async function repriceAllGroupsController(req, res) {
  try {
    const { concurrency, logEvery } = req.body || {};

    const r = await recomputeAllGroups({
      concurrency: Number.isFinite(Number(concurrency))
        ? Number(concurrency)
        : undefined,
      logEvery: Number.isFinite(Number(logEvery))
        ? Number(logEvery)
        : undefined,
      debug: false,
    });

    return res.json({ ok: true, ...r });
  } catch (err) {
    const status = err?.status || 500;
    return res
      .status(status)
      .json({ ok: false, message: err?.message || "Error reprice-all" });
  }
}

/**
 * GET /api/admin/reprice-progress
 */
export function repriceProgressController(req, res) {
  const progress = getGlobalPricingProgress();
  return res.json({ ok: true, ...progress });
}

/**
 * POST /api/admin/reprice-by-ids
 */
export async function repriceByIdsController(req, res) {
  try {
    const { ids, concurrency, logEvery } = req.body || {};
    if (!Array.isArray(ids) || ids.length === 0) {
      return res.status(400).json({
        ok: false,
        message: "Se requiere 'ids' (array no vacío)",
      });
    }

    const r = await recomputeGroupsByIds(ids, {
      concurrency: Number.isFinite(Number(concurrency))
        ? Number(concurrency)
        : undefined,
      logEvery: Number.isFinite(Number(logEvery))
        ? Number(logEvery)
        : undefined,
      debug: false,
      mode: "byIds",
    });

    return res.json({ ok: true, ...r });
  } catch (err) {
    const status = err?.status || 500;
    return res.status(status).json({
      ok: false,
      message: err?.message || "Error reprice-by-ids",
    });
  }
}

/**
 * POST /api/admin/increase-percent
 */
export async function increasePercentController(req, res) {
  try {
    const { percent, applyToIdeal, applyToHistorical } = req.body || {};
    const p = Number(percent);

    if (!Number.isFinite(p) || p === 0) {
      return res.status(400).json({
        ok: false,
        message: "percent debe ser un número distinto de cero",
      });
    }

    if (!applyToIdeal && !applyToHistorical) {
      return res.status(400).json({
        ok: false,
        message: "Debe seleccionar al menos un tipo de precio a actualizar",
      });
    }

    const r = await increasePricesPercentEngine({
      percent: p,
      applyToIdeal: !!applyToIdeal,
      applyToHistorical: !!applyToHistorical,
      debug: true,
    });

    return res.json({ ok: true, ...r });
  } catch (err) {
    const status = err?.status || 500;
    return res.status(status).json({
      ok: false,
      message: err?.message || "Error increase-percent",
    });
  }
}

/* ============================================================================
 * 4) Compat: Age-fix legacy + reprice (solo fechaNac) — migrado
 * ========================================================================== */

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
                                onError: null,
                                onNull: null,
                              },
                            },
                            {
                              $dateFromString: {
                                dateString: "$fechaNac",
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
 * Default export (opcionales)
 * ========================================================================== */

export default {
  repriceGroupController,
  repriceAllGroupsController,
  repriceProgressController,
  repriceByIdsController,
  increasePercentController,
};
