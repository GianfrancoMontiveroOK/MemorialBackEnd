// src/controllers/admin.stats.controller.js
import mongoose from "mongoose";
import Cliente from "../models/client.model.js";
import Payment from "../models/payment.model.js";
import LedgerEntry from "../models/ledger-entry.model.js";
import User from "../models/user.model.js";

/* ============== Helpers base ============== */
function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}
function pct(part, total) {
  const p = safeNum(part),
    t = safeNum(total);
  return t > 0 ? Number(((p / t) * 100).toFixed(2)) : 0;
}
function parsePeriodOrDefault(periodStr) {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const p =
    periodStr && /^\d{4}-\d{2}$/.test(periodStr) ? periodStr : `${yyyy}-${mm}`;
  const [Y, M] = p.split("-").map(Number);
  const start = new Date(Date.UTC(Y, M - 1, 1, 0, 0, 0));
  const end = new Date(Date.UTC(Y, M, 1, 0, 0, 0)); // exclusivo
  return { p, start, end };
}

/* ============== Paso 1: snapshot “Debido” por grupo ============== */
async function buildClientDueSnapshot({ idCobradorFilter, dueMember }) {
  const pipeline = [
    { $match: { activo: true } },
    {
      $group: {
        _id: "$idCliente",
        miembros: { $push: "$$ROOT" },
        titular: {
          $max: { $cond: [{ $eq: ["$integrante", 0] }, "$$ROOT", null] },
        },
      },
    },
    {
      $project: {
        idCliente: "$_id",
        integrantes: { $size: "$miembros" },

        // Sumas por grupo
        sumIdealGroup: {
          $sum: {
            $map: {
              input: "$miembros",
              as: "m",
              in: { $ifNull: ["$$m.cuotaIdeal", 0] },
            },
          },
        },
        sumCuotaGroup: {
          $sum: {
            $map: {
              input: "$miembros",
              as: "m",
              in: { $ifNull: ["$$m.cuota", 0] },
            },
          },
        },
        sumVigenteGroup: {
          $sum: {
            $map: {
              input: "$miembros",
              as: "m",
              in: {
                $cond: [
                  { $ifNull: ["$$m.usarCuotaIdeal", false] },
                  { $ifNull: ["$$m.cuotaIdeal", 0] },
                  { $ifNull: ["$$m.cuota", 0] },
                ],
              },
            },
          },
        },

        // Titular (con flag)
        titularIdeal: { $ifNull: ["$titular.cuotaIdeal", 0] },
        titularCuota: { $ifNull: ["$titular.cuota", 0] },
        titularUsaIdeal: { $ifNull: ["$titular.usarCuotaIdeal", false] },
        titularVigente: {
          $cond: [
            { $ifNull: ["$titular.usarCuotaIdeal", false] },
            { $ifNull: ["$titular.cuotaIdeal", 0] },
            { $ifNull: ["$titular.cuota", 0] },
          ],
        },

        // idCobrador
        idCobrador: {
          $ifNull: [
            "$titular.idCobrador",
            {
              $let: {
                vars: {
                  firstWith: {
                    $first: {
                      $filter: {
                        input: "$miembros",
                        as: "x",
                        cond: { $gt: ["$$x.idCobrador", null] },
                      },
                    },
                  },
                },
                in: "$$firstWith.idCobrador",
              },
            },
          ],
        },

        // Flags plan
        hasCrem: {
          $anyElementTrue: {
            $map: {
              input: "$miembros",
              as: "m",
              in: { $ifNull: ["$$m.cremacion", false] },
            },
          },
        },
        hasParc: {
          $anyElementTrue: {
            $map: {
              input: "$miembros",
              as: "m",
              in: { $ifNull: ["$$m.parcela", false] },
            },
          },
        },
      },
    },
    {
      $project: {
        idCliente: 1,
        integrantes: 1,
        idCobrador: 1,
        hasCrem: 1,
        hasParc: 1,
        titularUsaIdeal: 1,
        // Debe por grupo/titular
        dueGroup: {
          vigente: "$sumVigenteGroup",
          ideal: "$sumIdealGroup",
          cuota: "$sumCuotaGroup",
        },
        dueTitular: {
          vigente: "$titularVigente",
          ideal: "$titularIdeal",
          cuota: "$titularCuota",
        },
        // Para adopción por grupo, comparamos sumas
        groupUsaIdeal: { $eq: ["$sumVigenteGroup", "$sumIdealGroup"] },
      },
    },
  ];
  if (idCobradorFilter != null)
    pipeline.push({ $match: { idCobrador: idCobradorFilter } });

  const clientesAgg = await Cliente.aggregate(pipeline);

  const dueBreakdownTotals = { vigente: 0, ideal: 0, cuota: 0 };
  const cobradorByGroup = new Map();
  const planInfoByGroup = new Map();

  const usingIdealCountTitular = clientesAgg.filter(
    (g) => !!g.titularUsaIdeal
  ).length;
  const usingCuotaCountTitular = clientesAgg.length - usingIdealCountTitular;
  const usingIdealCountGroup = clientesAgg.filter(
    (g) => !!g.groupUsaIdeal
  ).length;
  const usingCuotaCountGroup = clientesAgg.length - usingIdealCountGroup;

  // Pre-acumulamos totales por dueMember actual
  for (const r of clientesAgg) {
    const base = dueMember === "group" ? r.dueGroup : r.dueTitular;
    dueBreakdownTotals.vigente += safeNum(base.vigente);
    dueBreakdownTotals.ideal += safeNum(base.ideal);
    dueBreakdownTotals.cuota += safeNum(base.cuota);

    if (r.idCobrador != null) cobradorByGroup.set(r.idCliente, r.idCobrador);
    planInfoByGroup.set(r.idCliente, {
      hasCrem: !!r.hasCrem,
      hasParc: !!r.hasParc,
    });
  }

  return {
    clientesAgg,
    dueBreakdownTotals,
    cobradorByGroup,
    planInfoByGroup,
    totals: {
      totalGrupos: clientesAgg.length,
      totalMiembros: clientesAgg.reduce(
        (a, r) => a + safeNum(r.integrantes),
        0
      ),
    },
    adoption: {
      titular: {
        usingIdeal: usingIdealCountTitular,
        usingCuota: usingCuotaCountTitular,
        rate: pct(usingIdealCountTitular, clientesAgg.length),
      },
      group: {
        usingIdeal: usingIdealCountGroup,
        usingCuota: usingCuotaCountGroup,
        rate: pct(usingIdealCountGroup, clientesAgg.length),
      },
    },
  };
}

/* ============== Paso 2: pagos del período ============== */
async function aggregatePaymentsForPeriod({
  period,
  periodStart,
  periodEnd,
  methodFilter,
  channelFilter,
  idCobradorFilter,
}) {
  const payMatch = {
    status: { $in: ["posted", "settled"] },
    ...(methodFilter ? { method: methodFilter } : {}),
    ...(channelFilter ? { channel: channelFilter } : {}),
    ...(idCobradorFilter != null
      ? { "collector.idCobrador": idCobradorFilter }
      : {}),
    $or: [
      { "allocations.period": period },
      { intendedPeriod: period },
      { postedAt: { $gte: periodStart, $lt: periodEnd } }, // fallback
    ],
  };

  const paymentsAgg = await Payment.aggregate([
    { $match: payMatch },
    {
      $addFields: {
        allocsForPeriod: {
          $filter: {
            input: { $ifNull: ["$allocations", []] },
            as: "al",
            cond: { $eq: ["$$al.period", period] },
          },
        },
      },
    },
    {
      $addFields: {
        hasAllocPeriod: { $gt: [{ $size: "$allocsForPeriod" }, 0] },
        hasIntended: { $eq: ["$intendedPeriod", period] },
        inDateRange: {
          $and: [
            { $gte: ["$postedAt", periodStart] },
            { $lt: ["$postedAt", periodEnd] },
          ],
        },
        appliedToPeriod: {
          $cond: [
            { $gt: [{ $size: "$allocsForPeriod" }, 0] },
            { $sum: "$allocsForPeriod.amountApplied" },
            {
              $cond: [
                { $eq: ["$intendedPeriod", period] },
                "$amount",
                {
                  $cond: [
                    {
                      $and: [
                        { $gte: ["$postedAt", periodStart] },
                        { $lt: ["$postedAt", periodEnd] },
                      ],
                    },
                    "$amount",
                    0,
                  ],
                },
              ],
            },
          ],
        },
      },
    },
    {
      $project: {
        _id: 1,
        amount: 1,
        appliedToPeriod: 1,
        method: 1,
        channel: 1,
        postedAt: 1,
        hasAllocPeriod: 1,
        hasIntended: 1,
        inDateRange: 1,
        "cliente.idCliente": 1,
        "collector.idCobrador": 1,
        "collector.userId": 1,
      },
    },
  ]);

  const paidByGroup = new Map();
  const paidByCollector = new Map();
  const paidByUser = new Map();
  const methodsMix = new Map();
  const channelsMix = new Map();
  const ticketValues = [];

  for (const p of paymentsAgg) {
    const applied = safeNum(p.appliedToPeriod);
    if (applied <= 0) continue;

    const g = p?.cliente?.idCliente;
    if (g != null) paidByGroup.set(g, (paidByGroup.get(g) || 0) + applied);

    const cid = p?.collector?.idCobrador;
    if (cid != null)
      paidByCollector.set(cid, (paidByCollector.get(cid) || 0) + applied);

    const uid = p?.collector?.userId;
    if (uid) {
      const key = String(uid);
      paidByUser.set(key, (paidByUser.get(key) || 0) + applied);
    }

    if (p.method)
      methodsMix.set(p.method, (methodsMix.get(p.method) || 0) + applied);
    if (p.channel)
      channelsMix.set(p.channel, (channelsMix.get(p.channel) || 0) + applied);

    ticketValues.push(safeNum(p.amount));
  }

  const totalPagadoPeriodo = paymentsAgg.reduce(
    (a, p) => a + safeNum(p.appliedToPeriod),
    0
  );
  const paymentsUsingFallbackDate = paymentsAgg.filter(
    (p) =>
      !p.hasAllocPeriod &&
      !p.hasIntended &&
      p.inDateRange &&
      safeNum(p.appliedToPeriod) > 0
  ).length;

  return {
    paymentsAgg,
    totalPagadoPeriodo,
    paidByGroup,
    paidByCollector,
    paidByUser,
    methodsMix,
    channelsMix,
    ticketValues,
    paymentsUsingFallbackDate,
  };
}

/* ============== Paso 3: coverage por grupo ============== */
// Reemplazar función computeCoverage existente por esta
function computeCoverage({ dueByGroup, paidByGroup }) {
  let gruposPaid = 0;
  let gruposPartial = 0;
  let gruposUnpaid = 0;

  const coverageRows = [];

  for (const [g, dueRaw] of dueByGroup.entries()) {
    const due = safeNum(dueRaw);
    const paid = safeNum(paidByGroup.get(g));
    const gap = Number((paid - due).toFixed(2));

    coverageRows.push({ idCliente: g, due, paid, gap });

    // ⛔️ No clasificar grupos cuyo due <= 0 (no cuentan como pagados)
    if (due <= 0) continue;

    // ✅ Clasificación estricta por grupo
    if (paid >= due) gruposPaid++;
    else if (paid > 0) gruposPartial++;
    else gruposUnpaid++;
  }

  return { coverageRows, gruposPaid, gruposPartial, gruposUnpaid };
}

/* ============== Paso 4: por cobrador (base seleccionada) ============== */
async function buildByCollector({
  clientesAgg,
  dueMode,
  dueMember,
  paidByCollector,
}) {
  const collectorsSet = new Set([
    ...paidByCollector.keys(),
    ...clientesAgg.map((r) => r.idCobrador).filter((x) => x != null),
  ]);

  const usersWithCollector = await User.find(
    { idCobrador: { $exists: true, $ne: null } },
    { name: 1, idCobrador: 1 }
  ).lean();

  const nameByCollector = new Map();
  for (const u of usersWithCollector) {
    const n = Number(u.idCobrador);
    if (Number.isFinite(n)) nameByCollector.set(n, u.name || `Cobrador ${n}`);
  }

  const byCobrador = [];
  for (const cid of collectorsSet) {
    const due = clientesAgg
      .filter((r) => r.idCobrador === cid)
      .reduce((a, r) => {
        const base = dueMember === "group" ? r.dueGroup : r.dueTitular;
        return (
          a +
          (dueMode === "ideal"
            ? base.ideal
            : dueMode === "cuota"
            ? base.cuota
            : base.vigente)
        );
      }, 0);

    const paid = safeNum(paidByCollector.get(cid));
    const cov = due > 0 ? paid / due : 0;
    const faltante = due - paid;

    byCobrador.push({
      idCobrador: cid,
      name: nameByCollector.get(cid) || `Cobrador ${cid}`,
      due: Number(due.toFixed(2)),
      paid: Number(paid.toFixed(2)),
      coverageRate: Number(cov.toFixed(4)),
      gapSum: Number(faltante.toFixed(2)),
      diffSum: Number(faltante.toFixed(2)),
    });
  }
  byCobrador.sort((a, b) => b.diffSum - a.diffSum);
  return byCobrador;
}

/* ============== NUEVO: por cobrador (ideal vs cuota, adopción) ============== */
async function buildByCollectorIdealVsCuota({ clientesAgg, dueMember }) {
  const collectors = new Map(); // cid -> { ideal, cuota, count, adoptIdealCount }
  for (const r of clientesAgg) {
    const cid = r.idCobrador;
    if (cid == null) continue;
    const base = dueMember === "group" ? r.dueGroup : r.dueTitular;
    const adoptIdeal =
      dueMember === "group" ? r.groupUsaIdeal : r.titularUsaIdeal;
    const row = collectors.get(cid) || {
      ideal: 0,
      cuota: 0,
      count: 0,
      adoptIdealCount: 0,
    };
    row.ideal += safeNum(base.ideal);
    row.cuota += safeNum(base.cuota);
    row.count += 1;
    row.adoptIdealCount += adoptIdeal ? 1 : 0;
    collectors.set(cid, row);
  }

  const usersWithCollector = await User.find(
    { idCobrador: { $exists: true, $ne: null } },
    { name: 1, idCobrador: 1 }
  ).lean();
  const nameByCollector = new Map(
    usersWithCollector.map((u) => [
      Number(u.idCobrador),
      u.name || `Cobrador ${u.idCobrador}`,
    ])
  );

  const rows = [];
  for (const [cid, v] of collectors.entries()) {
    const delta = v.ideal - v.cuota;
    rows.push({
      idCobrador: cid,
      name: nameByCollector.get(cid) || `Cobrador ${cid}`,
      idealSum: Number(v.ideal.toFixed(2)),
      cuotaSum: Number(v.cuota.toFixed(2)),
      deltaAmount: Number(delta.toFixed(2)),
      deltaPct: v.cuota > 0 ? Number(((delta / v.cuota) * 100).toFixed(2)) : 0,
      adoptionRate: v.count
        ? Number(((v.adoptIdealCount / v.count) * 100).toFixed(2))
        : 0,
      groups: v.count,
    });
  }
  rows.sort((a, b) => b.deltaAmount - a.deltaAmount);
  return rows;
}

/* ============== Paso 5: histograma de gaps (paid - due) ============== */
function buildDiffHistogram(coverageRows) {
  const bins = [
    { key: "≤-5000", from: -Infinity, to: -5000 },
    { key: "-5000 a -1000", from: -5000, to: -1000 },
    { key: "-1000 a -500", from: -1000, to: -500 },
    { key: "-500 a -100", from: -500, to: -100 },
    { key: "-100 a -1", from: -100, to: -0.01 },
    { key: "≈0", from: -0.009, to: 0.009 },
    { key: "1 a 100", from: 0.01, to: 100 },
    { key: "100 a 500", from: 100, to: 500 },
    { key: "500 a 1000", from: 500, to: 1000 },
    { key: "1000 a 5000", from: 1000, to: 5000 },
    { key: "≥5000", from: 5000, to: Infinity },
  ];
  const diffHistogram = bins.map((b) => ({ _id: b.key, count: 0 }));
  for (const r of coverageRows) {
    const gap = safeNum(r.paid - r.due);
    const idx = bins.findIndex((b) => gap >= b.from && gap <= b.to);
    if (idx >= 0) diffHistogram[idx].count++;
  }
  return diffHistogram;
}

/* ============== NUEVO: histograma de uplift (ideal - cuota) ============== */
function buildUpliftHistogram(clientesAgg, dueMember) {
  const bins = [
    { key: "≤0", from: -Infinity, to: 0 },
    { key: "1 a 100", from: 1, to: 100 },
    { key: "100 a 500", from: 100, to: 500 },
    { key: "500 a 1000", from: 500, to: 1000 },
    { key: "1000 a 5000", from: 1000, to: 5000 },
    { key: "≥5000", from: 5000, to: Infinity },
  ];
  const hist = bins.map((b) => ({ _id: b.key, count: 0 }));
  for (const r of clientesAgg) {
    const base = dueMember === "group" ? r.dueGroup : r.dueTitular;
    const uplift = safeNum(base.ideal) - safeNum(base.cuota);
    const idx = bins.findIndex((b) => uplift >= b.from && uplift <= b.to);
    if (idx >= 0) hist[idx].count++;
  }
  return hist;
}

/* ============== Paso 6: por plan (avg) ============== */
function buildByPlan({ planInfoByGroup, dueBySelectedForPlanAvg }) {
  const byPlanMap = new Map();
  for (const [groupId, info] of planInfoByGroup.entries()) {
    const plan =
      info.hasCrem && info.hasParc
        ? "CREM+PARC"
        : info.hasCrem
        ? "CREM"
        : info.hasParc
        ? "PARC"
        : "BASE";
    const dueSel = safeNum(dueBySelectedForPlanAvg.get(groupId) || 0);
    const prev = byPlanMap.get(plan) || { plan, count: 0, cuotaAcc: 0 };
    prev.count += 1;
    prev.cuotaAcc += dueSel;
    byPlanMap.set(plan, prev);
  }
  return Array.from(byPlanMap.values())
    .map((p) => ({
      plan: p.plan,
      count: p.count,
      cuotaAvg: p.count ? Number((p.cuotaAcc / p.count).toFixed(2)) : 0,
    }))
    .sort((a, b) => b.count - a.count);
}

/* ============== Paso 7: mix métodos/canales + tickets ============== */
function computeMixAndTickets({ methodsMix, channelsMix, ticketValues }) {
  const mix = {
    methods: Object.fromEntries(
      Array.from(methodsMix.entries()).map(([k, v]) => [
        k,
        Number(v.toFixed(2)),
      ])
    ),
    channels: Object.fromEntries(
      Array.from(channelsMix.entries()).map(([k, v]) => [
        k,
        Number(v.toFixed(2)),
      ])
    ),
  };
  ticketValues.sort((a, b) => a - b);
  const tCount = ticketValues.length;
  const tSum = ticketValues.reduce((a, v) => a + v, 0);
  const tAvg = tCount ? tSum / tCount : 0;
  const tMedian = tCount
    ? tCount % 2
      ? ticketValues[(tCount - 1) / 2]
      : (ticketValues[tCount / 2 - 1] + ticketValues[tCount / 2]) / 2
    : 0;
  return {
    mix,
    ticketsGlobal: {
      count: tCount,
      sum: Number(tSum.toFixed(2)),
      avg: Number(tAvg.toFixed(2)),
      median: Number(tMedian.toFixed(2)),
    },
  };
}

/* ============== Paso 8: ledger ============== */
async function getLedgerSnapshots({ periodStart, periodEnd }) {
  const ledgerAgg = await LedgerEntry.aggregate([
    { $match: { postedAt: { $gte: periodStart, $lt: periodEnd } } },
    {
      $group: {
        _id: { accountCode: "$accountCode", side: "$side" },
        amount: { $sum: "$amount" },
      },
    },
  ]);
  const ledgerSnapshot = {};
  for (const row of ledgerAgg) {
    const acc = row._id.accountCode,
      side = row._id.side;
    ledgerSnapshot[acc] = ledgerSnapshot[acc] || { debit: 0, credit: 0 };
    ledgerSnapshot[acc][side] = Number(row.amount.toFixed(2));
  }
  const ledgerByUserAgg = await LedgerEntry.aggregate([
    { $match: { postedAt: { $gte: periodStart, $lt: periodEnd } } },
    { $group: { _id: { userId: "$userId" }, amount: { $sum: "$amount" } } },
  ]);
  const totalLedgerUsuarios = ledgerByUserAgg.reduce(
    (a, r) => a + safeNum(r.amount),
    0
  );
  return { ledgerSnapshot, ledgerByUserAgg, totalLedgerUsuarios };
}

/* ============== Paso 9: nombres & shares ============== */
async function fetchUserNames({ paidByUser, ledgerByUserAgg }) {
  const userIdsSet = new Set([
    ...Array.from(paidByUser.keys()),
    ...ledgerByUserAgg.map((r) => String(r._id.userId)),
  ]);
  const userIds = Array.from(userIdsSet)
    .filter(Boolean)
    .map((s) => new mongoose.Types.ObjectId(s));
  const usersRows = userIds.length
    ? await User.find({ _id: { $in: userIds } }, { name: 1 }).lean()
    : [];
  return new Map(usersRows.map((u) => [String(u._id), u.name || "Usuario"]));
}
function computeShares({
  paidByUser,
  totalPagadoPeriodo,
  ledgerByUserAgg,
  totalLedgerUsuarios,
  usersById,
}) {
  const paidShareByUser = Array.from(paidByUser.entries())
    .map(([uid, amt]) => ({
      userId: uid,
      name: usersById.get(uid) || "Usuario",
      amount: Number(amt.toFixed(2)),
      pct: totalPagadoPeriodo
        ? Number(((amt / totalPagadoPeriodo) * 100).toFixed(2))
        : 0,
    }))
    .sort((a, b) => b.amount - a.amount);
  const ledgerShareByUser = ledgerByUserAgg
    .map((r) => {
      const uid = String(r._id.userId),
        amt = safeNum(r.amount);
      return {
        userId: uid,
        name: usersById.get(uid) || "Usuario",
        amount: Number(amt.toFixed(2)),
        pct: totalLedgerUsuarios
          ? Number(((amt / totalLedgerUsuarios) * 100).toFixed(2))
          : 0,
      };
    })
    .sort((a, b) => b.amount - a.amount);
  return { paidShareByUser, ledgerShareByUser };
}

/* ============== Paso 10: revenue summary (ideal vs cuota y cobrado) ============== */
function computeRevenueSummary({ dueBreakdownTotals, totalPagadoPeriodo }) {
  const vigenteSum = safeNum(dueBreakdownTotals.vigente);
  const idealSum = safeNum(dueBreakdownTotals.ideal);
  const cuotaSum = safeNum(dueBreakdownTotals.cuota);

  const upliftAmount = Number((idealSum - cuotaSum).toFixed(2));
  const upliftPct =
    cuotaSum > 0 ? Number(((upliftAmount / cuotaSum) * 100).toFixed(2)) : 0;

  const gapPaidVsIdeal = Number((idealSum - totalPagadoPeriodo).toFixed(2));
  const covPaidVsIdeal =
    idealSum > 0
      ? Number(((totalPagadoPeriodo / idealSum) * 100).toFixed(2))
      : 0;
  const gapPaidVsVigente = Number((vigenteSum - totalPagadoPeriodo).toFixed(2));
  const covPaidVsVigente =
    vigenteSum > 0
      ? Number(((totalPagadoPeriodo / vigenteSum) * 100).toFixed(2))
      : 0;

  return {
    revenue: {
      baselineCuota: Number(cuotaSum.toFixed(2)),
      targetIdeal: Number(idealSum.toFixed(2)),
      vigenteSum: Number(vigenteSum.toFixed(2)),
      upliftIdealVsCuota: { amount: upliftAmount, pct: upliftPct },
      collected: Number(totalPagadoPeriodo.toFixed(2)),
      vsIdeal: { gap: gapPaidVsIdeal, coveragePct: covPaidVsIdeal },
      vsVigente: { gap: gapPaidVsVigente, coveragePct: covPaidVsVigente },
    },
    dues: {
      vigente: Number(vigenteSum.toFixed(2)),
      ideal: Number(idealSum.toFixed(2)),
      cuota: Number(cuotaSum.toFixed(2)),
    },
    idealVsCuota: {
      idealSum: Number(idealSum.toFixed(2)),
      cuotaSum: Number(cuotaSum.toFixed(2)),
      deltaAmount: upliftAmount,
      deltaPct: upliftPct,
    },
  };
}

/* ============== NUEVO: top oportunidades (grupos con mayor uplift) ============== */
function buildTopUpliftGroups({ clientesAgg, dueMember, limit = 50 }) {
  const rows = [];
  for (const r of clientesAgg) {
    const base = dueMember === "group" ? r.dueGroup : r.dueTitular;
    const usesIdeal =
      dueMember === "group" ? !!r.groupUsaIdeal : !!r.titularUsaIdeal;
    if (usesIdeal) continue; // ya está en ideal; no es oportunidad

    const delta = safeNum(base.ideal) - safeNum(base.cuota);
    if (delta <= 0) continue;

    rows.push({
      idCliente: r.idCliente,
      idCobrador: r.idCobrador ?? null,
      integrantes: r.integrantes,
      cuota: Number(base.cuota.toFixed(2)),
      ideal: Number(base.ideal.toFixed(2)),
      vigente: Number(base.vigente.toFixed(2)),
      delta: Number(delta.toFixed(2)),
    });
  }
  rows.sort((a, b) => b.delta - a.delta);
  return rows.slice(0, limit);
}

/* ============== Controller principal ============== */
export async function getClientesStats(req, res, next) {
  try {
    const {
      p: period,
      start: periodStart,
      end: periodEnd,
    } = parsePeriodOrDefault(req.query.period);

    const idCobradorFilter =
      req.query.idCobrador != null && req.query.idCobrador !== ""
        ? Number(req.query.idCobrador)
        : null;
    const methodFilter = req.query.method || null;
    const channelFilter = req.query.channel || null;

    const dueMode = (req.query.dueMode || "vigente").toLowerCase(); // vigente | ideal | cuota
    const dueMember = (req.query.dueMember || "titular").toLowerCase(); // group | titular

    // 1) Debe snapshot
    const snap = await buildClientDueSnapshot({ idCobradorFilter, dueMember });
    const {
      clientesAgg,
      dueBreakdownTotals,
      cobradorByGroup,
      planInfoByGroup,
      totals,
      adoption,
    } = snap;

    // Selección para gráficas/kpis en base a dueMode
    const dueByGroupCoverage = new Map();

    // 🔹 Para totales/avg plan respetamos dueMember actual (hoy = "titular" = individual)
    const dueBySelectedForPlanAvg = new Map();

    for (const r of clientesAgg) {
      // Base para totales (según dueMember vigente)
      const baseTotals = dueMember === "group" ? r.dueGroup : r.dueTitular;
      const selTotals =
        dueMode === "ideal"
          ? baseTotals.ideal
          : dueMode === "cuota"
          ? baseTotals.cuota
          : baseTotals.vigente;
      dueBySelectedForPlanAvg.set(r.idCliente, safeNum(selTotals));

      // Base para cobertura: SIEMPRE por grupo
      const baseCoverage = r.dueGroup;
      const selCoverage =
        dueMode === "ideal"
          ? baseCoverage.ideal
          : dueMode === "cuota"
          ? baseCoverage.cuota
          : baseCoverage.vigente;
      dueByGroupCoverage.set(r.idCliente, safeNum(selCoverage));
    }

    // Totales “individuales” (respetan dueMember = titular por default)
    const totalDebido =
      dueMode === "ideal"
        ? dueBreakdownTotals.ideal
        : dueMode === "cuota"
        ? dueBreakdownTotals.cuota
        : dueBreakdownTotals.vigente;

    // 2) Pagos
    const pay = await aggregatePaymentsForPeriod({
      period,
      periodStart,
      periodEnd,
      methodFilter,
      channelFilter,
      idCobradorFilter,
    });
    const {
      totalPagadoPeriodo,
      paidByGroup,
      paidByCollector,
      paidByUser,
      methodsMix,
      channelsMix,
      ticketValues,
      paymentsUsingFallbackDate,
    } = pay;

    // 3) Coverage
    const { coverageRows, gruposPaid, gruposPartial, gruposUnpaid } =
      computeCoverage({ dueByGroup: dueByGroupCoverage, paidByGroup });

    // 4) Por cobrador (base seleccionada)
    const byCobrador = await buildByCollector({
      clientesAgg,
      dueMode,
      dueMember,
      paidByCollector,
    });

    // 4b) NUEVO: por cobrador ideal vs cuota + adopción
    const byCobradorIdealVsCuota = await buildByCollectorIdealVsCuota({
      clientesAgg,
      dueMember,
    });

    // 5) Histogramas
    const diffHistogram = buildDiffHistogram(coverageRows);
    const upliftHistogram = buildUpliftHistogram(clientesAgg, dueMember); // NUEVO

    // 6) Por plan
    const byPlan = buildByPlan({ planInfoByGroup, dueBySelectedForPlanAvg });

    // 7) Mix y tickets
    const { mix, ticketsGlobal } = computeMixAndTickets({
      methodsMix,
      channelsMix,
      ticketValues,
    });

    // 8) Ledger
    const { ledgerSnapshot, ledgerByUserAgg, totalLedgerUsuarios } =
      await getLedgerSnapshots({ periodStart, periodEnd });

    // 9) Nombres & shares
    const usersById = await fetchUserNames({ paidByUser, ledgerByUserAgg });
    const { paidShareByUser, ledgerShareByUser } = computeShares({
      paidByUser,
      totalPagadoPeriodo,
      ledgerByUserAgg,
      totalLedgerUsuarios,
      usersById,
    });

    // 10) Revenue + ideal vs cuota
    const { revenue, dues, idealVsCuota } = computeRevenueSummary({
      dueBreakdownTotals,
      totalPagadoPeriodo,
    });

    // NUEVO: impacto de política (pérdida potencial si aún usan cuota)
    const lostVsTargetAmount = safeNum(idealVsCuota.deltaAmount); // ideal - cuota
    const policyImpact = {
      lostVsTargetAmount: Number(lostVsTargetAmount.toFixed(2)),
      lostPctVsTarget: pct(lostVsTargetAmount, idealVsCuota.idealSum),
      lostPctVsCollected: pct(lostVsTargetAmount, totalPagadoPeriodo),
    };

    // NUEVO: Top oportunidades (grupos no ideal con delta grande)
    const topUpliftGroups = buildTopUpliftGroups({
      clientesAgg,
      dueMember,
      limit: 50,
    });

    // Summary final
    const summary = {
      totalGrupos: totals.totalGrupos,
      totalMiembros: totals.totalMiembros,
      avgIntegrantes: totals.totalGrupos
        ? Number((totals.totalMiembros / totals.totalGrupos).toFixed(2))
        : 0,

      // Totales paralelos (por dueMember actual)
      dues,

      // Base seleccionada y cobrado
      totalDebido: Number(totalDebido.toFixed(2)),
      totalPagadoPeriodo: Number(totalPagadoPeriodo.toFixed(2)),

      // Cobertura y desvíos
      coverageRate:
        totalDebido > 0
          ? Number((totalPagadoPeriodo / totalDebido).toFixed(4))
          : 0,
      delta: Number((totalPagadoPeriodo - totalDebido).toFixed(2)),
      desvioTotal: Number((totalDebido - totalPagadoPeriodo).toFixed(2)),

      // Estado por grupos
      grupos: {
        paid: gruposPaid,
        partial: gruposPartial,
        unpaid: gruposUnpaid,
        paidPct: pct(gruposPaid, totals.totalGrupos),
        partialPct: pct(gruposPartial, totals.totalGrupos),
        unpaidPct: pct(gruposUnpaid, totals.totalGrupos),
      },

      ticketsGlobal,
      mix,

      // Adopción de usarCuotaIdeal
      adoption, // { titular: {usingIdeal, usingCuota, rate}, group: {...} }

      // Comparaciones de ingresos y base ideal vs cuota
      revenue,
      idealVsCuota, // { idealSum, cuotaSum, deltaAmount, deltaPct }

      // Impacto de política (pérdida potencial)
      policyImpact, // { lostVsTargetAmount, lostPctVsTarget, lostPctVsCollected }
    };

    return res.json({
      ok: true,
      data: {
        summary,
        byCobrador, // due vs paid (base seleccionada)
        byCobradorIdealVsCuota, // NUEVO: ideal/cuota + adopción por cobrador
        byPlan,
        diffHistogram,
        upliftHistogram, // NUEVO: distribución de (ideal - cuota)
        ledgerSnapshot,
        coverage: coverageRows,
        paidShareByUser,
        ledgerShareByUser,
        topUpliftGroups, // NUEVO: top oportunidades (grupos)
      },
      meta: {
        scope: {
          period,
          idCobrador: idCobradorFilter,
          method: methodFilter,
          channel: channelFilter,
          dueMode,
          dueMember,
        },
        generatedAt: new Date().toISOString(),
        warnings: { paymentsUsingFallbackDate: pay.paymentsUsingFallbackDate },
      },
    });
  } catch (err) {
    next(err);
  }
}
