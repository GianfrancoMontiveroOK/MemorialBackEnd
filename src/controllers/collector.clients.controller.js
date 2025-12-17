// src/controllers/collector.clients.controller.js
import mongoose from "mongoose";
import Cliente from "../models/client.model.js";
import Payment from "../models/payment.model.js";
import LedgerEntry from "../models/ledger-entry.model.js"; // ⬅️ ajusta el path si es distinto
import User from "../models/user.model.js";
import { getClientPeriodState } from "../services/debt.service.js";
import {
  isObjectId,
  toInt,
  toDir,
  onlyDigits,
  projectCollector,
  yyyymmAR,
  comparePeriod,
} from "./payments.shared.js";

const { Types } = mongoose;
const normalizeDateStart = (v) => {
  if (!v) return null;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  d.setHours(0, 0, 0, 0);
  return d;
};

const normalizeDateEnd = (v) => {
  if (!v) return null;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  d.setHours(23, 59, 59, 999);
  return d;
};
/* ============ GET /collector/clientes ============ */
export async function listCollectorClients(req, res, next) {
  try {
    const myCollectorId = Number(req.user?.idCobrador);
    if (!Number.isFinite(myCollectorId)) {
      return res
        .status(400)
        .json({ ok: false, message: "Falta idCobrador en la sesión." });
    }

    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.min(toInt(req.query.limit, 25), 100);
    const qRaw = (req.query.q || "").trim();
    const sortByParam = (req.query.sortBy || "createdAt").toString();
    const sortDirParam = toDir(req.query.sortDir || req.query.order || "desc");

    const SORTABLE = new Set([
      "createdAt",
      "idCliente",
      "nombre",
      "ingreso",
      "cuota",
      "cuotaIdeal",
      "updatedAt",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "createdAt";

    // Scope base
    const and = [{ idCobrador: myCollectorId }];

    // Búsqueda
    const or = [];
    if (qRaw) {
      const esc = qRaw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const isNumeric = /^\d+$/.test(qRaw);
      const qDigits = onlyDigits(qRaw);

      or.push({ nombre: { $regex: esc, $options: "i" } });
      or.push({ domicilio: { $regex: esc, $options: "i" } });
      if (isNumeric) {
        or.push({ idCliente: Number(qRaw) });
      } else if (qDigits.length >= 3) {
        or.push({ cp: { $regex: qDigits, $options: "i" } });
      }
    }
    const matchStage =
      or.length > 0 ? { $and: [...and, { $or: or }] } : { $and: and };

    // Período actual en formatos útiles
    const now = new Date();
    const NOW_PERIOD = yyyymmAR(now); // "YYYY-MM"
    const NOW_NUM = now.getFullYear() * 100 + (now.getMonth() + 1); // YYYYMM num

    // Proyección final del front: agregamos billing explícito
    const PROJECT_FINAL = { ...projectCollector, billing: 1 };

    const pipeline = [
      { $match: matchStage },

      // normalizaciones y helpers
      {
        $addFields: {
          createdAtSafe: { $ifNull: ["$createdAt", { $toDate: "$_id" }] },
          _rankTitular: { $cond: [{ $eq: ["$rol", "TITULAR"] }, 0, 1] },
          _rankIntegrante: {
            $cond: [
              {
                $and: [
                  { $isNumber: "$integrante" },
                  { $gte: ["$integrante", 0] },
                ],
              },
              "$integrante",
              9999,
            ],
          },
          _cuotaVigente: {
            $cond: [
              { $eq: [{ $ifNull: ["$usarCuotaIdeal", false] }, true] },
              { $ifNull: ["$cuotaIdeal", 0] },
              { $ifNull: ["$cuota", 0] },
            ],
          },
          __isActive: {
            $and: [
              { $ne: [{ $type: "$baja" }, "date"] },
              {
                $or: [
                  { $eq: ["$activo", true] },
                  { $not: [{ $eq: ["$activo", false] }] },
                ],
              },
            ],
          },
        },
      },

      // ordenar para quedarnos con el primer doc del grupo (titular primero)
      {
        $sort: {
          idCliente: 1,
          _rankTitular: 1,
          _rankIntegrante: 1,
          createdAtSafe: 1,
          _id: 1,
        },
      },

      // agrupar por grupo (idCliente)
      {
        $group: {
          _id: "$idCliente",
          firstDoc: { $first: "$$ROOT" },
          integrantesCount: { $sum: { $cond: ["$__isActive", 1, 0] } },
          cremacionesCount: {
            $sum: {
              $cond: [
                { $and: ["$__isActive", { $toBool: "$cremacion" }] },
                1,
                0,
              ],
            },
          },
          edadMax: {
            $max: {
              $cond: [
                "$__isActive",
                {
                  $cond: [
                    { $isNumber: "$edad" },
                    "$edad",
                    { $ifNull: ["$edad", 0] },
                  ],
                },
                0,
              ],
            },
          },
          createdAtSafe: { $min: "$createdAtSafe" },
          updatedAtMax: { $max: "$updatedAt" },
        },
      },

      // dejar la vista para UI
      {
        $project: {
          _id: "$firstDoc._id",
          idCliente: "$_id",
          nombre: "$firstDoc.nombre",
          nombreTitular: "$firstDoc.nombreTitular",
          domicilio: "$firstDoc.domicilio",
          ciudad: "$firstDoc.ciudad",
          provincia: "$firstDoc.provincia",
          cp: "$firstDoc.cp",
          telefono: "$firstDoc.telefono",
          cuota: "$firstDoc.cuota",
          cuotaIdeal: "$firstDoc.cuotaIdeal",
          usarCuotaIdeal: "$firstDoc.usarCuotaIdeal",
          cuotaVigente: "$firstDoc._cuotaVigente",
          sexo: "$firstDoc.sexo",
          idCobrador: "$firstDoc.idCobrador",
          activo: "$firstDoc.activo",
          ingreso: "$firstDoc.ingreso",
          vigencia: "$firstDoc.vigencia",
          baja: "$firstDoc.baja",
          createdAt: "$firstDoc.createdAt",
          updatedAt: "$firstDoc.updatedAt",
          rol: "$firstDoc.rol",
          integrante: "$firstDoc.integrante",
          integrantesCount: 1,
          cremacionesCount: 1,
          edadMax: 1,
          createdAtSafe: 1,
          updatedAtMax: 1,
        },
      },

      // lookup pagos: último período y suma aplicada al período actual
      {
        $lookup: {
          from: Payment.collection.name,
          let: { idCli: "$idCliente" },
          pipeline: [
            {
              $match: {
                $expr: { $eq: ["$cliente.idCliente", "$$idCli"] },
                status: { $in: ["posted", "settled"] },
              },
            },
            { $unwind: "$allocations" },
            {
              $group: {
                _id: "$cliente.idCliente",
                maxPeriodPaid: { $max: "$allocations.period" }, // "YYYY-MM"
                paidNowSum: {
                  $sum: {
                    $cond: [
                      { $eq: ["$allocations.period", NOW_PERIOD] },
                      {
                        $ifNull: [
                          "$allocations.amountApplied",
                          "$allocations.amount",
                        ],
                      },
                      0,
                    ],
                  },
                },
              },
            },
          ],
          as: "billAgg",
        },
      },
      { $addFields: { billAgg: { $arrayElemAt: ["$billAgg", 0] } } },

      // convertir maxPeriodPaid → número YYYYMM (si no hay, considerar "mes anterior" para marcar due)
      {
        $addFields: {
          __maxNum: {
            $cond: [
              { $ifNull: ["$billAgg.maxPeriodPaid", false] },
              {
                $let: {
                  vars: {
                    y: {
                      $toInt: {
                        $substrBytes: ["$billAgg.maxPeriodPaid", 0, 4],
                      },
                    },
                    m: {
                      $toInt: {
                        $substrBytes: ["$billAgg.maxPeriodPaid", 5, 2],
                      },
                    },
                  },
                  in: { $add: [{ $multiply: ["$$y", 100] }, "$$m"] },
                },
              },
              { $subtract: [NOW_NUM, 1] }, // SIN pagos: se considera atrasado (al menos 1 mes)
            ],
          },
        },
      },

      // contadores base
      {
        $addFields: {
          "billing.lastPaidPeriod": "$billAgg.maxPeriodPaid",
          "billing.paidNow": { $ifNull: ["$billAgg.paidNowSum", 0] },
          "billing.chargeNow": { $ifNull: ["$cuotaVigente", 0] },
          "billing.arrearsCount": {
            $max: [{ $subtract: [NOW_NUM, "$__maxNum"] }, 0],
          },
          "billing.aheadCount": {
            $max: [{ $subtract: ["$__maxNum", NOW_NUM] }, 0],
          },
        },
      },

      // estado actual
      {
        $addFields: {
          "billing.current": {
            $cond: [
              {
                $or: [
                  { $gte: ["$billing.paidNow", "$billing.chargeNow"] },
                  { $gt: ["$__maxNum", NOW_NUM] },
                ],
              },
              "paid",
              "due",
            ],
          },
        },
      },

      // orden final (sobre grupos)
      {
        $sort:
          sortBy === "createdAt"
            ? { createdAtSafe: sortDirParam, _id: sortDirParam }
            : { [sortBy]: sortDirParam, _id: sortDirParam },
      },

      // ⬅️ paginación SIEMPRE
      { $skip: (page - 1) * limit },
      { $limit: limit },

      // proyección final (incluye billing)
      { $project: PROJECT_FINAL },
    ];

    const [items, totalRes] = await Promise.all([
      Cliente.aggregate(pipeline).allowDiskUse(true),
      Cliente.aggregate([
        { $match: matchStage },
        { $group: { _id: "$idCliente" } },
        { $count: "n" },
      ]),
    ]);
    const total = totalRes?.[0]?.n || 0;

    return res.json({
      ok: true,
      items,
      total,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
    });
  } catch (err) {
    next(err);
  }
}

/* ============ GET /collector/clientes/:id ============ */
export async function getCollectorClientById(req, res, next) {
  try {
    const myCollectorId = Number(req.user?.idCobrador);
    if (!Number.isFinite(myCollectorId)) {
      return res
        .status(400)
        .json({ ok: false, message: "Falta idCobrador en la sesión." });
    }

    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ ok: false, message: "ID inválido" });

    const doc = await Cliente.findById(id)
      .select(
        "_id idCliente nombre nombreTitular rol integrante sexo edad " +
          "domicilio ciudad provincia cp telefono " +
          "cuota cuotaIdeal usarCuotaIdeal activo ingreso vigencia baja " +
          "createdAt updatedAt idCobrador"
      )
      .lean();

    if (!doc)
      return res
        .status(404)
        .json({ ok: false, message: "Cliente no encontrado" });
    if (Number(doc.idCobrador) !== myCollectorId)
      return res
        .status(403)
        .json({ ok: false, message: "El cliente no pertenece a tu cartera." });

    const cuotaVigente = doc.usarCuotaIdeal
      ? Number(doc.cuotaIdeal || 0)
      : Number(doc.cuota || 0);
    const data = { ...doc, cuotaVigente };

    const familyRaw = await Cliente.find({ idCliente: doc.idCliente })
      .select(
        "_id idCliente nombre nombreTitular rol integrante sexo edad " +
          "domicilio ciudad provincia cp telefono " +
          "cuota cuotaIdeal usarCuotaIdeal activo ingreso vigencia baja " +
          "createdAt updatedAt idCobrador"
      )
      .sort({ rol: 1, integrante: 1, nombre: 1, _id: 1 })
      .lean();

    const family = (familyRaw || []).map((m) => ({
      ...m,
      cuotaVigente: m.usarCuotaIdeal
        ? Number(m.cuotaIdeal || 0)
        : Number(m.cuota || 0),
    }));

    return res.json({ ok: true, data, family });
  } catch (err) {
    next(err);
  }
}

/* ============ GET /collector/clientes/:id/deuda ============ */
export async function getCollectorClientDebt(req, res, next) {
  try {
    const myCollectorId = Number(req.user?.idCobrador);
    if (!Number.isFinite(myCollectorId)) {
      return res
        .status(400)
        .json({ ok: false, message: "Falta idCobrador en la sesión." });
    }
    const myCollectorIdStr = String(myCollectorId);

    const id = String(req.params.id || "").trim();
    if (!isObjectId(id)) {
      return res.status(400).json({ ok: false, message: "ID inválido" });
    }

    const member = await Cliente.findById(id)
      .select(
        "_id idCliente nombre nombreTitular idCobrador usarCuotaIdeal cuota cuotaIdeal"
      )
      .lean();

    if (!member) {
      return res
        .status(404)
        .json({ ok: false, message: "Cliente no encontrado" });
    }

    // ✅ cartera robusta (idCobrador puede ser number o string)
    const memberCidStr = member?.idCobrador != null ? String(member.idCobrador) : "";
    if (memberCidStr !== myCollectorIdStr) {
      return res
        .status(403)
        .json({ ok: false, message: "El cliente no pertenece a tu cartera." });
    }

    const { from, to, includeFuture } = req.query || {};
    const base = await getClientPeriodState(member, {
      from,
      to,
      includeFuture: Number(includeFuture),
    });

    let periods = Array.isArray(base?.periods) ? [...base.periods] : [];
    const nowPeriod = yyyymmAR(new Date());

    const cuotaVigente =
      Number(member.usarCuotaIdeal ? member.cuotaIdeal : member.cuota) || 0;

    // ✅ Suma ya imputada al período actual (solo pagos válidos)
    const paidNowAgg = await Payment.aggregate([
      {
        $match: {
          status: { $in: ["posted", "settled"] },
          // memberId robusto (ObjectId/string)
          $expr: {
            $eq: [{ $toString: "$cliente.memberId" }, String(member._id)],
          },
        },
      },
      { $unwind: "$allocations" },
      { $match: { "allocations.period": nowPeriod } },
      {
        $group: {
          _id: null,
          sum: {
            $sum: {
              $ifNull: ["$allocations.amountApplied", "$allocations.amount"],
            },
          },
        },
      },
    ]).allowDiskUse(true);

    const alreadyAppliedNow = Number(paidNowAgg?.[0]?.sum || 0);

    // Ajustar/inyectar período actual
    const idx = periods.findIndex((p) => p?.period === nowPeriod);
    const computedBalanceNow = Math.max(0, cuotaVigente - alreadyAppliedNow);

    if (idx === -1) {
      periods.push({
        period: nowPeriod,
        charge: cuotaVigente,
        paid: alreadyAppliedNow,
        balance: computedBalanceNow,
        status: computedBalanceNow > 0 ? "due" : "paid",
      });
    } else {
      const cur = periods[idx] || {};
      const charge = Number(cur.charge ?? cuotaVigente);
      const paid = Math.max(Number(cur.paid || 0), alreadyAppliedNow);
      const balance = Math.max(0, charge - paid);
      periods[idx] = {
        ...cur,
        period: nowPeriod,
        charge,
        paid,
        balance,
        status: balance > 0 ? "due" : "paid",
      };
    }

    const totalDueUpToNow = periods
      .filter((p) => comparePeriod(p.period, nowPeriod) <= 0)
      .reduce((acc, p) => acc + Math.max(0, Number(p.balance || 0)), 0);

    return res.json({
      ok: true,
      clientId: String(member._id),
      currency: "ARS",
      from: base?.from || from || null,
      to: base?.to || to || null,
      grandTotals: base?.grandTotals || null,
      periods: periods.sort((a, b) =>
        a.period < b.period ? -1 : a.period > b.period ? 1 : 0
      ),
      summary: {
        nowPeriod,
        cuotaVigente,
        alreadyAppliedNow,
        balanceNow: computedBalanceNow,
        totalDueUpToNow,
      },
    });
  } catch (err) {
    next(err);
  }
}


export async function getCollectorSummary(req, res, next) {
  try {
    const myCollectorId = Number(req.user?.idCobrador);
    if (!Number.isFinite(myCollectorId)) {
      return res
        .status(400)
        .json({ ok: false, message: "Falta idCobrador en la sesión." });
    }
    const myCollectorIdStr = String(myCollectorId);

    // ─────────────────────── Rango de fechas / período ───────────────────────
    const now = new Date();

    // Si en tu proyecto existen normalizeDateStart/End como en admin, los usamos:
    let rangeStart = normalizeDateStart(req.query?.dateFrom);
    let rangeEnd = normalizeDateEnd(req.query?.dateTo);

    // Si no mandan rango, usamos el mes actual completo
    if (!rangeStart || !rangeEnd) {
      const yearNow = now.getFullYear();
      const monthNow = now.getMonth(); // 0–11
      rangeStart = new Date(yearNow, monthNow, 1, 0, 0, 0, 0);
      rangeEnd = new Date(yearNow, monthNow + 1, 0, 23, 59, 59, 999);
    }

    const period = yyyymmAR(rangeStart);
    const year = rangeStart.getFullYear();
    const month = rangeStart.getMonth();

    const monthNamesEs = [
      "enero","febrero","marzo","abril","mayo","junio",
      "julio","agosto","septiembre","octubre","noviembre","diciembre",
    ];
    const label = `${monthNamesEs[month] || "Mes"} ${year}`.replace(
      /^\w/,
      (c) => c.toUpperCase()
    );

    const daysInPeriod = new Date(year, month + 1, 0).getDate();
    const daysElapsed =
      now.getFullYear() === year && now.getMonth() === month
        ? now.getDate()
        : daysInPeriod;
    const daysRemaining = Math.max(daysInPeriod - daysElapsed, 0);

    // Días hábiles (lun–sáb)
    const countWorkingDays = () => {
      let total = 0;
      let elapsed = 0;
      for (let d = 1; d <= daysInPeriod; d++) {
        const dt = new Date(year, month, d);
        const day = dt.getDay(); // 0=dom..6=sáb
        const isWorking = day >= 1 && day <= 6;
        if (!isWorking) continue;
        total++;
        if (
          now.getFullYear() === year &&
          now.getMonth() === month &&
          d <= now.getDate()
        ) {
          elapsed++;
        } else if (now.getFullYear() > year || now.getMonth() > month) {
          elapsed = total;
        }
      }
      return { total, elapsed, remaining: Math.max(total - elapsed, 0) };
    };

    const {
      total: workingDaysTotal,
      elapsed: workingDaysElapsed,
      remaining: workingDaysRemaining,
    } = countWorkingDays();

    // ───────────────────── Config de comisión (User) ─────────────────────
    let baseCommissionRate = 0;
    let graceDays = 7;
    let penaltyPerDay = 0;

    const userDoc = await User.findById(req.user._id)
      .select("porcentajeCobrador commissionGraceDays commissionPenaltyPerDay name email idCobrador")
      .lean();

    if (userDoc) {
      const rawPercent = userDoc.porcentajeCobrador;
      if (typeof rawPercent === "number" && rawPercent > 0) {
        baseCommissionRate = rawPercent <= 1 ? rawPercent : rawPercent / 100;
      }
      if (
        userDoc.commissionGraceDays != null &&
        Number.isFinite(Number(userDoc.commissionGraceDays))
      ) {
        graceDays = Number(userDoc.commissionGraceDays);
      }
      const rawPenalty = userDoc.commissionPenaltyPerDay;
      if (typeof rawPenalty === "number" && rawPenalty > 0) {
        penaltyPerDay = rawPenalty <= 1 ? rawPenalty : rawPenalty / 100;
      }
    }

    // ───────────────── Clientes asignados + cuota vigente ─────────────────
    const clientsAgg = await Cliente.aggregate([
      {
        $match: {
          $or: [{ idCobrador: myCollectorId }, { idCobrador: myCollectorIdStr }],
        },
      },
      {
        $addFields: {
          createdAtSafe: { $ifNull: ["$createdAt", { $toDate: "$_id" }] },
          _rankTitular: { $cond: [{ $eq: ["$rol", "TITULAR"] }, 0, 1] },
          _rankIntegrante: {
            $cond: [
              { $and: [{ $isNumber: "$integrante" }, { $gte: ["$integrante", 0] }] },
              "$integrante",
              9999,
            ],
          },
          _cuotaVigente: {
            $cond: [
              { $eq: [{ $ifNull: ["$usarCuotaIdeal", false] }, true] },
              { $ifNull: ["$cuotaIdeal", 0] },
              { $ifNull: ["$cuota", 0] },
            ],
          },
          __isActive: {
            $and: [
              { $ne: [{ $type: "$baja" }, "date"] },
              {
                $or: [
                  { $eq: ["$activo", true] },
                  { $not: [{ $eq: ["$activo", false] }] },
                ],
              },
            ],
          },
        },
      },
      {
        $sort: {
          idCliente: 1,
          _rankTitular: 1,
          _rankIntegrante: 1,
          createdAtSafe: 1,
          _id: 1,
        },
      },
      {
        $group: {
          _id: "$idCliente",
          firstDoc: { $first: "$$ROOT" },
          cuotaVigente: { $first: "$_cuotaVigente" },
          isActive: { $max: "$__isActive" },
        },
      },
      { $match: { isActive: true } },
      {
        $group: {
          _id: null,
          assignedClients: { $sum: 1 },
          totalChargeNow: { $sum: "$cuotaVigente" },
        },
      },
    ]).allowDiskUse(true);

    const assignedClients = clientsAgg?.[0]?.assignedClients || 0;
    const totalChargeNow = clientsAgg?.[0]?.totalChargeNow || 0;

    // ───────────────── Movimientos cliente → cobrador (BASE REAL) ─────────────────
    const ledgerAgg = await LedgerEntry.aggregate([
      {
        $match: {
          accountCode: "CAJA_COBRADOR",
          side: "debit",
          "dimensions.idCliente": { $exists: true, $ne: null },
          $or: [
            { "dimensions.idCobrador": myCollectorId },
            { "dimensions.idCobrador": myCollectorIdStr },
          ],
          $expr: {
            $and: [
              { $gte: [{ $ifNull: ["$postedAt", "$createdAt"] }, rangeStart] },
              { $lte: [{ $ifNull: ["$postedAt", "$createdAt"] }, rangeEnd] },
            ],
          },
        },
      },
      {
        $group: {
          _id: null,
          totalCollected: { $sum: "$amount" },
          clients: { $addToSet: "$dimensions.idCliente" },
        },
      },
    ]).allowDiskUse(true);

    const totalCollectedThisPeriod = ledgerAgg?.[0]?.totalCollected || 0;
    const clientsWithPayment = Array.isArray(ledgerAgg?.[0]?.clients)
      ? ledgerAgg[0].clients.length
      : 0;
    const clientsWithoutPayment = Math.max(assignedClients - clientsWithPayment, 0);

    // ───────────────── Saldo actual en mano del cobrador ─────────────────
    const cashAccounts = ["CAJA_COBRADOR", "A_RENDIR_COBRADOR"];
    const balanceAgg = await LedgerEntry.aggregate([
      {
        $match: {
          accountCode: { $in: cashAccounts },
          $or: [
            { "dimensions.idCobrador": myCollectorId },
            { "dimensions.idCobrador": myCollectorIdStr },
            { "dimensions.cobradorId": myCollectorIdStr }, // compat viejas
          ],
        },
      },
      {
        $group: {
          _id: null,
          debits: { $sum: { $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0] } },
          credits: { $sum: { $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0] } },
        },
      },
    ]).allowDiskUse(true);

    const debits = balanceAgg?.[0]?.debits || 0;
    const credits = balanceAgg?.[0]?.credits || 0;
    const collectorBalance = debits - credits;

    // ───────────────── Comisiones (igual admin) ─────────────────
    const expectedCommission = totalChargeNow * baseCommissionRate;
    const totalCommission = totalCollectedThisPeriod * baseCommissionRate;

    const paidAgg = await LedgerEntry.aggregate([
      {
        $match: {
          accountCode: "COMISION_COBRADOR",
          side: "debit",
          $or: [
            { "dimensions.idCobrador": myCollectorId },
            { "dimensions.idCobrador": myCollectorIdStr },
          ],
          $expr: {
            $and: [
              { $gte: [{ $ifNull: ["$postedAt", "$createdAt"] }, rangeStart] },
              { $lte: [{ $ifNull: ["$postedAt", "$createdAt"] }, rangeEnd] },
            ],
          },
        },
      },
      { $group: { _id: null, totalPaid: { $sum: "$amount" } } },
    ]).allowDiskUse(true);

    const alreadyPaid = paidAgg?.[0]?.totalPaid || 0;
    const pendingCommission = Math.max(totalCommission - alreadyPaid, 0);

    const rootAmounts = {
      expectedCommission,
      totalCommission,
      totalCommissionNoPenalty: totalCommission, // (igual que admin por ahora)
      alreadyPaid,
      pendingCommission,
    };

    return res.json({
      ok: true,
      data: {
        collector: {
          userId: req.user?._id,
          name: userDoc?.name || null,
          email: userDoc?.email || null,
          idCobrador: myCollectorId,
        },
        month: {
          period,
          label,
          daysInPeriod,
          daysElapsed,
          daysRemaining,
          workingDaysTotal,
          workingDaysElapsed,
          workingDaysRemaining,
          totalChargeNow,
          totalCollectedThisPeriod,
          clientsWithPayment,
          clientsWithoutPayment,
          assignedClients, // útil para UI
        },
        balance: {
          collectorBalance,
        },
        // mismo contrato que admin
        amounts: rootAmounts,
        commissions: {
          config: {
            basePercent: baseCommissionRate,
            graceDays,
            penaltyPerDay,
          },
          amounts: rootAmounts,
        },
      },
    });
  } catch (err) {
    next(err);
  }
}

