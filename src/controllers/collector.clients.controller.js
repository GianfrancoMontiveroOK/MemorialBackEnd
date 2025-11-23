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
} from "./collector.shared.js";

const { Types } = mongoose;

/* ============ GET /collector/clientes ============ */
export async function listCollectorClients(req, res, next) {
  try {
    const myCollectorId = Number(req.user?.idCobrador);
    if (!Number.isFinite(myCollectorId)) {
      return res
        .status(400)
        .json({ ok: false, message: "Falta idCobrador en la sesión." });
    }

    // full=1 => trae todo (sin skip/limit) para paginar/filtrar en UI
    const FULL = String(req.query.full || "") === "1";

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
              { $subtract: [NOW_NUM, 1] }, // ⬅️ SIN pagos: se considera atrasado (al menos 1 mes)
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

      // estado actual:
      // - paid si cubrió el mes actual (paidNow >= chargeNow) o si está adelantado (__maxNum > NOW_NUM)
      // - sino due
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

      // orden final
      {
        $sort:
          sortBy === "createdAt"
            ? { createdAtSafe: sortDirParam, _id: sortDirParam }
            : { [sortBy]: sortDirParam, _id: sortDirParam },
      },

      // paginación solo si NO es full
      ...(!FULL ? [{ $skip: (page - 1) * limit }, { $limit: limit }] : []),

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
      page: FULL ? 1 : page,
      pageSize: FULL ? items.length : limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
      full: FULL ? 1 : 0,
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

    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ ok: false, message: "ID inválido" });

    const member = await Cliente.findById(id)
      .select(
        "_id idCliente nombre nombreTitular idCobrador usarCuotaIdeal cuota cuotaIdeal"
      )
      .lean();

    if (!member)
      return res
        .status(404)
        .json({ ok: false, message: "Cliente no encontrado" });
    if (Number(member.idCobrador) !== myCollectorId)
      return res
        .status(403)
        .json({ ok: false, message: "El cliente no pertenece a tu cartera." });

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

    // Suma ya imputada al período actual
    const paidNowAgg = await Payment.aggregate([
      { $match: { "cliente.memberId": new Types.ObjectId(member._id) } },
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

    /* ────────────────────── Fecha / período actual ────────────────────── */

    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth(); // 0–11
    const period = yyyymmAR(now); // "YYYY-MM"

    const monthStart = new Date(year, month, 1, 0, 0, 0, 0);
    const monthEnd = new Date(year, month + 1, 0, 23, 59, 59, 999);

    const daysInPeriod = new Date(year, month + 1, 0).getDate();
    const daysElapsed = now.getDate();
    const daysRemaining = Math.max(daysInPeriod - daysElapsed, 0);

    // Días hábiles (lun–sáb) del mes
    const countWorkingDays = () => {
      let total = 0;
      let elapsed = 0;

      for (let d = 1; d <= daysInPeriod; d++) {
        const dt = new Date(year, month, d);
        const day = dt.getDay(); // 0 = dom, 1 = lun, ..., 6 = sáb
        const isWorking = day >= 1 && day <= 6; // lun–sáb

        if (!isWorking) continue;
        total++;
        if (d <= daysElapsed) elapsed++;
      }
      const remaining = Math.max(total - elapsed, 0);
      return { total, elapsed, remaining };
    };

    const {
      total: workingDaysTotal,
      elapsed: workingDaysElapsed,
      remaining: workingDaysRemaining,
    } = countWorkingDays();

    const diffInDays = (from, to) => {
      const a = new Date(from);
      const b = new Date(to);
      if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return 0;
      const ms = b.getTime() - a.getTime();
      return Math.max(0, Math.floor(ms / (1000 * 60 * 60 * 24)));
    };

    /* ────────────────────── Config de comisión (User) ────────────────────── */

    let baseCommissionRate = 0; // decimal (0.05 = 5 %)
    let graceDays = 7;
    let penaltyPerDay = 0; // caída de tasa por día extra, decimal

    try {
      const userDoc = await User.findById(req.user._id)
        .select(
          "porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .lean();

      // Porcentaje principal (soporta 5 o 0.05)
      const rawPercent = userDoc?.porcentajeCobrador;
      if (typeof rawPercent === "number" && rawPercent > 0) {
        baseCommissionRate = rawPercent <= 1 ? rawPercent : rawPercent / 100;
      }

      // Días de gracia configurables
      if (
        userDoc &&
        userDoc.commissionGraceDays != null &&
        Number.isFinite(Number(userDoc.commissionGraceDays))
      ) {
        graceDays = Number(userDoc.commissionGraceDays);
      }

      // Penalidad por día (soporta 0.1 = 10% de la tasa por día, o 10 = 10% también)
      const rawPenalty = userDoc?.commissionPenaltyPerDay;
      if (typeof rawPenalty === "number" && rawPenalty > 0) {
        penaltyPerDay = rawPenalty <= 1 ? rawPenalty : rawPenalty / 100;
      }
    } catch {
      // Si falla, usamos defaults suaves
      baseCommissionRate = 0;
      graceDays = 7;
      penaltyPerDay = 0;
    }

    /* ────────────────────── Clientes asignados y cuota del mes ────────────────────── */

    const clientsAgg = await Cliente.aggregate([
      { $match: { idCobrador: myCollectorId } },

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

    /* ────────────────────── Pagos del período + comisión pago a pago ────────────────────── */

    const paymentsAgg = await Payment.aggregate([
      {
        $match: {
          "collector.idCobrador": myCollectorId,
          status: { $in: ["posted", "settled"] },
          $expr: {
            $and: [
              {
                $gte: [{ $ifNull: ["$postedAt", "$createdAt"] }, monthStart],
              },
              {
                $lte: [{ $ifNull: ["$postedAt", "$createdAt"] }, monthEnd],
              },
            ],
          },
        },
      },
      { $unwind: "$allocations" },
      {
        $match: {
          "allocations.period": period, // sólo lo imputado a este período
        },
      },
      {
        $project: {
          _id: 1,
          postedAt: { $ifNull: ["$postedAt", "$createdAt"] },
          amountApplied: "$allocations.amountApplied",
          "cliente.idCliente": 1,
        },
      },
    ]).allowDiskUse(true);

    const clientsSet = new Set();
    let totalCollectedThisPeriod = 0;
    let totalCommissionIdeal = 0; // sin penalidad
    let totalCommissionDiscounted = 0; // con penalidad por demora

    for (const p of paymentsAgg) {
      const clientId = p.cliente?.idCliente;
      if (clientId != null) clientsSet.add(clientId);

      const applied = Number(p.amountApplied) || 0;
      totalCollectedThisPeriod += applied;

      // Comisión ideal si todo estuviera en término
      const idealRate = baseCommissionRate;
      const idealCommission = applied * idealRate;
      totalCommissionIdeal += idealCommission;

      // Comisión efectiva con descuento por demora (si corresponde)
      let effectiveRate = idealRate;

      if (idealRate > 0 && penaltyPerDay > 0 && p.postedAt) {
        const daysHeld = diffInDays(p.postedAt, now);
        if (daysHeld > graceDays) {
          const extraDays = daysHeld - graceDays;
          const reduction = penaltyPerDay * extraDays;
          effectiveRate = Math.max(0, idealRate - reduction);
        }
      }

      const discountedCommission = applied * effectiveRate;
      totalCommissionDiscounted += discountedCommission;
    }

    const clientsWithPayment = clientsSet.size;
    const clientsWithoutPayment = Math.max(
      assignedClients - clientsWithPayment,
      0
    );

    /* ────────────────────── Saldo en mano (Ledger) ────────────────────── */

    const cashAccounts = ["CAJA_COBRADOR", "A_RENDIR_COBRADOR"];

    const balanceAgg = await LedgerEntry.aggregate([
      {
        $match: {
          "dimensions.idCobrador": myCollectorId,
          accountCode: { $in: cashAccounts },
        },
      },
      {
        $group: {
          _id: null,
          debits: {
            $sum: {
              $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0],
            },
          },
          credits: {
            $sum: {
              $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0],
            },
          },
        },
      },
    ]).allowDiskUse(true);

    const debits = balanceAgg?.[0]?.debits || 0;
    const credits = balanceAgg?.[0]?.credits || 0;
    const collectorBalance = debits - credits;

    /* ────────────────────── Comisiones globales ────────────────────── */

    // Ganancia esperada: si cobrara toda la cartera en término
    const expectedCommission = totalChargeNow * baseCommissionRate;

    // Ganancia actual:
    // - totalCommissionIdeal: lo que sería si todos los pagos del período
    //   mantuvieran el % completo.
    // - totalCommissionDiscounted: lo que realmente corresponde hoy,
    //   aplicando la caída por días de más con el dinero.
    const currentCommission = totalCommissionDiscounted;

    /* ────────────────────── Armado de respuesta ────────────────────── */

    const monthNamesEs = [
      "enero",
      "febrero",
      "marzo",
      "abril",
      "mayo",
      "junio",
      "julio",
      "agosto",
      "septiembre",
      "octubre",
      "noviembre",
      "diciembre",
    ];
    const label = `${monthNamesEs[month] || "Mes"} ${year}`.replace(
      /^\w/,
      (c) => c.toUpperCase()
    );

    return res.json({
      ok: true,
      data: {
        assignedClients,
        month: {
          period, // "YYYY-MM"
          label,
          // días de calendario
          daysInPeriod,
          daysElapsed,
          daysRemaining,
          // días hábiles (lun–sáb)
          workingDaysTotal,
          workingDaysElapsed,
          workingDaysRemaining,
          // cobranza
          totalChargeNow, // suma de cuotas vigentes de la cartera
          totalCollectedThisPeriod,
          clientsWithPayment,
          clientsWithoutPayment,
        },
        balance: {
          collectorBalance,
        },
        commissions: {
          config: {
            basePercent: baseCommissionRate, // decimal (0.05 = 5 %)
            graceDays,
            penaltyPerDay, // decimal, caída de la tasa por día extra
          },
          amounts: {
            expectedCommission, // si cobrara toda la cartera en término
            totalCommission: currentCommission, // ya con descuento
            totalCommissionNoPenalty: totalCommissionIdeal, // referencia sin descuento
          },
        },
      },
    });
  } catch (err) {
    next(err);
  }
}
