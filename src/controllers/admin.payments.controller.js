// src/controllers/admin.payments.controller.js
import mongoose from "mongoose";
import crypto from "crypto";

import Cliente from "../models/client.model.js";
import Payment from "../models/payment.model.js";
import LedgerEntry from "../models/ledger-entry.model.js";
import Receipt from "../models/receipt.model.js";
import User from "../models/user.model.js";

import { generateReceipt } from "../services/receipt.service.js";
import { enqueue } from "../services/outbox.service.js";
import { getClientPeriodState } from "../services/debt.service.js";

import {
  ACCOUNTS,
  isObjectId,
  toInt,
  toDir,
  yyyymmAR,
  comparePeriod,
  fifoAllocateUntilNow,
  serializePayment,
} from "./payments.shared.js";

const { Types } = mongoose;

/**
 * Cobro desde OFICINA (admin / superAdmin)
 *
 * POST /admin/pagos
 *
 * Diferencias vs cobrador:
 *  - No valida cartera del cobrador (oficina puede cobrar a cualquiera).
 *  - Usa idCobrador simbólico ADMIN_COLLECTOR_ID (ej. 0).
 *  - Asiento:
 *      DEBIT  CAJA_ADMIN / CAJA_SUPERADMIN (según rol)
 *      CREDIT INGRESOS_CUOTAS
 */

const ADMIN_COLLECTOR_ID = 0; // idCobrador simbólico para caja de oficina

/* ============ POST /admin/pagos ============ */
export async function createAdminPayment(req, res, next) {
  const session = await mongoose.startSession();
  session.startTransaction();
  try {
    const myUserId = req.user?._id || req.user?.id;
    if (!myUserId) {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "Sesión inválida: falta userId." });
    }

    // Sólo admin / superAdmin pueden cobrar por caja de oficina
    const myUser = await User.findById(myUserId)
      .select("role")
      .session(session)
      .lean();

    if (!myUser) {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "Usuario no encontrado." });
    }

    if (!["admin", "superAdmin"].includes(myUser.role)) {
      await session.abortTransaction();
      return res.status(403).json({
        ok: false,
        message: "No tiene permiso para cobrar por caja de oficina.",
      });
    }

    const userRole = myUser.role; // "admin" | "superAdmin"

    const {
      clienteId,
      idCliente: legacyIdCliente,
      amount,
      method,
      notes,
      idempotencyKey,
      channel = "backoffice", // por defecto, cobro de oficina
      intendedPeriod,
      externalRef,
      geo,
      device,
      ip,
      cashSessionId,
      strategy = "auto", // "auto" | "manual"
      breakdown = [], // [{ period, amount }]
      collectedAt,
    } = req.body || {};

    if (!isObjectId(clienteId)) {
      await session.abortTransaction();
      return res.status(400).json({ ok: false, message: "clienteId inválido" });
    }

    // 1) Buscar cliente (SIN validar cartera)
    const member = await Cliente.findById(clienteId)
      .select(
        "_id idCliente nombre nombreTitular idCobrador usarCuotaIdeal cuota cuotaIdeal"
      )
      .session(session)
      .lean();

    if (!member) {
      await session.abortTransaction();
      return res
        .status(404)
        .json({ ok: false, message: "Cliente no encontrado" });
    }

    if (
      legacyIdCliente != null &&
      Number(legacyIdCliente) !== Number(member.idCliente)
    ) {
      await session.abortTransaction();
      return res.status(400).json({
        ok: false,
        message: "idCliente no coincide con el del cliente",
      });
    }

    // 2) Estado de deuda hasta período actual (igual que cobrador)
    const nowPeriod = yyyymmAR(new Date());
    let debtState = await getClientPeriodState(member, {
      to: nowPeriod,
      includeFuture: 0,
    });

    const totalDueUpToNow = (debtState?.periods || [])
      .filter((p) => comparePeriod(p.period, nowPeriod) <= 0)
      .reduce((acc, p) => acc + Math.max(0, Number(p.balance || 0)), 0);

    if (totalDueUpToNow <= 0) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        code: "CLIENT_UP_TO_DATE",
        message: "El cliente está al día hasta el período actual.",
        nowPeriod,
      });
    }

    // 3) Monto final (usa cuota vigente si no mandan amount)
    const computedAmount =
      Number(member.usarCuotaIdeal ? member.cuotaIdeal : member.cuota) || 0;
    const finalAmount = Number(amount) > 0 ? Number(amount) : computedAmount;

    if (!(finalAmount > 0)) {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "No hay cuota vigente definida (> 0)." });
    }

    // 4) Método + idempotencia
    const finalMethod = (method || "efectivo").toString().trim().toLowerCase();
    const finalIdem =
      (idempotencyKey && String(idempotencyKey).trim()) ||
      `pay_office_${member._id}_${Date.now()}_${crypto
        .randomBytes(4)
        .toString("hex")}`;

    const existing = await Payment.findOne({ idempotencyKey: finalIdem })
      .session(session)
      .lean();
    if (existing) {
      const rx = await Receipt.findOne({ paymentId: existing._id })
        .session(session)
        .lean();
      return res
        .status(200)
        .json({ ok: true, data: serializePayment(existing, rx) });
    }

    // 5) Construcción de imputaciones (igual filosofía que cobrador)
    let allocations = [];
    let periodsApplied = [];

    const balMap = new Map(
      (debtState?.periods || []).map((p) => [
        p.period,
        Math.max(0, Number(p.balance || 0)),
      ])
    );

    const STRAT = String(strategy || "auto").toLowerCase();

    if (STRAT === "manual") {
      let sum = 0;
      for (const row of breakdown) {
        const period = String(row?.period || "");
        const amt = Number(row?.amount || 0);
        if (!period || !(amt > 0)) {
          await session.abortTransaction();
          return res
            .status(400)
            .json({ ok: false, message: "breakdown inválido" });
        }
        if (comparePeriod(period, nowPeriod) > 0) {
          await session.abortTransaction();
          return res.status(409).json({
            ok: false,
            code: "PERIOD_IN_FUTURE",
            message: `No se puede imputar a un período futuro (${period}).`,
          });
        }
        const bal = balMap.get(period) || 0;
        if (amt > bal) {
          await session.abortTransaction();
          return res.status(409).json({
            ok: false,
            code: "OVERPAY_PERIOD",
            message: `El período ${period} no admite más cobros (saldo: ${bal}).`,
          });
        }
        sum += amt;
        allocations.push({
          period,
          amountApplied: amt,
          statusAfter: amt === bal ? "paid" : "partial",
          memberId: member._id,
        });
      }

      if (sum > finalAmount) {
        await session.abortTransaction();
        return res.status(409).json({
          ok: false,
          code: "BREAKDOWN_EXCEEDS_AMOUNT",
          message: "La suma del breakdown excede el monto del pago.",
        });
      }

      const remaining = finalAmount - sum;
      if (remaining > 0) {
        const { allocations: auto } = fifoAllocateUntilNow(
          debtState,
          nowPeriod,
          remaining
        );
        for (const a of auto) {
          const bal = balMap.get(a.period) || 0;
          allocations.push({
            period: a.period,
            amountApplied: a.amount,
            statusAfter: a.amount >= bal ? "paid" : "partial",
            memberId: member._id,
          });
        }
      }
      periodsApplied = Array.from(new Set(allocations.map((a) => a.period)));
    } else {
      const { allocations: fifo } = fifoAllocateUntilNow(
        debtState,
        nowPeriod,
        finalAmount
      );
      const totalAllocated = fifo.reduce((acc, a) => acc + a.amount, 0);
      if (totalAllocated <= 0) {
        await session.abortTransaction();
        return res.status(409).json({
          ok: false,
          code: "NOTHING_TO_ALLOCATE",
          message:
            "No hay períodos con saldo para imputar hasta el período actual.",
        });
      }
      allocations = fifo.map((a) => {
        const bal = balMap.get(a.period) || 0;
        return {
          period: a.period,
          amountApplied: a.amount,
          statusAfter: a.amount >= bal ? "paid" : "partial",
          memberId: member._id,
        };
      });
      periodsApplied = allocations.map((a) => a.period);
    }

    // 6) Re-chequeo anti-carrera con estado fresco dentro de la TX
    debtState = await getClientPeriodState(member, {
      to: nowPeriod,
      includeFuture: 0,
    });
    const freshBal = new Map(
      (debtState?.periods || []).map((p) => [
        p.period,
        Math.max(0, Number(p.balance || 0)),
      ])
    );
    for (const a of allocations) {
      const bal = freshBal.get(a.period) ?? 0;
      if (a.amount > bal + 0.0001) {
        await session.abortTransaction();
        return res.status(409).json({
          ok: false,
          code: "RACE_CONDITION_OVERPAY",
          message: `El período ${a.period} cambió y ya no admite ${a.amount} (saldo: ${bal}). Refrescá y reintentá.`,
        });
      } else {
        freshBal.set(a.period, Math.max(0, bal - a.amount));
      }
    }

    // 7) Crear Payment (draft → posted)
    const payDocs = await Payment.create(
      [
        {
          kind: "payment",
          cliente: {
            memberId: member._id,
            idCliente: member.idCliente,
            nombre: member.nombre,
            nombreTitular: member.nombreTitular || null,
          },
          collector: {
            idCobrador: ADMIN_COLLECTOR_ID,
            userId: myUserId,
          },
          currency: "ARS",
          amount: finalAmount,
          method: finalMethod,
          channel,
          intendedPeriod: intendedPeriod || null,
          notes: notes || "",
          idempotencyKey: finalIdem,
          externalRef: externalRef || null,
          cashSessionId: cashSessionId || null,
          geo: geo || undefined,
          device: device || undefined,
          ip: ip || undefined,
          createdBy: myUserId,
          allocations,
          meta: { periodsApplied },
        },
      ],
      { session }
    );

    const p = payDocs[0];
    p.markPosted();
    if (collectedAt) p.postedAt = new Date(collectedAt);
    await p.save({ session });

    const postedAt = p.postedAt || new Date();

    // 8) Ledger (doble partida) — CAJA_ADMIN / CAJA_SUPERADMIN vs INGRESOS_CUOTAS
    const cashAccountCode =
      userRole === "superAdmin"
        ? ACCOUNTS.CAJA_SUPERADMIN
        : ACCOUNTS.CAJA_ADMIN;

    if (!cashAccountCode) {
      throw new Error(
        "Configuración de cuentas contables inválida: falta CAJA_ADMIN/CAJA_SUPERADMIN en ACCOUNTS."
      );
    }
    if (!ACCOUNTS.INGRESOS_CUOTAS) {
      throw new Error(
        "Configuración de cuentas contables inválida: falta INGRESOS_CUOTAS en ACCOUNTS."
      );
    }

    await LedgerEntry.insertMany(
      [
        {
          paymentId: p._id,
          userId: myUserId,
          side: "debit",
          accountCode: cashAccountCode,
          amount: Math.abs(p.amount),
          currency: p.currency,
          postedAt,
          dimensions: {
            idCobrador: ADMIN_COLLECTOR_ID,
            idCliente: p.cliente.idCliente,
            canal: p.channel,
          },
        },
        {
          paymentId: p._id,
          userId: myUserId,
          side: "credit",
          accountCode: ACCOUNTS.INGRESOS_CUOTAS,
          amount: Math.abs(p.amount),
          currency: p.currency,
          postedAt,
          dimensions: {
            idCobrador: ADMIN_COLLECTOR_ID,
            idCliente: p.cliente.idCliente,
            canal: p.channel,
          },
        },
      ],
      { session }
    );

    // 9) Recibo
    let receipt;
    try {
      const { pdfPath, pdfUrl, receiptNumber, qrData, signature } =
        await generateReceipt(
          p.toObject(),
          {
            _id: member._id,
            idCliente: member.idCliente,
            nombre: member.nombre,
            nombreTitular: member.nombreTitular || null,
          },
          { at: postedAt }
        );

      const rxDocs = await Receipt.create(
        [
          {
            paymentId: p._id,
            number: receiptNumber,
            qrData,
            pdfPath,
            pdfUrl,
            signature,
            voided: false,
          },
        ],
        { session }
      );
      receipt = rxDocs[0];
    } catch {
      const rxDocs = await Receipt.create(
        [
          {
            paymentId: p._id,
            number: null,
            qrData: { error: "pdf_generation_failed" },
            pdfPath: null,
            pdfUrl: null,
            voided: false,
          },
        ],
        { session }
      );
      receipt = rxDocs[0];
    }

    // 10) Outbox
    await enqueue(
      "payment.posted",
      {
        paymentId: p._id.toString(),
        idCliente: p.cliente.idCliente,
        memberId: p.cliente.memberId.toString(),
        amount: p.amount,
        currency: p.currency,
        method: p.method,
        channel: p.channel,
        postedAt: (p.postedAt || new Date()).toISOString(),
        idCobrador: p.collector.idCobrador,
        userId: String(p.collector.userId),
        externalRef: p.externalRef || null,
        periodsApplied: p.meta?.periodsApplied || [],
      },
      { session }
    );

    await session.commitTransaction();
    return res.status(201).json({
      ok: true,
      data: serializePayment(p.toObject(), receipt.toObject()),
    });
  } catch (err) {
    try {
      await session.abortTransaction();
    } catch {}

    // Idempotencia por índice único
    if (err?.code === 11000 && err?.keyPattern?.idempotencyKey) {
      try {
        const dup = await Payment.findOne({
          idempotencyKey: req.body?.idempotencyKey,
        }).lean();
        const rx = dup
          ? await Receipt.findOne({ paymentId: dup._id }).lean()
          : null;
        if (dup) {
          return res
            .status(200)
            .json({ ok: true, data: serializePayment(dup, rx) });
        }
      } catch {}
    }

    return next(err);
  } finally {
    session.endSession();
  }
}

/**
 * Listado de pagos de oficina (admin / superAdmin)
 *
 * GET /admin/pagos
 *
 * Similar a listCollectorPayments, pero:
 *  - No limita por "collector.idCobrador" del usuario.
 *  - Devuelve sólo pagos de canal "backoffice" (caja de oficina) y kind "payment".
 *  - Permite filtrar opcionalmente por clientId, collectorId, fechas, etc.
 */
export async function listAdminPayments(req, res, next) {
  try {
    const myUserId = req.user?._id || req.user?.id;
    if (!myUserId) {
      return res
        .status(400)
        .json({ ok: false, message: "Sesión inválida: falta userId." });
    }

    const myUser = await User.findById(myUserId).select("role").lean();
    if (!myUser || !["admin", "superAdmin"].includes(myUser.role)) {
      return res.status(403).json({
        ok: false,
        message: "No tiene permiso para ver pagos de oficina.",
      });
    }

    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.min(toInt(req.query.limit, 25), 100);
    const qRaw = String(req.query.q || "").trim();
    const dateFrom = String(req.query.dateFrom || "");
    const dateTo = String(req.query.dateTo || "");
    const clientId = String(req.query.clientId || "").trim();
    const method = String(req.query.method || "").trim();
    const status = String(req.query.status || "").trim();
    const sortByParam = (req.query.sortBy || "postedAt").toString();
    const sortDirParam = toDir(req.query.sortDir || "desc");
    const collectorIdParam = String(req.query.collectorId || "").trim();

    // Base: sólo pagos de tipo "payment" y canal "backoffice"
    const and = [{ kind: "payment" }, { channel: "backoffice" }];

    // Opcional: filtrar por cobrador (idCobrador) si se manda collectorId
    if (collectorIdParam) {
      const n = Number(collectorIdParam);
      if (Number.isFinite(n)) {
        and.push({ "collector.idCobrador": n });
      }
    }

    // Filtrar por cliente (memberId)
    if (clientId && isObjectId(clientId)) {
      and.push({ "cliente.memberId": new Types.ObjectId(clientId) });
    }

    if (method) and.push({ method });
    if (status) and.push({ status });

    // Rango por postedAt (fallback createdAt)
    const parseISO = (s) => {
      const dt = new Date(`${s}T00:00:00`);
      return Number.isNaN(dt.getTime()) ? null : dt;
    };
    const fromDt = dateFrom ? parseISO(dateFrom) : null;
    const toDt = dateTo ? parseISO(dateTo) : null;

    if (fromDt || toDt) {
      and.push({
        $expr: {
          $and: [
            fromDt
              ? { $gte: [{ $ifNull: ["$postedAt", "$createdAt"] }, fromDt] }
              : { $eq: [1, 1] },
            toDt
              ? {
                  $lte: [
                    { $ifNull: ["$postedAt", "$createdAt"] },
                    new Date(new Date(toDt).setHours(23, 59, 59, 999)),
                  ],
                }
              : { $eq: [1, 1] },
          ],
        },
      });
    }

    // Búsqueda simple (nombre, idCliente, externalRef, número de recibo)
    if (qRaw) {
      const isNum = /^\d+$/.test(qRaw);

      const receipts = await Receipt.find(
        isNum
          ? { number: qRaw }
          : {
              number: new RegExp(
                qRaw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
                "i"
              ),
            }
      )
        .select("paymentId")
        .limit(200)
        .lean();

      const rxIds = (receipts || []).map((r) => r.paymentId);

      const or = [
        { "cliente.nombre": { $regex: qRaw, $options: "i" } },
        { externalRef: { $regex: qRaw, $options: "i" } },
      ];
      if (isNum) or.push({ "cliente.idCliente": Number(qRaw) });
      if (rxIds.length) or.push({ _id: { $in: rxIds } });

      and.push({ $or: or });
    }

    const match = and.length ? { $and: and } : {};

    const SORTABLE = new Set([
      "postedAt",
      "createdAt",
      "amount",
      "cliente.idCliente",
      "method",
      "status",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "postedAt";

    const sortStage =
      sortBy === "postedAt"
        ? { $sort: { postedAt: sortDirParam, _id: sortDirParam } }
        : { $sort: { [sortBy]: sortDirParam, _id: sortDirParam } };

    const project = {
      _id: 1,
      kind: 1,
      status: 1,
      postedAt: 1,
      createdAt: 1,
      settledAt: 1,
      amount: 1,
      currency: 1,
      method: 1,
      channel: 1,
      notes: 1,
      idempotencyKey: 1,
      externalRef: 1,
      cashSessionId: 1,
      cliente: 1,
      collector: 1,
      "meta.periodsApplied": 1,
    };

    const [items, count] = await Promise.all([
      Payment.aggregate([
        { $match: match },
        sortStage,
        { $skip: (page - 1) * limit },
        { $limit: limit },
        { $project: project },
      ]).allowDiskUse(true),
      Payment.countDocuments(match),
    ]);

    const ids = items.map((p) => p._id);
    const receipts = await Receipt.find({ paymentId: { $in: ids } })
      .select("paymentId number pdfUrl voided")
      .lean();
    const rxByPay = new Map(receipts.map((r) => [String(r.paymentId), r]));

    const out = items.map((p) =>
      serializePayment(p, rxByPay.get(String(p._id)) || null)
    );

    return res.json({
      ok: true,
      okList: true,
      items: out,
      total: count,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
    });
  } catch (err) {
    return next(err);
  }
}
