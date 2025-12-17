// src/controllers/collector.payments.controller.js
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
  // 🆕 nuevas utils de períodos / atrasos
  countArrearsMonths,
} from "./payments.shared.js";

export async function createCollectorPayment(req, res, next) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const myCollectorId = Number(req.user?.idCobrador);

    // ✅ normalizamos SIEMPRE a ObjectId para refs
    const myUserIdRaw = req.user?._id || req.user?.id;
    const myUserOid = myUserIdRaw
      ? new mongoose.Types.ObjectId(String(myUserIdRaw))
      : null;

    if (!Number.isFinite(myCollectorId) || !myUserOid) {
      await session.abortTransaction();
      return res.status(400).json({
        ok: false,
        message: "Sesión inválida: falta idCobrador o userId.",
      });
    }

    const {
      clienteId,
      idCliente: legacyIdCliente,
      amount,
      method,
      notes,
      idempotencyKey,
      channel = "field",
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

    // 1) Validar cliente y scope
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

    if (Number(member.idCobrador) !== myCollectorId) {
      await session.abortTransaction();
      return res
        .status(403)
        .json({ ok: false, message: "El cliente no pertenece a tu cartera." });
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

    // 2) Estado de deuda hasta periodo actual
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

    // 🆕 2.1) Meses de atraso (solo lectura para reglas de negocio)
    const arrearsMonths = countArrearsMonths(debtState, nowPeriod);

    // 🆕 Regla de corte: 4+ meses de atraso → no se permite cobrar por cobrador
    if (arrearsMonths >= 4) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        code: "ARREARS_CUTOFF_4M",
        message:
          "El grupo familiar supera el límite de 4 meses de atraso. Contactar administración.",
        nowPeriod,
        arrearsMonths,
      });
    }

    // 3) Monto final
    const cuotaVigente =
      Number(member.usarCuotaIdeal ? member.cuotaIdeal : member.cuota) || 0;

    const hasExplicitAmount = Number(amount) > 0;
    const isAuto = String(strategy).toLowerCase() === "auto";

    let finalAmount;
    if (hasExplicitAmount) {
      finalAmount = Number(amount);
    } else if (isAuto) {
      finalAmount = totalDueUpToNow;
    } else {
      finalAmount = cuotaVigente;
    }

    if (!(finalAmount > 0)) {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "No hay monto válido para cobrar (> 0)." });
    }

    // 4) Método + idempotencia
    const finalMethod = (method || "efectivo").toString().trim().toLowerCase();
    const finalIdem =
      (idempotencyKey && String(idempotencyKey).trim()) ||
      `pay_${member._id}_${Date.now()}_${crypto
        .randomBytes(4)
        .toString("hex")}`;

    const existing = await Payment.findOne({ idempotencyKey: finalIdem })
      .session(session)
      .lean();

    if (existing) {
      const rx = await Receipt.findOne({ paymentId: existing._id })
        .session(session)
        .lean();

      // ✅ OJO: acá NO abortamos TX porque no hicimos writes todavía,
      // pero igual devolvemos respuesta directa.
      await session.abortTransaction();
      return res
        .status(200)
        .json({ ok: true, data: serializePayment(existing, rx) });
    }

    // 5) Construcción de imputaciones
    let allocations = [];
    let periodsApplied = [];

    const balMap = new Map(
      (debtState?.periods || []).map((p) => [
        p.period,
        Math.max(0, Number(p.balance || 0)),
      ])
    );

    if (String(strategy).toLowerCase() === "manual") {
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

      if (!hasExplicitAmount && sum > 0) {
        finalAmount = sum;
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

    // 6) Re-chequeo anti-carrera con estado fresco dentro de la misma TX
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

      if (a.amountApplied > bal + 0.0001) {
        await session.abortTransaction();
        return res.status(409).json({
          ok: false,
          code: "RACE_CONDITION_OVERPAY",
          message: `El período ${a.period} cambió y ya no admite ${a.amountApplied} (saldo: ${bal}). Refrescá y reintentá.`,
        });
      }

      freshBal.set(a.period, Math.max(0, bal - a.amountApplied));
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
          collector: { idCobrador: myCollectorId, userId: myUserOid },
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
          createdBy: myUserOid,
          allocations,
          meta: {
            periodsApplied,
            arrearsMonthsAtPayment: arrearsMonths,
          },
        },
      ],
      { session }
    );

    const p = payDocs[0];
    p.markPosted();
    if (collectedAt) p.postedAt = new Date(collectedAt);
    await p.save({ session });

    // 8) Ledger (doble partida por total) — ✅ SCHEMA NUEVO (sin performedBy/cobradorId)
    const postedAt = p.postedAt || new Date();

    // ✅ Nombre del cliente (string)
    const clientName =
      String(member?.nombreTitular || member?.nombre || "").trim() ||
      `Cliente #${member.idCliente}`;

    // ✅ Buscar al cobrador en DB por userId y resolver nombre real
    const collectorUser = await User.findById(myUserOid)
      .select("_id name email")
      .session(session)
      .lean();

    const collectorName =
      String(collectorUser?.name || collectorUser?.email || "").trim() ||
      `Cobrador #${myCollectorId}`;

    // ✅ dimensions (solo lo que conservás en schema)
    const ledgerDims = {
      idCobrador: myCollectorId,
      idCliente: Number(member.idCliente),
      canal: String(p.channel || "").trim() || null,
      plan: null,
      note: String(p.notes || "").trim(),
    };

    const amtAbs = Math.abs(Number(p.amount) || 0);

    const baseCommon = {
      paymentId: p._id,
      userId: myUserOid, // actor/dueño del asiento
      kind: "payment_collector",
      currency: p.currency,
      postedAt,
      dimensions: ledgerDims,
    };

    await LedgerEntry.insertMany(
      [
        // ✅ DÉBITO: del CLIENTE → al COBRADOR (entra a CAJA_COBRADOR)
        {
          ...baseCommon,
          side: "debit",
          accountCode: ACCOUNTS.CAJA_COBRADOR,
          amount: amtAbs,

          fromUser: clientName,
          toUser: collectorName,
          fromAccountCode: "CLIENTE",
          toAccountCode: ACCOUNTS.CAJA_COBRADOR,
        },

        // ✅ CRÉDITO: del COBRADOR → al CLIENTE (como querés que se vea en UI)
        // (y contablemente: de CAJA_COBRADOR → a INGRESOS_CUOTAS)
        {
          ...baseCommon,
          side: "credit",
          accountCode: ACCOUNTS.INGRESOS_CUOTAS,
          amount: amtAbs,

          fromUser: collectorName,
          toUser: clientName,
          fromAccountCode: ACCOUNTS.CAJA_COBRADOR,
          toAccountCode: ACCOUNTS.INGRESOS_CUOTAS,
        },
      ],
      { session, ordered: true }
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

    // 10) Outbox (igual)
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
        arrearsMonthsAtPayment: p.meta?.arrearsMonthsAtPayment ?? null,
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

/* ============ GET /collector/pagos ============ */
export async function listCollectorPayments(req, res, next) {
  try {
    const myCollectorId = Number(req.user?.idCobrador);
    if (!Number.isFinite(myCollectorId)) {
      return res
        .status(400)
        .json({ ok: false, message: "Falta idCobrador en la sesión." });
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

    // ✅ “nueva lógica” acá no toca ledger; mantenemos scope por idCobrador (Payment source of truth)
    const and = [{ "collector.idCobrador": myCollectorId }];

    if (clientId && isObjectId(clientId)) {
      and.push({ "cliente.memberId": new mongoose.Types.ObjectId(clientId) });
    }
    if (method) and.push({ method });
    if (status) and.push({ status });

    // rango por postedAt (fallback createdAt)
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
      items: out,
      total: count,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
    });
  } catch (err) {
    next(err);
  }
}
