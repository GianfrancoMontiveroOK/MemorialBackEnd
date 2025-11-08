// src/controllers/admin.transactions.controller.js
import mongoose from "mongoose";
import Payment from "../models/payment.model.js";
import Receipt from "../models/receipt.model.js";

const toDir = (v) => (String(v || "").toLowerCase() === "asc" ? 1 : -1);

/**
 * GET /api/adminTransactions/transactions
 * Admin/SuperAdmin: lista TODOS los pagos con filtros.
 * Query:
 *  - page, limit   (1-based)
 *  - q             (cliente.nombre | cliente.idCliente | receipt.number | externalRef)
 *  - dateFrom/to   (YYYY-MM-DD) filtra por postedAt (o createdAt si no está posteado)
 *  - clientId      (_id del miembro)
 *  - method        (efectivo|transferencia|tarjeta|qr|otro)
 *  - status        (draft|posted|settled|reversed)
 *  - sortBy        (postedAt|createdAt|amount|cliente.idCliente|method|status)
 *  - sortDir       (asc|desc)
 */
export async function listAllPayments(req, res, next) {
  try {
    const toInt = (v, d = 1) => {
      const n = parseInt(String(v), 10);
      return Number.isFinite(n) ? n : d;
    };

    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.min(toInt(req.query.limit, 25), 200);

    const qRaw = String(req.query.q || "").trim();
    const dateFrom = String(req.query.dateFrom || "");
    const dateTo = String(req.query.dateTo || "");
    const clientId = String(req.query.clientId || "").trim();
    const method = String(req.query.method || "").trim();
    const status = String(req.query.status || "").trim();

    const sortByParam = (req.query.sortBy || "postedAt").toString();
    const sortDirParam = toDir(req.query.sortDir || "desc");

    // ---- Filtros base ----
    const and = [];

    if (clientId && mongoose.Types.ObjectId.isValid(clientId)) {
      and.push({ "cliente.memberId": new mongoose.Types.ObjectId(clientId) });
    }
    if (method) and.push({ method });
    if (status) and.push({ status });

    // Rango de fechas por postedAt o (si no existe) createdAt
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

    // Búsqueda por q (nombre, idCliente, nro de recibo, externalRef)
    if (qRaw) {
      const isNum = /^\d+$/.test(qRaw);

      // Buscar recibos por número (lookup liviano fuera del aggregate)
      const receiptJoin = await Receipt.find(
        isNum
          ? { number: Number(qRaw) }
          : {
              number: new RegExp(
                qRaw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
                "i"
              ),
            }
      )
        .select("paymentId")
        .limit(500)
        .lean();

      const receiptPaymentIds = (receiptJoin || []).map((r) => r.paymentId);

      const or = [
        { "cliente.nombre": { $regex: qRaw, $options: "i" } },
        { externalRef: { $regex: qRaw, $options: "i" } },
      ];
      if (isNum) or.push({ "cliente.idCliente": Number(qRaw) });
      if (receiptPaymentIds.length)
        or.push({ _id: { $in: receiptPaymentIds } });

      and.push({ $or: or });
    }

    const match = and.length ? { $and: and } : {};

    // Orden
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

    // Adjuntar número de recibo rápido
    const ids = items.map((p) => p._id);
    const receipts = await Receipt.find({ paymentId: { $in: ids } })
      .select("paymentId number pdfUrl voided")
      .lean();
    const rxByPay = new Map(receipts.map((r) => [String(r.paymentId), r]));

    const out = items.map((p) => {
      const r = rxByPay.get(String(p._id));
      return {
        ...p,
        receipt: r
          ? { _id: r._id, number: r.number, pdfUrl: r.pdfUrl, voided: r.voided }
          : null,
      };
    });

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
