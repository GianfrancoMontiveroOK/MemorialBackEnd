// src/controllers/admin-receipts.controller.js
import mongoose from "mongoose";
import Receipt from "../models/receipt.model.js";
import Payment from "../models/payment.model.js";

const toInt = (v, def = 0) => {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
};
const toDir = (v) => (String(v || "").toLowerCase() === "asc" ? 1 : -1);

export async function listAdminReceipts(req, res, next) {
  try {
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limitRaw = Math.min(
      toInt(req.query.limit || req.query.pageSize, 25),
      100
    );
    const limit = Math.max(limitRaw, 1); // evita 0

    const qRaw = String(req.query.q || "").trim();
    const dateFrom = String(req.query.dateFrom || "");
    const dateTo = String(req.query.dateTo || "");
    const method = String(req.query.method || ""); // ej: "efectivo"
    const status = String(req.query.status || ""); // ej: "posted"
    const onlyWithPdf = String(req.query.onlyWithPdf || "true") === "true";

    const sortByParam = (req.query.sortBy || "postedAt").toString();
    const sortDirParam = toDir(req.query.sortDir || "desc");

    // Receipt base filter
    const matchReceipt = {};
    if (onlyWithPdf) matchReceipt.pdfUrl = { $nin: [null, "", false] };

    // Buscador por número de recibo
    let receiptOr = [];
    if (qRaw) {
      const isNum = /^\d+$/.test(qRaw);
      receiptOr = [
        isNum ? { number: qRaw } : { number: { $regex: qRaw, $options: "i" } },
      ];
    }

    // Rango de fechas sobre postedAt (fallback createdAt)
    const parseISO = (s) => {
      const dt = new Date(`${s}T00:00:00`);
      return Number.isNaN(dt.getTime()) ? null : dt;
    };
    const fromDt = dateFrom ? parseISO(dateFrom) : null;
    const toDt = dateTo ? parseISO(dateTo) : null;

    // Pipeline principal
    const pipeline = [
      { $match: matchReceipt },
      {
        $lookup: {
          from: Payment.collection.name,
          localField: "paymentId",
          foreignField: "_id",
          as: "payment",
        },
      },
      { $unwind: "$payment" },
    ];

    // Filtros sobre Payment
    const and = [];
    if (method) and.push({ "payment.method": method });
    if (status) and.push({ "payment.status": status });

    if (fromDt || toDt) {
      and.push({
        $expr: {
          $and: [
            fromDt
              ? {
                  $gte: [
                    { $ifNull: ["$payment.postedAt", "$payment.createdAt"] },
                    fromDt,
                  ],
                }
              : { $eq: [1, 1] },
            toDt
              ? {
                  $lte: [
                    { $ifNull: ["$payment.postedAt", "$payment.createdAt"] },
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
      const or = [
        ...receiptOr,
        { "payment.cliente.nombre": { $regex: qRaw, $options: "i" } },
        { "payment.externalRef": { $regex: qRaw, $options: "i" } },
      ];
      if (isNum) or.push({ "payment.cliente.idCliente": Number(qRaw) });
      and.push({ $or: or });
    }

    if (and.length) pipeline.push({ $match: { $and: and } });

    // Proyección
    pipeline.push({
      $project: {
        _id: 1,
        paymentId: 1,
        number: 1,
        pdfUrl: 1,
        qrData: 1,
        voided: 1,
        signature: 1,
        payment: {
          _id: "$payment._id",
          amount: "$payment.amount",
          currency: "$payment.currency",
          method: "$payment.method",
          status: "$payment.status",
          postedAt: "$payment.postedAt",
          createdAt: "$payment.createdAt",
          idempotencyKey: "$payment.idempotencyKey",
          externalRef: "$payment.externalRef",
          channel: "$payment.channel",
          collector: "$payment.collector",
          cliente: "$payment.cliente",
        },
      },
    });

    // Orden
    const SORTABLE = new Set([
      "postedAt",
      "createdAt",
      "number",
      "payment.amount",
      "payment.method",
      "payment.status",
      "payment.cliente.idCliente",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "postedAt";
    const sortStage =
      sortBy === "postedAt"
        ? { $sort: { "payment.postedAt": sortDirParam, _id: sortDirParam } }
        : { $sort: { [sortBy]: sortDirParam, _id: sortDirParam } };

    // Paginación
    const dataPipeline = [
      ...pipeline,
      sortStage,
      { $skip: (page - 1) * limit },
      { $limit: limit },
    ];

    // Conteo sin sort/skip/limit (más simple y preciso)
    const countPipeline = [...pipeline, { $count: "n" }];

    const [items, countRes] = await Promise.all([
      Receipt.aggregate(dataPipeline).allowDiskUse(true),
      Receipt.aggregate(countPipeline).allowDiskUse(true),
    ]);

    const total = countRes?.[0]?.n || 0;

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
