// src/controllers/collector.receipts.controller.js
import mongoose from "mongoose";
import Receipt from "../models/receipt.model.js";
import Payment from "../models/payment.model.js";

const { Types } = mongoose;

// -------- utils ----------
const toObjectId = (v) =>
  v && Types.ObjectId.isValid(v) ? new Types.ObjectId(String(v)) : null;

const toInt = (v, d = 0) => {
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : d;
};

const parseISODate = (s) => {
  if (!s) return null;
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
};

/**
 * Pipeline flexible para listar recibos:
 * - Filtros en Receipt (number, voided, rango por createdAt)
 * - $lookup a Payment
 * - Filtros tolerantes:
 *    * collector.idCobrador (Number)
 *    * cliente.memberId (ObjectId) y/o cliente.idCliente (Number)
 * - Orden, paginado y normalización
 */
function buildPipeline({
  q,
  includeVoided,
  dateFrom,
  dateTo,
  sortBy,
  sortDir,
  page,
  limit,
  collectorIdNum, // Number o null
  clientMemberId, // ObjectId o null
  idClienteNum, // Number o null
  enforceCollector = true,
  enforceClient = true,
}) {
  // --- Receipt filters ---
  const receiptMatch = {};
  if (q) {
    receiptMatch.number = { $regex: q, $options: "i" };
  }
  if (includeVoided !== "1") {
    receiptMatch.voided = { $ne: true };
  }
  if (dateFrom || dateTo) {
    receiptMatch.createdAt = {};
    if (dateFrom) receiptMatch.createdAt.$gte = dateFrom;
    if (dateTo) {
      const end = new Date(dateTo);
      end.setUTCHours(23, 59, 59, 999);
      receiptMatch.createdAt.$lte = end;
    }
  }

  const pipeline = [
    { $match: receiptMatch },
    {
      $lookup: {
        from: Payment.collection.name,
        localField: "paymentId",
        foreignField: "_id",
        as: "pay",
      },
    },
    { $unwind: "$pay" },
  ];

  // --- Payment filters ---
  const exprAND = [];

  // collector.idCobrador (Number) si se exige
  if (enforceCollector && Number.isFinite(collectorIdNum)) {
    exprAND.push({ $eq: ["$pay.collector.idCobrador", collectorIdNum] });
  }

  // cliente.memberId (ObjectId) si se exige
  if (enforceClient && clientMemberId) {
    // Comparamos como string para tolerar tipos
    exprAND.push({
      $eq: [{ $toString: "$pay.cliente.memberId" }, String(clientMemberId)],
    });
  }

  // cliente.idCliente (Number) opcional
  if (Number.isFinite(idClienteNum)) {
    exprAND.push({ $eq: ["$pay.cliente.idCliente", idClienteNum] });
  }

  // Búsqueda “soft” por externalRef (no bloquea si está vacío)
  if (q) {
    pipeline.push({
      $addFields: {
        __extRef: { $ifNull: ["$pay.externalRef", ""] },
      },
    });
    // Nota: no se agrega a AND; si querés forzar, podés pushear un $regexMatch en exprAND
  }

  if (exprAND.length) {
    pipeline.push({ $match: { $expr: { $and: exprAND } } });
  }

  pipeline.push(
    {
      $project: {
        _id: 1,
        number: 1,
        createdAt: 1,
        pdfUrl: 1,
        voided: 1,

        amount: { $ifNull: ["$pay.amount", 0] },
        method: "$pay.method",
        postedAt: "$pay.postedAt",

        // identidades normalizadas desde Payment
        clientId: "$pay.cliente.memberId",
        idCliente: "$pay.cliente.idCliente",
        collectorId: "$pay.collector.idCobrador",

        // por si necesitás auditar
        externalRef: { $ifNull: ["$pay.externalRef", ""] },
      },
    },
    { $sort: { [sortBy]: sortDir, _id: -1 } },
    {
      $facet: {
        items: [{ $skip: (page - 1) * limit }, { $limit: limit }],
        totalRows: [{ $count: "count" }],
      },
    },
    {
      $project: {
        items: 1,
        total: { $ifNull: [{ $arrayElemAt: ["$totalRows.count", 0] }, 0] },
      },
    }
  );

  return pipeline;
}

export async function listCollectorReceipts(req, res) {
  try {
    // middlewares deberían setear req.user.idCobrador (Number)
    const userCollectorId = req.user?.idCobrador;
    const collectorIdNum = Number.isFinite(Number(userCollectorId))
      ? Number(userCollectorId)
      : null;

    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.min(Math.max(toInt(req.query.limit, 25), 1), 200);
    const q = String(req.query.q || "").trim();
    const sortBy = req.query.sortBy || "createdAt";
    const sortDir = req.query.sortDir === "asc" ? 1 : -1;

    const clientMemberId = toObjectId((req.query.clientId || "").trim());
    const idClienteNum =
      req.query.idCliente != null ? Number(req.query.idCliente) : null;

    const dateFrom = parseISODate((req.query.dateFrom || "").trim());
    const dateTo = parseISODate((req.query.dateTo || "").trim());

    // (1) collector + client
    let pipeline = buildPipeline({
      q,
      includeVoided: req.query.includeVoided,
      dateFrom,
      dateTo,
      sortBy,
      sortDir,
      page,
      limit,
      collectorIdNum,
      clientMemberId,
      idClienteNum,
      enforceCollector: true,
      enforceClient: !!clientMemberId,
    });

    let agg = await Receipt.aggregate(pipeline);
    let { items = [], total = 0 } = agg?.[0] || {};

    // (2) fallback: solo client (relajamos collector) si vino clientId
    if ((!items || items.length === 0) && clientMemberId) {
      pipeline = buildPipeline({
        q,
        includeVoided: req.query.includeVoided,
        dateFrom,
        dateTo,
        sortBy,
        sortDir,
        page,
        limit,
        collectorIdNum,
        clientMemberId,
        idClienteNum,
        enforceCollector: false,
        enforceClient: true,
      });
      agg = await Receipt.aggregate(pipeline);
      items = agg?.[0]?.items || [];
      total = agg?.[0]?.total || 0;
      if (items.length > 0) {
        console.log("[Receipts] fallback sin collectorId →", items.length);
      }
    }

    // (3) diagnóstico: sin filtros (para logs)
    if (!items || items.length === 0) {
      const diag = buildPipeline({
        q,
        includeVoided: req.query.includeVoided,
        dateFrom,
        dateTo,
        sortBy,
        sortDir,
        page,
        limit,
        collectorIdNum: null,
        clientMemberId: null,
        idClienteNum: null,
        enforceCollector: false,
        enforceClient: false,
      });
      const d = await Receipt.aggregate(diag);
      const dItems = d?.[0]?.items || [];
      const dTotal = d?.[0]?.total || 0;
      console.log(
        `[Receipts] diag sin filtros → items=${dItems.length}, total=${dTotal}`
      );
    }

    // normalización para el front
    const norm = (items || []).map((r) => {
      const year =
        (r.createdAt && new Date(r.createdAt).getFullYear()) ||
        (r.postedAt && new Date(r.postedAt).getFullYear()) ||
        new Date().getFullYear();
      const pdf = r.pdfUrl || `/files/receipts/${year}/${r._id}.pdf`;

      return {
        _id: r._id,
        number: r.number,
        amount: Number(r.amount || 0),
        createdAt: r.createdAt || r.postedAt || null,
        method: r.method || null,
        clientId: r.clientId || null, // ObjectId del miembro
        idCliente: r.idCliente ?? null, // número de grupo
        collectorId: r.collectorId ?? null, // idCobrador (Number)
        voided: !!r.voided,
        pdfUrl: pdf,
      };
    });

    return res.json({
      items: norm,
      total,
      page,
      limit,
      sortBy,
      sortDir: sortDir === 1 ? "asc" : "desc",
    });
  } catch (err) {
    console.error("listCollectorReceipts error:", err);
    return res
      .status(500)
      .json({ message: "No se pudieron listar los recibos" });
  }
}

export async function streamCollectorReceiptPdf(req, res) {
  try {
    const { id } = req.params;
    if (!Types.ObjectId.isValid(id)) {
      return res.status(400).json({ message: "ID inválido" });
    }
    const rx = await Receipt.findById(id).lean();
    if (!rx) return res.status(404).json({ message: "Recibo no encontrado" });

    if (rx.pdfUrl) return res.redirect(rx.pdfUrl);

    const year =
      (rx.createdAt && new Date(rx.createdAt).getFullYear()) ||
      new Date().getFullYear();
    return res.redirect(`/files/receipts/${year}/${rx._id}.pdf`);
  } catch (err) {
    console.error("streamCollectorReceiptPdf error:", err);
    return res.status(500).json({ message: "No se pudo abrir el PDF" });
  }
}
