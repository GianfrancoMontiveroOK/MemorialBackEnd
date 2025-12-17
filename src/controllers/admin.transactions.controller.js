// src/controllers/admin.transactions.controller.js
import mongoose from "mongoose";
import fs from "fs";

import Payment from "../models/payment.model.js";
import Receipt from "../models/receipt.model.js";
import Cliente from "../models/client.model.js";
import LedgerEntry from "../models/ledger-entry.model.js";
import { ACCOUNTS } from "../config/accounts.js";
import { generateReceipt } from "../services/receipt.service.js";
import { enqueue } from "../services/outbox.service.js";
import { getClientPeriodState } from "../services/debt.service.js"; // ⬅️ solo este

const toDir = (v) => (String(v || "").toLowerCase() === "asc" ? 1 : -1);

// Comparación simple de períodos "YYYY-MM"
const comparePeriod = (a, b) => {
  if (!a && !b) return 0;
  if (!a) return -1;
  if (!b) return 1;
  if (a === b) return 0;
  // Como usamos formato "YYYY-MM", la comparación lexicográfica funciona
  return a < b ? -1 : 1;
};

/* =================== Helpers de periodo/alloc locales =================== */

// YYYY-MM (hora AR no nos importa para ordenar)
function yyyymmAR(date = new Date()) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

/**
 * FIFO simple usando el resultado de getClientPeriodState:
 * - Usa debtState.periods (array de { period, balance, status, ... })
 * - Recorre desde el período más viejo hasta nowPeriod
 * - Imputa hasta donde alcance "amount" contra balance>0
 *
 * Devuelve: { allocations: [{ period, amount }], remaining }
 */
function fifoAllocateUntilNow(debtState, nowPeriod, amount) {
  const allocations = [];
  let remaining = Number(amount) || 0;
  if (!debtState || !Array.isArray(debtState.periods) || !(remaining > 0)) {
    return { allocations, remaining };
  }

  const sorted = [...debtState.periods]
    .filter((p) => p.period && p.period <= nowPeriod)
    .sort((a, b) => (a.period < b.period ? -1 : a.period > b.period ? 1 : 0));

  for (const row of sorted) {
    if (remaining <= 0) break;
    const bal = Math.max(0, Number(row.balance || 0));
    if (bal <= 0) continue;

    const take = Math.min(bal, remaining);
    if (take <= 0) continue;

    allocations.push({ period: row.period, amount: take });
    remaining -= take;
  }

  return { allocations, remaining };
}

/* ===================== Listado normal de transacciones ===================== */

/**
 * GET /api/adminTransactions/transactions
 * Admin/SuperAdmin: lista TODOS los pagos con filtros.
 * Query:
 *  - page, limit   (1-based)
 *  - q             (cliente.nombre | cliente.idCliente | receipt.number | externalRef)
 *  - dateFrom/to   (YYYY-MM-DD) filtra por postedAt (o createdAt si no está posteado)
 *  - clientId      (_id del miembro)
 *  - method        (efectivo|transferencia|tarjeta|qr|otro|debito_automatico)
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
      meta: 1,
    };

    const listPipeline = [
      { $match: match },
      sortStage,
      { $skip: (page - 1) * limit },
      { $limit: limit },
      { $project: project },
    ];

    const listAgg = Payment.aggregate(listPipeline);
    listAgg.allowDiskUse(true);

    const [items, count] = await Promise.all([
      listAgg,
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

/* ========================= Helpers de archivos ========================= */

async function readUploadedTextFile(file) {
  if (!file) return "";
  if (file.buffer) {
    // multer memoryStorage
    return file.buffer.toString("latin1");
  }
  if (file.path) {
    return fs.promises.readFile(file.path, "latin1");
  }
  return "";
}

/**
 * Layout Naranja 115 caracteres:
 *  1          : tipo ("D")
 *  2–17       : nroTarjeta (16)
 *  18–29      : importe (12, 10 enteros + 2 decimales, sin separador)
 *  30–37      : fechaAlta AAAAMMDD
 *  38–67      : nroDebito (30) → cuenta del cliente en la empresa
 *  68–75      : fechaVto AAAAMMDD
 *  76–77      : nroCuota (2)
 *  78–85      : nroFactura (8)
 *  86–89      : anioCuota (4)
 *  90–112     : datosAdicionales (23)
 *  113–115    : resultado (en DAR) / espacios en DAF
 */
function parseNaranjaDetailLine(line) {
  if (!line || line[0] !== "D" || line.length < 115) return null;

  const tipo = line[0];
  const tarjeta = line.slice(1, 17);
  const importeRaw = line.slice(17, 29);
  const fechaAlta = line.slice(29, 37);
  const nroDebitoRaw = line.slice(37, 67);
  const fechaVto = line.slice(67, 75);
  const nroCuota = line.slice(75, 77);
  const nroFactura = line.slice(77, 85);
  const anioCuota = line.slice(85, 89);
  const datosAdic = line.slice(89, 112);
  const resultCode = line.slice(112, 115); // DAR: código; DAF: espacios

  const importeCentavos = Number(importeRaw);
  const importe = isNaN(importeCentavos) ? 0 : importeCentavos / 100;

  const nroDebito = nroDebitoRaw.trim();
  const legacyIdCliente = nroDebito ? Number(nroDebito) : NaN;

  const toDate = (yyyymmdd) => {
    if (!/^\d{8}$/.test(yyyymmdd)) return null;
    const y = Number(yyyymmdd.slice(0, 4));
    const m = Number(yyyymmdd.slice(4, 6)) - 1;
    const d = Number(yyyymmdd.slice(6, 8));
    const dt = new Date(Date.UTC(y, m, d, 3, 0, 0)); // AR aprox
    return isNaN(dt.getTime()) ? null : dt;
  };

  return {
    tipo,
    tarjeta,
    importeRaw,
    importeCentavos,
    importe,
    fechaAlta,
    fechaAltaDate: toDate(fechaAlta),
    nroDebitoRaw,
    nroDebito,
    legacyIdCliente: Number.isFinite(legacyIdCliente) ? legacyIdCliente : null,
    fechaVto,
    fechaVtoDate: toDate(fechaVto),
    nroCuota,
    nroFactura,
    anioCuota,
    datosAdic,
    resultCode: resultCode.trim() || null, // en DAF queda null
    rawLine: line,
  };
}

/* ================= Helpers Naranja: aprobación / intentos ================= */

// Según tabla de códigos (DAR): "000" = operación aprobada
const NARANJA_APPROVED_CODES = new Set(["000"]);

/**
 * En DAF el campo viene en blanco ⇒ se considera APROBADO.
 * En DAR viene código ⇒ usamos la tabla de resultado.
 */
function isNaranjaApproved(resultCode) {
  const code = String(resultCode || "").trim();
  if (!code) {
    // DAF: "espacios en DAF" → se asume aprobado
    return true;
  }
  return NARANJA_APPROVED_CODES.has(code);
}

/**
 * PAGO de débito automático APROBADO:
 * - Crea Payment POSTED.
 * - Aplica allocations FIFO.
 * - Crea ledger, recibo y mensaje en outbox.
 */
async function registerAutoDebitPayment({
  session,
  member, // doc Cliente
  amount, // número ARS
  collectorId, // 14 = Naranja, 6 = Nación
  userId, // req.user._id (admin que importa)
  source, // "naranja" | "bna"
  externalRef,
  collectedAt,
  assetAccountCode, // cuenta de activo (BANCO_NACION / TARJETA_NARANJA / CAJA_COBRADOR)
  resultCode,
}) {
  const nowPeriod = yyyymmAR(new Date());

  // 1) Estado de deuda hasta período actual
  let debtState = await getClientPeriodState(member, {
    to: nowPeriod,
    includeFuture: 0,
  });

  const totalDueUpToNow = (debtState?.periods || [])
    .filter((p) => comparePeriod(p.period, nowPeriod) <= 0)
    .reduce((acc, p) => acc + Math.max(0, Number(p.balance || 0)), 0);

  const finalAmount = Number(amount);
  if (!(finalAmount > 0)) {
    throw new Error("Monto de débito automático inválido (<= 0)");
  }

  if (!userId) {
    throw new Error("userId requerido para registrar pago automático.");
  }

  const finalMethod = "debito_automatico";
  const channel =
    source === "naranja"
      ? "debito_automatico_naranja"
      : "debito_automatico_bna";

  // 2) Idempotencia: cliente + origen + monto + fecha
  const finalIdem =
    `auto_${source}_${member._id}_${finalAmount}_` +
    (collectedAt ? collectedAt.toISOString().slice(0, 10) : "nodate");

  const existing = await Payment.findOne({ idempotencyKey: finalIdem })
    .session(session)
    .lean();
  if (existing) {
    const rx = await Receipt.findOne({ paymentId: existing._id })
      .session(session)
      .lean();
    return { reused: true, payment: existing, receipt: rx };
  }

  // 3) Imputaciones FIFO
  let allocations = [];
  let periodsApplied = [];

  const balMap = new Map(
    (debtState?.periods || []).map((p) => [
      p.period,
      Math.max(0, Number(p.balance || 0)),
    ])
  );

  const { allocations: fifo } = fifoAllocateUntilNow(
    debtState,
    nowPeriod,
    finalAmount
  );
  const totalAllocated = fifo.reduce((acc, a) => acc + a.amount, 0);

  if (totalAllocated > 0) {
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
  } else {
    allocations = [];
    periodsApplied = [];
  }

  // 4) Re-chequeo anti-carrera
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
      throw new Error(
        `El período ${a.period} cambió y ya no admite ${a.amount} (saldo: ${bal}).`
      );
    } else {
      freshBal.set(a.period, Math.max(0, bal - a.amount));
    }
  }

  // 5) Payment: lo creamos y lo dejamos realmente POSTED
  const postedAt = collectedAt || new Date();

  const payDocs = await Payment.create(
    [
      {
        kind: "payment",
        // status lo vamos a setear con markPosted o a mano:
        cliente: {
          memberId: member._id,
          idCliente: member.idCliente,
          nombre: member.nombre,
          nombreTitular: member.nombreTitular || null,
        },
        collector: {
          idCobrador: collectorId,
          userId: userId || null,
        },
        createdBy: new mongoose.Types.ObjectId(String(userId)),
        currency: "ARS",
        amount: finalAmount,
        method: finalMethod,
        channel,
        intendedPeriod: null,
        notes: `[${source}] Débito automático importado`,
        idempotencyKey: finalIdem,
        externalRef: externalRef || null,
        cashSessionId: null,
        allocations,
        meta: {
          periodsApplied,
          source,
          imported: true,
          autoDebitAttempt: false,
          autoDebitApproved: true,
          gatewayResultCode: resultCode || null,
        },
      },
    ],
    { session }
  );

  const p = payDocs[0];

  // 🔴 ACÁ FORZAMOS QUE QUEDE POSTED SIEMPRE:
  if (typeof p.markPosted === "function") {
    p.markPosted({ at: postedAt, by: userId || null });
  } else {
    p.status = "posted";
    p.postedAt = postedAt;
  }

  await p.save({ session });

  // 6) Ledger ✅ (BANCO_NACION / TARJETA_NARANJA = cajas físicas como CAJA_CHICA)
  const debitAccount =
    assetAccountCode ||
    (source === "naranja"
      ? ACCOUNTS.TARJETA_NARANJA
      : source === "bna"
      ? ACCOUNTS.BANCO_NACION
      : ACCOUNTS.CAJA_COBRADOR);

  const creditAccount = ACCOUNTS.INGRESOS_CUOTAS;

  const amtAbs = Math.abs(Number(p.amount || 0));
  const execOid = userId ? new mongoose.Types.ObjectId(String(userId)) : null;

  // ✅ Dirección única (misma en ambas partidas):
  // CREDIT (fromAccount) = INGRESOS_CUOTAS
  // DEBIT  (toAccount)   = BANCO_NACION / TARJETA_NARANJA
  const fromAccountCode = creditAccount;
  const toAccountCode = debitAccount;

  // ✅ Dueños (strings) = cajas/cuentas, NO personas
  const fromUserLabel = creditAccount; // "INGRESOS_CUOTAS"
  const toUserLabel = debitAccount; // "BANCO_NACION" | "TARJETA_NARANJA" | etc.

  const dimsCommon = {
    idCobrador: p.collector?.idCobrador ?? null,
    idCliente: p.cliente?.idCliente ?? null,
    canal: p.channel,
    source,
    imported: true,
    gatewayResultCode: resultCode || null,
    executedByUserId: execOid ? String(execOid) : null,
  };

  await LedgerEntry.insertMany(
    [
      // DEBIT: aumenta BANCO_NACION / TARJETA_NARANJA
      {
        paymentId: p._id,
        userId: execOid, // auditoría (no ownership)
        kind: "AUTO_DEBIT_POSTED",
        side: "debit",
        accountCode: debitAccount,
        amount: amtAbs,
        currency: p.currency,
        postedAt,

        fromUser: fromUserLabel, // "INGRESOS_CUOTAS"
        toUser: toUserLabel, // "BANCO_NACION" | "TARJETA_NARANJA"
        fromAccountCode,
        toAccountCode,

        dimensions: dimsCommon,
      },

      // CREDIT: reconoce ingreso (cuotas)
      {
        paymentId: p._id,
        userId: execOid,
        kind: "AUTO_DEBIT_POSTED",
        side: "credit",
        accountCode: creditAccount,
        amount: amtAbs,
        currency: p.currency,
        postedAt,

        // ✅ misma dirección (NO invertir)
        fromUser: fromUserLabel, // owner del credit via fromUser
        toUser: toUserLabel,
        fromAccountCode,
        toAccountCode,

        dimensions: dimsCommon,
      },
    ],
    { session }
  );

  // 7) Recibo
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

  // 8) Outbox
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
      postedAt: postedAt.toISOString(),
      idCobrador: p.collector.idCobrador,
      userId: String(p.collector.userId || ""),
      externalRef: p.externalRef || null,
      periodsApplied: p.meta?.periodsApplied || [],
    },
    { session }
  );

  return { reused: false, payment: p, receipt };
}

/**
 * INTENTO de débito automático NO APROBADO:
 * - Crea Payment en borrador.
 * - SIN allocations, SIN ledger, SIN recibo, SIN outbox.
 */
async function registerAutoDebitAttempt({
  session,
  member, // doc Cliente
  amount,
  collectorId,
  userId,
  source, // "naranja" | "bna"
  externalRef,
  collectedAt,
  resultCode,
}) {
  const finalAmount = Number(amount);
  if (!(finalAmount > 0)) {
    throw new Error("Monto de intento de débito automático inválido (<= 0)");
  }

  if (!userId) {
    throw new Error("userId requerido para registrar intento automático.");
  }

  const finalMethod = "debito_automatico";
  const channel =
    source === "naranja"
      ? "debito_automatico_naranja"
      : "debito_automatico_bna";

  const finalIdem =
    `auto_attempt_${source}_${member._id}_${finalAmount}_` +
    (collectedAt ? collectedAt.toISOString().slice(0, 10) : "nodate") +
    `_${resultCode || "noresult"}`;

  const existing = await Payment.findOne({ idempotencyKey: finalIdem })
    .session(session)
    .lean();
  if (existing) {
    return { reused: true, payment: existing };
  }

  const payDocs = await Payment.create(
    [
      {
        kind: "payment",
        status: "draft", // intento, queda en borrador
        cliente: {
          memberId: member._id,
          idCliente: member.idCliente,
          nombre: member.nombre,
          nombreTitular: member.nombreTitular || null,
        },
        collector: {
          idCobrador: collectorId,
          userId: userId || null,
        },
        createdBy: new mongoose.Types.ObjectId(String(userId)),
        currency: "ARS",
        amount: finalAmount,
        method: finalMethod,
        channel,
        allocations: [], // SIN imputación
        intendedPeriod: null,
        notes: `[${source}] Intento de débito automático NO APROBADO`,
        idempotencyKey: finalIdem,
        externalRef: externalRef || null,
        cashSessionId: null,
        meta: {
          source,
          imported: true,
          autoDebitAttempt: true,
          autoDebitApproved: false,
          gatewayResultCode: resultCode || null,
          attemptAt: collectedAt ? collectedAt.toISOString() : null,
        },
      },
    ],
    { session }
  );

  const p = payDocs[0];
  // NO markPosted, NO ledger, NO receipt, NO outbox.
  return { reused: false, payment: p };
}

/* ==================== Controlador de importación Naranja ==================== */

const COLLECTOR_ID_NARANJA = 14; // Tarjeta Naranja
const COLLECTOR_ID_BNA = 6; // Banco Nación

/**
 * POST /api/adminTransactions/import-naranja
 * body: multipart/form-data con campo "file"
 */
export async function importNaranjaResultFile(req, res, next) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const file = req.file;
    if (!file) {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "Falta archivo (campo file)." });
    }

    const raw = await readUploadedTextFile(file);
    const lines = String(raw)
      .split(/\r?\n/)
      .map((l) => l.replace(/\r$/, ""))
      .filter((l) => l.trim().length > 0);

    const detailLines = lines.filter((l) => l[0] === "D" && l.length >= 50);

    const userId = req.user?._id || req.user?.id || null;

    const summary = {
      totalLines: lines.length,
      detailLines: detailLines.length,
      processed: 0,
      reused: 0,
      attempts: 0,
      skippedNoCliente: 0,
      skippedParseError: 0,
      results: [],
    };

    for (const line of detailLines) {
      const parsed = parseNaranjaDetailLine(line);
      if (!parsed) {
        summary.skippedParseError++;
        summary.results.push({
          line,
          status: "skipped_parse_error",
        });
        continue;
      }

      const { legacyIdCliente, importe, fechaVtoDate, tarjeta, resultCode } =
        parsed;

      if (!legacyIdCliente) {
        summary.skippedNoCliente++;
        summary.results.push({
          ...parsed,
          status: "skipped_no_legacy_idCliente",
        });
        continue;
      }

      const member = await Cliente.findOne({
        idCliente: legacyIdCliente,
      }).session(session);

      if (!member) {
        summary.skippedNoCliente++;
        summary.results.push({
          ...parsed,
          status: "skipped_cliente_not_found",
        });
        continue;
      }

      const externalRef = `naranja:${tarjeta}:${legacyIdCliente}`;

      if (isNaranjaApproved(resultCode)) {
        // PAGO APROBADO (incluye DAF donde resultCode está vacío)
        const { reused, payment, receipt } = await registerAutoDebitPayment({
          session,
          member,
          amount: importe,
          collectorId: COLLECTOR_ID_NARANJA,
          userId,
          source: "naranja",
          externalRef,
          collectedAt: fechaVtoDate || null,
          assetAccountCode: ACCOUNTS.TARJETA_NARANJA,
          resultCode,
        });

        if (reused) summary.reused++;
        else summary.processed++;

        summary.results.push({
          ...parsed,
          status: reused ? "created_approved_reused" : "created_approved",
          approved: true,
          paymentId: payment._id,
          receiptId: receipt?._id || null,
        });
      } else {
        // INTENTO NO APROBADO (DAR con código ≠ "000")
        const { reused, payment } = await registerAutoDebitAttempt({
          session,
          member,
          amount: importe,
          collectorId: COLLECTOR_ID_NARANJA,
          userId,
          source: "naranja",
          externalRef,
          collectedAt: fechaVtoDate || null,
          resultCode,
        });

        if (reused) summary.reused++;
        else summary.attempts++;

        summary.results.push({
          ...parsed,
          status: reused ? "attempt_reused" : "attempt_created",
          approved: false,
          paymentId: payment._id,
          receiptId: null,
        });
      }
    }

    await session.commitTransaction();
    return res.status(200).json({
      ok: true,
      data: summary,
    });
  } catch (err) {
    try {
      await session.abortTransaction();
    } catch {}
    return next(err);
  } finally {
    session.endSession();
  }
}

/**
 * POST /api/adminTransactions/import-bna
 * Por ahora stub (no implementado aún).
 */
export async function importBancoNacionResultFile(req, res, next) {
  return res.status(501).json({
    ok: false,
    message:
      "Importación de Banco Nación todavía no implementada. Primero terminamos Naranja.",
  });
}
