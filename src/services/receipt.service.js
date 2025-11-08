// src/services/receipt.service.js
import fs from "fs";
import path from "path";
import PDFDocument from "pdfkit";
import QRCode from "qrcode";
import Counter from "../models/counter.model.js";
import { ensureDirp, joinSafe } from "../utils/fs.util.js";
import {
  hmacSign,
  canonicalizeReceiptSignature,
} from "../utils/crypto.util.js";

const {
  // dónde se guardan archivos en disco (carpeta real)
  FILES_DIR = "files",
  // subcarpeta pública para recibos
  RECEIPTS_DIR = "files/receipts",
  // ruta pública expuesta por Express (app.use("/files", ...))
  FILES_PUBLIC_BASE = "/files",
  // origen público del server (IMPORTANTÍSIMO)
  SERVER_PUBLIC_ORIGIN = process.env.SERVER_PUBLIC_ORIGIN ||
    "http://localhost:4000",

  RECEIPT_PREFIX = "MEM",
  RECEIPT_PADDING = "7",
  RECEIPT_HMAC_SECRET = "",
  COMPANY_NAME = "Memorial S.A.",
  COMPANY_ADDRESS = "Av. Siempreviva 123, San Rafael, Mendoza",
  COMPANY_TAX_ID = "CUIT 30-12345678-9",
} = process.env;

function getYearStr(d = new Date()) {
  return String(d.getUTCFullYear());
}

async function nextSequence(key) {
  const doc = await Counter.findOneAndUpdate(
    { _id: key },
    {
      $inc: { seq: 1 },
      $setOnInsert: { meta: { period: key.split(":")[1] || "" } },
    },
    { new: true, upsert: true }
  ).lean();
  return doc.seq;
}

export async function getNextReceiptNumber({ at = new Date() } = {}) {
  const year = getYearStr(at);
  const counterKey = `receipt:${year}`;
  const seq = await nextSequence(counterKey);
  const pad = Math.max(1, Number(RECEIPT_PADDING || 7));
  return `${RECEIPT_PREFIX}${year}-${String(seq).padStart(pad, "0")}`;
}

export function buildQrPayload({ receiptNumber, payment, client, signature }) {
  return {
    v: 1,
    r: receiptNumber,
    a: Number(payment?.amount || 0),
    m: String(payment?.method || "cash"),
    p: String(payment?._id || ""),
    c: {
      idCliente: client?.idCliente ?? null,
      nombre: client?.nombre || client?.name || "",
      _id: client?._id || "",
    },
    t: payment?.postedAt ? new Date(payment.postedAt).toISOString() : null,
    sig: signature || "",
  };
}

async function generateQrPngBuffer(payloadObj) {
  const data = JSON.stringify(payloadObj);
  return await QRCode.toBuffer(data, {
    errorCorrectionLevel: "M",
    width: 256,
    margin: 1,
  });
}

/**
 * Devuelve:
 *   { pdfPath, pdfUrl, receiptNumber, qrData, signature }
 * - pdfPath: ruta ABSOLUTA en disco (para el server)
 * - pdfUrl:  URL ABSOLUTA pública (para el front) -> SERVER_PUBLIC_ORIGIN + FILES_PUBLIC_BASE + /receipts/<AÑO>/<NRO>.pdf
 */
export async function buildReceiptPDF(
  payment,
  client,
  { at = new Date() } = {}
) {
  if (!payment?._id) throw new Error("payment inválido en buildReceiptPDF");
  if (!client?._id) throw new Error("client inválido en buildReceiptPDF");

  const receiptNumber = await getNextReceiptNumber({ at });

  const canonical = canonicalizeReceiptSignature({
    receiptNumber,
    paymentId: String(payment._id),
    clientId: String(client._id),
    amount: payment.amount,
    postedAt: payment.postedAt || payment.createdAt || at,
  });
  const signature = hmacSign(canonical, RECEIPT_HMAC_SECRET);

  const qrData = buildQrPayload({ receiptNumber, payment, client, signature });
  const qrPng = await generateQrPngBuffer(qrData);

  const year = getYearStr(at);

  // === Paths de salida ===
  // 1) Disco (carpeta real)
  const diskDir = joinSafe(RECEIPTS_DIR, year); // p.ej. files/receipts/2025
  ensureDirp(diskDir);
  const fileName = `${receiptNumber}.pdf`;
  const pdfPath = joinSafe(diskDir, fileName); // ABSOLUTO en disco si RECEIPTS_DIR es absoluto

  // 2) URL pública ABSOLUTA
  const publicPath = `${FILES_PUBLIC_BASE.replace(
    /\/+$/,
    ""
  )}/receipts/${year}/${fileName}`;
  const pdfUrl = `${SERVER_PUBLIC_ORIGIN.replace(/\/+$/, "")}${publicPath}`;

  // === Render PDF ===
  await new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: "A4", margin: 48 });
    const stream = fs.createWriteStream(pdfPath);
    doc.pipe(stream);

    // Header
    doc
      .fontSize(18)
      .font("Helvetica-Bold")
      .text(COMPANY_NAME, { align: "left" });
    doc.moveDown(0.2);
    doc.fontSize(10).font("Helvetica").text(COMPANY_ADDRESS);
    doc.text(COMPANY_TAX_ID);
    doc.moveDown(0.8);

    doc
      .fontSize(12)
      .font("Helvetica-Bold")
      .text(`RECIBO: ${receiptNumber}`, { align: "right" });
    doc
      .fontSize(10)
      .font("Helvetica")
      .text(
        `Fecha: ${new Date(at).toLocaleString("es-AR", {
          timeZone: "America/Argentina/Mendoza",
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
        })}`,
        { align: "right" }
      );

    doc.moveDown(1);

    // Cliente
    doc.fontSize(12).font("Helvetica-Bold").text("Datos del Cliente");
    doc.moveDown(0.2);
    doc
      .fontSize(10)
      .font("Helvetica")
      .text(`Nombre: ${client?.nombre || client?.name || "-"}`);
    doc.text(`ID Cliente: ${client?.idCliente ?? "-"}`);
    if (client?.documento) doc.text(`Documento: ${client.documento}`);
    doc.moveDown(0.8);
    doc
      .moveTo(48, doc.y)
      .lineTo(548, doc.y)
      .strokeColor("#999")
      .stroke()
      .moveDown(0.8);

    // Pago
    doc.fontSize(12).font("Helvetica-Bold").text("Detalle del Pago");
    doc.moveDown(0.2);
    doc
      .fontSize(10)
      .font("Helvetica")
      .text(`Payment ID: ${String(payment._id)}`);
    doc.text(`Método: ${String(payment.method || "efectivo")}`);
    doc.text(`Estado: ${String(payment.status || "posted")}`);
    doc.text(
      `Fecha imputación: ${
        payment.postedAt
          ? new Date(payment.postedAt).toLocaleString("es-AR", {
              timeZone: "America/Argentina/Mendoza",
            })
          : "-"
      }`
    );
    doc.text(`Monto: $ ${Number(payment.amount || 0).toFixed(2)}`);
    if (payment?.notes) doc.text(`Notas: ${String(payment.notes)}`);

    doc.moveDown(0.8);
    doc
      .moveTo(48, doc.y)
      .lineTo(548, doc.y)
      .strokeColor("#999")
      .stroke()
      .moveDown(0.8);

    // QR
    const qrX = 48;
    const qrY = doc.y;
    try {
      doc.image(qrPng, qrX, qrY, { width: 120, height: 120 });
    } catch {
      doc.fontSize(8).text(JSON.stringify(qrData), { width: 200 });
    }
    doc
      .fontSize(9)
      .font("Helvetica")
      .text(
        "Escanee el código para validar el recibo. La firma digital (HMAC) permite verificar la integridad.",
        qrX + 140,
        qrY,
        { width: 360 }
      );
    doc.moveDown(8);

    // Footer
    doc.moveDown(1.2);
    doc
      .fontSize(8)
      .fillColor("#555")
      .text(
        "Este comprobante corresponde a un cobro registrado en el sistema Memorial. Cualquier alteración invalida el documento.",
        { align: "center" }
      );

    doc.end();
    stream.on("finish", resolve);
    stream.on("error", reject);
  });

  return { pdfPath, pdfUrl, receiptNumber, qrData, signature };
}

export async function generateReceipt(payment, client, opts = {}) {
  return await buildReceiptPDF(payment, client, opts);
}
