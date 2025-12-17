// src/services/receipt.service.js
import fs from "fs";
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

  // ✅ Tu dominio público (en prod NO debe ser localhost)
  SERVER_PUBLIC_ORIGIN = "https://www.api.memorialsanrafael.com.ar",

  // Providers (opcionales; si no los usás, no pasa nada)
  RENDER_EXTERNAL_URL = "",
  RENDER_EXTERNAL_HOSTNAME = "",
  RAILWAY_PUBLIC_DOMAIN = "",
  VERCEL_URL = "",

  RECEIPT_PREFIX = "MEM",
  RECEIPT_PADDING = "7",
  RECEIPT_HMAC_SECRET = "",
  COMPANY_NAME = "Memorial S.A.",
  COMPANY_ADDRESS = "Av. El Libertador 329, San Rafael, Mendoza",
  COMPANY_TAX_ID = "CUIT 30-12345678-9",

  NODE_ENV = "development",
} = process.env;

/* ---------------- origin helpers (solo para pdfUrl) ---------------- */
function isBadOrigin(o) {
  const s = String(o || "")
    .toLowerCase()
    .trim();
  return (
    !s ||
    s.includes("localhost") ||
    s.includes("127.0.0.1") ||
    s.startsWith("http://localhost") ||
    s.startsWith("https://localhost")
  );
}

function normalizeOrigin(o) {
  return String(o || "")
    .trim()
    .replace(/\/+$/, "");
}

// ✅ Se resuelve SIEMPRE en el service
function resolveServerPublicOrigin() {
  // 1) Preferido: tu dominio real (custom domain)
  const manual = normalizeOrigin(SERVER_PUBLIC_ORIGIN);
  if (manual && !isBadOrigin(manual)) return manual;

  // 2) Render
  const renderUrl = normalizeOrigin(RENDER_EXTERNAL_URL);
  if (renderUrl && !isBadOrigin(renderUrl)) return renderUrl;
  const renderHost = normalizeOrigin(RENDER_EXTERNAL_HOSTNAME);
  if (renderHost && !isBadOrigin(renderHost)) return `https://${renderHost}`;

  // 3) Railway
  const rail = normalizeOrigin(RAILWAY_PUBLIC_DOMAIN);
  if (rail && !isBadOrigin(rail)) return `https://${rail}`;

  // 4) Vercel (viene sin protocolo)
  const vercel = normalizeOrigin(VERCEL_URL);
  if (vercel && !isBadOrigin(vercel)) return `https://${vercel}`;

  // 5) Dev only fallback
  if (String(NODE_ENV).toLowerCase() !== "production") {
    return "http://localhost:4000";
  }

  // ❌ Prod: nunca devolver localhost por “default”
  throw new Error(
    "No se pudo resolver SERVER_PUBLIC_ORIGIN en producción. Seteá SERVER_PUBLIC_ORIGIN."
  );
}

/* ---------------- receipt logic ---------------- */
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
  // Disco
  const diskDir = joinSafe(RECEIPTS_DIR, year); // files/receipts/2025
  ensureDirp(diskDir);
  const fileName = `${receiptNumber}.pdf`;
  const pdfPath = joinSafe(diskDir, fileName);

  // ✅ URL pública ABSOLUTA (sin localhost en prod)
  const origin = resolveServerPublicOrigin();
  const publicPath = `${FILES_PUBLIC_BASE.replace(
    /\/+$/,
    ""
  )}/receipts/${year}/${fileName}`;
  const pdfUrl = `${origin}${publicPath}`;

  // === Render PDF (TU DISEÑO ORIGINAL, SIN CAMBIOS) ===
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
