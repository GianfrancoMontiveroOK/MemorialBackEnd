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
  FILES_DIR = "files",
  RECEIPTS_DIR = "files/receipts",
  FILES_PUBLIC_BASE = "/files",

  // ⛔️ NO default a localhost acá
  SERVER_PUBLIC_ORIGIN = "https://www.api.memorialsanrafael.com.ar/",

  // Providers (auto)
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
    "No se pudo resolver SERVER_PUBLIC_ORIGIN en producción. Seteá SERVER_PUBLIC_ORIGIN (recomendado) o asegurate que tu provider exponga una URL pública."
  );
}

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

  // Disco
  const diskDir = joinSafe(RECEIPTS_DIR, year);
  ensureDirp(diskDir);

  const fileName = `${receiptNumber}.pdf`;
  const pdfPath = joinSafe(diskDir, fileName);

  // ✅ URL pública (resuelta por el service)
  const origin = resolveServerPublicOrigin();
  const publicPath = `${FILES_PUBLIC_BASE.replace(
    /\/+$/,
    ""
  )}/receipts/${year}/${fileName}`;
  const pdfUrl = `${origin}${publicPath}`;

  await new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: "A4", margin: 48 });
    const stream = fs.createWriteStream(pdfPath);
    doc.pipe(stream);

    // ... tu render del PDF igual que antes ...
    doc.end();
    stream.on("finish", resolve);
    stream.on("error", reject);
  });

  return { pdfPath, pdfUrl, receiptNumber, qrData, signature };
}

export async function generateReceipt(payment, client, opts = {}) {
  return await buildReceiptPDF(payment, client, opts);
}
