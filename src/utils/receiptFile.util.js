// src/utils/receiptFile.util.js
import fs from "node:fs";
import path from "node:path";
import Payment from "../models/payment.model.js";
import Receipt from "../models/receipt.model.js";

const RECEIPTS_DIR =
  process.env.RECEIPTS_DIR || path.join(process.cwd(), "storage", "receipts");
// Debe matchear tu app.js => app.use("/files/receipts", express.static(RECEIPTS_DIR))
const PUBLIC_PREFIX = "/files/receipts/"; // <- importante: con trailing slash

const ALT_DIRS = [
  RECEIPTS_DIR,
  path.join(process.cwd(), "files", "receipts"),
  path.join(process.cwd(), "storage", "receipts"),
].filter((p, i, a) => !!p && a.indexOf(p) === i);

const exists = (p) => !!p && fs.existsSync(p);

function pathFromPdfUrl(pdfUrl) {
  if (!pdfUrl) return null;
  let pathname = pdfUrl;
  try {
    // Acepta http://otro-server:4000/files/receipts/2025/NAME.pdf
    pathname = new URL(pdfUrl).pathname;
  } catch {
    // si ya era relativo, lo usamos tal cual
  }
  // nos quedamos con lo que viene *después* de /files/receipts/
  const idx = pathname.indexOf(PUBLIC_PREFIX);
  if (idx === -1) return null;

  const rel = pathname.slice(idx + PUBLIC_PREFIX.length); // "2025/NAME.pdf"
  // construir candidato en el RECEIPTS_DIR actual
  return path.join(RECEIPTS_DIR, rel);
}

function yearFromNumber(number = "") {
  const m = String(number).match(/\D(\d{4})-\d+$/);
  return m ? m[1] : null;
}

export async function locateReceiptPdf(rx) {
  if (!rx) return null;

  // 0) pdfPath ya válido
  if (rx.pdfPath && exists(rx.pdfPath)) {
    return { absPath: rx.pdfPath, fileName: path.basename(rx.pdfPath) };
  }

  // 1) mapear desde pdfUrl absoluto o relativo (aunque sea de otro server)
  const mapped = pathFromPdfUrl(rx.pdfUrl);
  if (mapped && exists(mapped)) {
    return { absPath: mapped, fileName: path.basename(mapped) };
  }

  // 2) Buscar por número dentro de ALT_DIRS (por si cambiaste de carpeta)
  const fileBase = rx.number ? `${rx.number}.pdf` : null;
  if (fileBase) {
    // año por número (MEM2025-...)
    const y = yearFromNumber(rx.number);
    const candidates = [];
    for (const base of ALT_DIRS) {
      if (y) candidates.push(path.join(base, y, fileBase));
      candidates.push(path.join(base, fileBase)); // fallback sin subcarpeta
    }
    for (const c of candidates) {
      if (exists(c)) return { absPath: c, fileName: path.basename(c) };
    }
  }

  return null;
}

export async function backfillReceiptPdf(rx, found) {
  if (!rx || !found?.absPath) return;
  try {
    await Receipt.updateOne(
      { _id: rx._id },
      { $set: { pdfPath: found.absPath } }
    ).lean();
  } catch {}
}
