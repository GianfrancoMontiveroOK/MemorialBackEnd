import fs from "node:fs";
import path from "node:path";
import PDFDocument from "pdfkit";
import Receipt from "../models/receipt.model.js";

const FILES_BASE_DIR =
  (process.env.FILES_BASE_DIR && process.env.FILES_BASE_DIR.trim()) ||
  process.cwd();

// Donde servís estático los archivos. Ej: app.use("/files", express.static("files"))
const FILES_PUBLIC_BASE = (process.env.FILES_PUBLIC_BASE || "/files").replace(
  /\/+$/,
  ""
);

function ensureDirSync(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

export async function ensureReceiptPdf(rx) {
  if (!rx) throw new Error("Receipt requerido");
  // Si ya lo tiene, devolvémoslo
  if (rx.pdfPath && fs.existsSync(rx.pdfPath)) {
    const rel = path.relative(FILES_BASE_DIR, rx.pdfPath).replaceAll("\\", "/");
    const pdfUrl = `${FILES_PUBLIC_BASE}/${rel}`;
    return { absPath: rx.pdfPath, pdfUrl };
  }

  // Construcción de path destino
  const created = new Date(rx.createdAt || Date.now());
  const yyyy = String(created.getFullYear());
  const mm = String(created.getMonth() + 1).padStart(2, "0");

  const safeNumber = (rx.number || rx._id).toString().replace(/[^\w.-]+/g, "_");
  const relDir = `files/receipts/${yyyy}/${mm}`;
  const absDir = path.resolve(FILES_BASE_DIR, relDir);
  ensureDirSync(absDir);

  const filename = `Recibo_${safeNumber}.pdf`;
  const absPath = path.resolve(absDir, filename);

  // Generar PDF (mínimo ejemplo — reemplazá por tu template real)
  await new Promise((resolve, reject) => {
    const doc = new PDFDocument({ size: "A4", margin: 36 });
    const stream = fs.createWriteStream(absPath);
    doc.pipe(stream);

    doc.fontSize(18).text("RECIBO MEMORIAL", { align: "center" }).moveDown();
    doc.fontSize(12).text(`Recibo: ${rx.number || rx._id}`);
    if (rx.clientName) doc.text(`Cliente: ${rx.clientName}`);
    if (rx.clientId) doc.text(`Cliente #${rx.clientId}`);
    if (rx.amount != null) doc.text(`Importe: $ ${Number(rx.amount || 0)}`);
    doc.text(
      `Fecha: ${new Date(rx.createdAt || Date.now()).toLocaleString("es-AR")}`
    );
    doc.moveDown().text("Gracias por su pago.", { align: "left" });

    doc.end();
    stream.on("finish", resolve);
    stream.on("error", reject);
  });

  const rel = path.relative(FILES_BASE_DIR, absPath).replaceAll("\\", "/");
  const pdfUrl = `${FILES_PUBLIC_BASE}/${rel}`;

  // Persistimos en Mongo
  await Receipt.updateOne(
    { _id: rx._id },
    { $set: { pdfPath: absPath, pdfUrl } }
  );

  return { absPath, pdfUrl };
}
