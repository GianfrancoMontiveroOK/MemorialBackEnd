// /routes/receipts.routes.js
import { Router } from "express";
import fs from "node:fs";
import Receipt from "../models/receipt.model.js";
import { requireSession, ensureUserLoaded } from "../middlewares/roles.js";
import {
  locateReceiptPdf,
  backfillReceiptPdf,
} from "../utils/receiptFile.util.js";

const router = Router();

router.get(
  "/receipts/:id/pdf",
  requireSession,
  ensureUserLoaded,
  async (req, res) => {
    try {
      const id = String(req.params.id || "").trim();
      if (!/^[0-9a-fA-F]{24}$/.test(id)) {
        return res.status(404).json({ message: "Recibo no encontrado" });
      }

      const rx = await Receipt.findById(id).lean();
      if (!rx) return res.status(404).json({ message: "Recibo no encontrado" });

      // RBAC si aplica
      // if (!puedeVer(req.user, rx)) return res.status(403).json({ message: "Prohibido" });

      const found = await locateReceiptPdf(rx);
      if (!found || !found.absPath || !fs.existsSync(found.absPath)) {
        console.warn(
          `[receipts] 404: PDF no disponible. id=${id} number=${
            rx.number || "-"
          } pdfPath=${rx.pdfPath || "-"}`
        );
        return res.status(404).json({ message: "PDF no disponible" });
      }

      await backfillReceiptPdf(rx, found);

      const safeName = (rx.number || id).toString().replace(/[^\w.-]+/g, "_");
      res.setHeader("Content-Type", "application/pdf");
      res.setHeader(
        "Content-Disposition",
        `inline; filename="Recibo_${safeName}.pdf"`
      );
      res.setHeader("Cache-Control", "private, max-age=0, must-revalidate");

      const stream = fs.createReadStream(found.absPath);
      stream.on("error", (err) => {
        console.error("[receipts] stream error:", err);
        if (!res.headersSent)
          res.status(500).json({ message: "Error leyendo el PDF" });
        else res.end();
      });
      stream.pipe(res);
    } catch (err) {
      console.error("[receipts] 500:", err);
      res.status(500).json({ message: "Error interno" });
    }
  }
);

export default router;
