// src/routes/collector.routes.js
import { Router } from "express";

// Clientes (cartera del cobrador)
import {
  listCollectorClients,
  getCollectorClientById,
  getCollectorClientDebt,
  getCollectorSummary,
} from "../controllers/collector.clients.controller.js";

// Pagos (transaccional)
import {
  listCollectorPayments,
  createCollectorPayment,
  // reverseCollectorPayment, // ⬅️ cuando lo implementes, descomentar
} from "../controllers/collector.payments.controller.js";

// Recibos (solo lectura para cobradores) ⬅️ NUEVO
import {
  listCollectorReceipts, // GET /collector/receipts
  streamCollectorReceiptPdf, // GET /collector/receipts/:id/pdf  (opcional)
} from "../controllers/collector.receipts.controller.js";

import {
  requireSession,
  ensureUserLoaded,
  cobradorOnly,
  ensureCollectorLoaded,
} from "../middlewares/roles.js";

const router = Router();

/* ────────────────────── Clientes del cobrador ────────────────────── */

// GET /collector/clientes → lista 1 por grupo (TITULAR) con agregados
router.get(
  "/clientes",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded, // setea req.user.idCobrador
  cobradorOnly,
  listCollectorClients
);

// GET /collector/clientes/:id → detalle (miembro) + grupo, proyección “collector”
router.get(
  "/clientes/:id",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded,
  cobradorOnly,
  getCollectorClientById
);

// GET /collector/clientes/:id/deuda → estado por períodos (MVP)
router.get(
  "/clientes/:id/deuda",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded,
  cobradorOnly,
  getCollectorClientDebt
);

/* ─────────────────────────── Pagos ─────────────────────────── */

// GET /collector/pagos → lista pagos reales del cobrador
router.get(
  "/pagos",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded,
  cobradorOnly,
  listCollectorPayments
);

// POST /collector/pagos → crea pago transaccional (Payment + Ledger + Receipt + Outbox)
router.post(
  "/pagos",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded,
  cobradorOnly,
  createCollectorPayment
);

// // POST /collector/pagos/:id/reverse → reversa un pago (cuando implementes el controller)
// router.post(
//   "/pagos/:id/reverse",
//   requireSession,
//   ensureUserLoaded,
//   ensureCollectorLoaded,
//   cobradorOnly,
//   reverseCollectorPayment
// );

/* ────────────────────────── Recibos ────────────────────────── */
/**
 * GET /collector/receipts
 * Lista recibos emitidos por pagos del cobrador (filtrable por clientId, fechas, q).
 * Responde en el mismo shape que listCollectorPayments: { items, total, page, limit, ... }
 */
router.get(
  "/receipts",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded,
  cobradorOnly,
  listCollectorReceipts
);

/**
 * GET /collector/receipts/:id/pdf
 * (Opcional) Stream del PDF del recibo por ID.
 * Útil si no querés exponer /files/... directamente o si el front decide usar endpoint seguro.
 */
router.get(
  "/receipts/:id/pdf",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded,
  cobradorOnly,
  streamCollectorReceiptPdf
);

router.get(
  "/summary",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded, // setea req.user.idCobrador
  cobradorOnly,
  getCollectorSummary
);

export default router;
