// src/routes/admin.ledger-entry.routes.js
import { Router } from "express";
import {
  requireSession,
  ensureUserLoaded,
  adminOnly,
} from "../middlewares/roles.js";
import { listAdminLedgerEntries } from "../controllers/admin.ledger-entry.controller.js";

const router = Router();

/**
 * GET /admin/ledger-entries
 * Acceso: admin | superAdmin
 */
router.get(
  "/admin/ledger-entries",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  listAdminLedgerEntries
);

export default router;
