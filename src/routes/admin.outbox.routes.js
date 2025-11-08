// src/routes/admin.outbox.routes.js
import { Router } from "express";
import {
  requireSession,
  ensureUserLoaded,
  adminOnly,
} from "../middlewares/roles.js";
import { listAdminOutbox } from "../controllers/admin.outbox.controller.js";

const router = Router();

/**
 * GET /admin/outbox
 * Acceso: admin | superAdmin
 */
router.get(
  "/admin/outbox",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  listAdminOutbox
);

export default router;
