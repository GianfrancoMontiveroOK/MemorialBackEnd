// src/routes/admin.stats.routes.js
import { Router } from "express";
import { getClientesStats } from "../controllers/admin.stats.controller.js";
import { requireSession, adminOnly } from "../middlewares/roles.js";

const router = Router();

/**
 * GET /admin/clientes/stats
 * Query:
 *  - period=YYYY-MM   (requerido)
 *  - idCobrador=NUM   (opcional)
 *  - method=efectivo|transferencia|tarjeta|qr|otro (opcional)
 *  - channel=field|backoffice|portal|api (opcional)
 */
router.get(
  "/admin/clientes/stats",
  requireSession, // cookie/JWT válida
  adminOnly, // sólo admin/superAdmin (según tu implementación)
  getClientesStats
);

export default router;
