// src/routes/admin-arqueos.routes.js
import { Router } from "express";
import {
  requireSession,
  adminOnly,
  superAdminOnly,
} from "../middlewares/roles.js";

import {
  createAdminPayment,
  listAdminPayments, // 👈 nuevo import
} from "../controllers/admin.payments.controller.js";

const router = Router();

// 🔹 Cobro desde oficina (admin/superAdmin)
router.post("/pagos", requireSession, adminOnly, createAdminPayment);

// 🔹 Listado de pagos (vista admin/superAdmin)
// Incluye pagos de oficina y de cobradores; se filtra por cliente, fechas, etc.
router.get("/pagos", requireSession, adminOnly, listAdminPayments);

/**
 * ...aquí siguen/van el resto de rutas de arqueos
 *   router.get("/arqueos/...", requireSession, adminOnly, ... )
 *   router.get("/arqueos/resumen", requireSession, superAdminOnly, ... )
 */

export default router;
