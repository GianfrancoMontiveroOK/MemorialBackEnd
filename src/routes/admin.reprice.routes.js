// src/routes/admin.reprice.routes.js
import { Router } from "express";
import {
  repriceGroupController,
  repriceAllGroupsController,
  repriceByIdsController,
  increasePercentController,
  repriceProgressController,
} from "../controllers/admin.reprice.controller.js";


// Helpers simples (iguales a settings.routes)
function requireAuth(req, res, next) {
  if (req?.session?.user || req?.user) return next();
  return res.status(401).json({ ok: false, message: "No autenticado" });
}
function requireSuperAdmin(req, res, next) {
  const role = req?.session?.user?.role || req?.user?.role;
  if (role === "superAdmin") return next();
  return res
    .status(403)
    .json({ ok: false, message: "Requiere rol superAdmin" });
}

const router = Router();

/**
 * POST /api/admin/reprice/:idCliente
 * Recalcula 'cuotaIdeal' para un grupo (por idCliente).
 */
router.post(
  "/reprice/:idCliente",
  requireAuth,
  requireSuperAdmin,
  repriceGroupController
);

/**
 * POST /api/admin/reprice-all
 * Recalcula 'cuotaIdeal' para TODOS los grupos.
 * Body opcional: { concurrency?: number, logEvery?: number }
 */
router.post(
  "/reprice-all",
  requireAuth,
  requireSuperAdmin,
  repriceAllGroupsController
);

router.get(
  "/reprice-progress",
  requireAuth,
  requireSuperAdmin,
  repriceProgressController
);

/**
 * POST /api/admin/reprice-by-ids
 * Recalcula 'cuotaIdeal' para una lista de grupos.
 * Body: { ids: Array<string|number>, concurrency?: number, logEvery?: number }
 */
router.post(
  "/reprice-by-ids",
  requireAuth,
  requireSuperAdmin,
  repriceByIdsController
);

/**
 * POST /api/admin/increase-percent
 * Aumenta precios por porcentaje.
 * Body:
 * {
 *   percent: number,
 *   applyToIdeal: boolean,
 *   applyToHistorical: boolean,
 * }
 */
router.post(
  "/increase-percent",
  requireAuth,
  requireSuperAdmin,
  increasePercentController
);

export default router;
