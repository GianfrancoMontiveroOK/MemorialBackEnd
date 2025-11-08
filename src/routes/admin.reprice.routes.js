// src/routes/admin.reprice.routes.js
import { Router } from "express";
import {
  recomputeGroupPricing,
  recomputeAllGroups,
  recomputeGroupsByIds,
} from "../services/pricing.services.js";

// Helpers simples (iguales a settings.routes)
function requireAuth(req, res, next) {
  if (req?.session?.user || req?.user) return next();
  return res.status(401).json({ ok: false, message: "No autenticado" });
}
function requireSuperAdmin(req, res, next) {
  const role = req?.session?.user?.role || req?.user?.role;
  if (role === "superAdmin") return next();
  return res.status(403).json({ ok: false, message: "Requiere rol superAdmin" });
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
  async (req, res) => {
    try {
      const { idCliente } = req.params;
      if (!idCliente?.toString().trim()) {
        return res.status(400).json({ ok: false, message: "idCliente requerido" });
      }
      const r = await recomputeGroupPricing(idCliente, { debug: false });
      res.json({ ok: true, ...r });
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ ok: false, message: err?.message || "Error reprice grupo" });
    }
  }
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
  async (req, res) => {
    try {
      const { concurrency, logEvery } = req.body || {};
      const r = await recomputeAllGroups({
        concurrency: Number.isFinite(Number(concurrency)) ? Number(concurrency) : undefined,
        logEvery: Number.isFinite(Number(logEvery)) ? Number(logEvery) : undefined,
        debug: false,
      });
      res.json({ ok: true, ...r });
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ ok: false, message: err?.message || "Error reprice-all" });
    }
  }
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
  async (req, res) => {
    try {
      const { ids, concurrency, logEvery } = req.body || {};
      if (!Array.isArray(ids) || ids.length === 0) {
        return res.status(400).json({ ok: false, message: "Se requiere 'ids' (array no vacío)" });
      }
      const r = await recomputeGroupsByIds(ids, {
        concurrency: Number.isFinite(Number(concurrency)) ? Number(concurrency) : undefined,
        logEvery: Number.isFinite(Number(logEvery)) ? Number(logEvery) : undefined,
        debug: false,
      });
      res.json({ ok: true, ...r });
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({ ok: false, message: err?.message || "Error reprice-by-ids" });
    }
  }
);

export default router;
