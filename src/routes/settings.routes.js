// src/routes/settings.routes.js
import { Router } from "express";
import {
  getGlobalPriceRules,
  updateGlobalPriceRules,
} from "../services/priceRules.provider.js";

// (Opcional) helpers simples de auth/roles.
// Si ya tenés middlewares, reemplazá por los tuyos (e.g., requireAuth, requireRole('superAdmin')).
function requireAuth(req, res, next) {
  // Ajustá según tu sesión/token
  if (req?.session?.user || req?.user) return next();
  return res.status(401).json({ message: "No autenticado" });
}
function requireSuperAdmin(req, res, next) {
  const role = req?.session?.user?.role || req?.user?.role;
  if (role === "superAdmin") return next();
  return res.status(403).json({ message: "Requiere rol superAdmin" });
}

const router = Router();

/**
 * GET /api/settings/price-rules
 * Devuelve el objeto de reglas dinámicas vigentes
 */
router.get(
  "/price-rules",
  requireAuth,
  requireSuperAdmin,
  async (req, res, next) => {
    try {
      const rules = await getGlobalPriceRules();
      res.json({ priceRules: rules });
    } catch (err) {
      next(err);
    }
  }
);

/**
 * PUT /api/settings/price-rules
 * Body: { priceRules: {...} }
 * Persiste y cachea las reglas nuevas.
 */
router.put(
  "/price-rules",
  requireAuth,
  requireSuperAdmin,
  async (req, res, next) => {
    try {
      const incoming = req.body?.priceRules ?? req.body;
      if (!incoming || typeof incoming !== "object") {
        return res.status(400).json({ message: "Payload inválido" });
      }
      const saved = await updateGlobalPriceRules(incoming);
      res.json({ priceRules: saved, ok: true });
    } catch (err) {
      next(err);
    }
  }
);

/* =================== OPCIONALES: tareas de mantenimiento ===================

import { default as recomputeGroupPricing } from "../services/pricing.services.js";
import fixAgesAndReprice from "../job/age.fix.js";
import fixZeroPricing from "../job/pricing.fix.js";

// Disparar reprice global (gated: superAdmin)
router.post("/reprice-all", requireAuth, requireSuperAdmin, async (req, res, next) => {
  try {
    // Podés leer base de req.body o de las reglas globales
    // … implementar según tu preferencia …
    res.json({ ok: true, started: true });
  } catch (err) {
    next(err);
  }
});

*/

export default router;
