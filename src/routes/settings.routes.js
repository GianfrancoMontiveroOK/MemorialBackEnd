// src/routes/settings.routes.js
import { Router } from "express";
import {
  getGlobalPriceRules,
  updateGlobalPriceRules,
} from "../services/priceRules.provider.js";

// Helpers simples de auth/roles (usamos los tuyos)
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
 * GET /api/settings/price-rules
 * Devuelve reglas vigentes (forzando lectura fresca para evitar caché viejo)
 */
router.get(
  "/price-rules",
  requireAuth,
  requireSuperAdmin,
  async (_req, res) => {
    try {
      const rules = await getGlobalPriceRules({ force: true });
      res.json({ ok: true, rules });
    } catch (err) {
      const status = err?.status || 500;
      res.status(status).json({
        ok: false,
        message: err?.message || "Error al obtener reglas",
        details: err?.details,
      });
    }
  }
);

/**
 * PUT /api/settings/price-rules
 * Body: { priceRules: {...} }  o directamente {...}
 * Persiste reglas y devuelve la versión efectiva (normalizada).
 */
router.put("/price-rules", requireAuth, requireSuperAdmin, async (req, res) => {
  try {
    const incoming = req.body?.priceRules ?? req.body;
    if (!incoming || typeof incoming !== "object" || Array.isArray(incoming)) {
      return res
        .status(400)
        .json({
          ok: false,
          message: "Payload inválido: se esperaba un objeto 'priceRules'.",
        });
    }
    const rules = await updateGlobalPriceRules(incoming);
    res.json({ ok: true, rules });
  } catch (err) {
    const status = err?.status || 500;
    res.status(status).json({
      ok: false,
      message: err?.message || "Error al actualizar reglas",
      details: err?.details,
    });
  }
});

/* =================== OPCIONALES: tareas de mantenimiento ===================

import { recomputeAllGroups, recomputeGroupPricing } from "../services/pricing.services.js";
// Si unís jobs en pricing.jobs.js:
// import { fixAgesAndReprice, fixZeroPricing } from "../job/pricing.jobs.js";

// Reprice global (gated: superAdmin)
// router.post("/reprice-all", requireAuth, requireSuperAdmin, async (_req, res) => {
//   try {
//     const r = await recomputeAllGroups({ debug: false });
//     res.json({ ok: true, ...r });
//   } catch (err) {
//     res.status(500).json({ ok: false, message: err?.message || "Error reprice-all" });
//   }
// });

*/

export default router;
