// src/routes/clientes.routes.js
import { Router } from "express";
import {
  listClientes,
  getClienteById,
  createCliente,
  updateCliente,
  deleteCliente,
  getClientesStats,
  getClientDebtAdmin,
  getCollectorSummaryAdmin,
} from "../controllers/clients.controller.js";

const router = Router();

/* ===================== Middlewares base ===================== */
function requireAuth(req, res, next) {
  if (req?.user || req?.session?.user) return next();
  return res.status(401).json({ ok: false, message: "No autenticado" });
}

function getRole(req) {
  return req?.user?.role || req?.session?.user?.role || null;
}

function requireAdminOrSuperAdmin(req, res, next) {
  const role = getRole(req);
  if (role === "admin" || role === "superAdmin") return next();
  return res
    .status(403)
    .json({ ok: false, message: "Requiere rol admin o superAdmin" });
}

/* 
   Cobrador puede ver SOLO /:id, con datos reducidos.
   Este middleware permite admin/superAdmin/cobrador para GET /:id
   y marca `req.redactSensitive = true` si el rol es 'cobrador'.
*/
function allowGetOneForCobrador(req, res, next) {
  const role = getRole(req);
  if (role === "cobrador") {
    // señal para el controller: devolver info reducida / anonimizada
    req.redactSensitive = true;
    return next();
  }
  if (role === "admin" || role === "superAdmin") {
    req.redactSensitive = false;
    return next();
  }
  return res
    .status(403)
    .json({ ok: false, message: "Acceso denegado a esta ruta" });
}

/* ===================== Rutas ===================== */

router.use(requireAuth);

// ---- SOLO admin/superAdmin ----
router.get("/stats", requireAdminOrSuperAdmin, getClientesStats);

// resumen admin de un cobrador (comisiones, KPIs, etc.)
router.get(
  "/collector-summary",
  requireAdminOrSuperAdmin,
  getCollectorSummaryAdmin
);

router.get("/", requireAdminOrSuperAdmin, listClientes);
router.post("/", requireAdminOrSuperAdmin, createCliente);
router.put("/:id", requireAdminOrSuperAdmin, updateCliente);
router.delete("/:id", requireAdminOrSuperAdmin, deleteCliente);

// deuda detallada del cliente (solo admin/superAdmin)
router.get("/:id/deuda", requireAdminOrSuperAdmin, getClientDebtAdmin);

// ---- GET /:id accesible a cobrador (con redacción) y a admin/superAdmin (completo) ----
router.get("/:id", allowGetOneForCobrador, getClienteById);

export default router;
