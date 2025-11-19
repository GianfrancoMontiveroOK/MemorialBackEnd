// src/routes/admin-arqueos.routes.js
import { Router } from "express";
import {
  requireSession,
  adminOnly,
  superAdminOnly,
} from "../middlewares/roles.js";
import {
  listArqueosUsuarios,
  getArqueoUsuarioDetalle,
  listArqueoUsuarioClientes, // JSON listado de clientes por cobrador
  crearArqueoUsuario, // Crear arqueo/corte manual
  exportCollectorClientsCSV, // CSV de clientes por cobrador

  // ➕ NUEVOS controladores de caja chica / grande
  depositoCajaChica, // admin → su CAJA_CHICA
  ingresoCajaGrande, // superAdmin: CAJA_CHICA (admin) → CAJA_GRANDE (SA)
  extraccionCajaGrande, // superAdmin: CAJA_GRANDE → CAJA_SUPERADMIN
  getGlobalCajasBalance,
  getArqueoGlobalTotals,
} from "../controllers/admin-arqueos.controller.js";

const router = Router();

/**
 * Listado de cajas por usuario (admins y cobradores)
 * GET /api/admin/arqueos/usuarios
 */
router.get(
  "/admin/arqueos/usuarios",
  requireSession,
  adminOnly,
  listArqueosUsuarios
);

/**
 * Detalle de movimientos del usuario (drill-down)
 * GET /api/admin/arqueos/usuarios/detalle
 */
router.get(
  "/admin/arqueos/usuarios/detalle",
  requireSession,
  adminOnly,
  getArqueoUsuarioDetalle
);

/**
 * Listado JSON de clientes del cobrador (agrupado por idCliente/titular)
 * GET /api/admin/arqueos/usuarios/clientes
 */
router.get(
  "/admin/arqueos/usuarios/clientes",
  requireSession,
  adminOnly,
  listArqueoUsuarioClientes
);

/**
 * Descarga CSV de clientes del cobrador
 * GET /api/admin/arqueos/usuarios/clientes-csv
 */
router.get(
  "/admin/arqueos/usuarios/clientes-csv",
  requireSession,
  adminOnly,
  exportCollectorClientsCSV
);

/**
 * Crear arqueo/corte manual de caja
 * POST /api/admin/arqueos/usuarios/arqueo
 */
router.post(
  "/admin/arqueos/usuarios/arqueo",
  requireSession,
  adminOnly,
  crearArqueoUsuario
);

/* =========================================================================
 *  NUEVAS RUTAS — Caja chica / Caja grande
 * ========================================================================= */

/**
 * Depósito a CAJA_CHICA (admin sobre sí mismo o superAdmin sobre cualquier admin)
 * POST /api/admin/caja/chica/deposito
 */
router.post(
  "/admin/caja/chica/deposito",
  requireSession,
  adminOnly, // permite admin y superAdmin
  depositoCajaChica
);

/**
 * Ingreso a CAJA_GRANDE (mueve desde CAJA_CHICA de un admin)
 * Sólo superAdmin
 * POST /api/admin/caja/grande/ingreso
 */
router.post(
  "/admin/caja/grande/ingreso",
  requireSession,
  superAdminOnly,
  ingresoCajaGrande
);

/**
 * Extracción desde CAJA_GRANDE hacia CAJA_SUPERADMIN (billetera SA)
 * Sólo superAdmin
 * POST /api/admin/caja/grande/extraccion
 */
router.post(
  "/admin/caja/grande/extraccion",
  requireSession,
  superAdminOnly,
  extraccionCajaGrande
);
router.get(
  "/admin/arqueos/global-cajas-balance",
  requireSession,
  adminOnly,
  getGlobalCajasBalance
);
router.get(
  "/admin/arqueos/global-totals",
  requireSession,
  adminOnly, // o superAdminOnly si querés restringirlo
  getArqueoGlobalTotals
);
export default router;
