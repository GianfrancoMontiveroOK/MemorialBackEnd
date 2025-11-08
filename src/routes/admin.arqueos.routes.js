// src/routes/admin-arqueos.routes.js
import { Router } from "express";
import { requireSession, adminOnly } from "../middlewares/roles.js";
import {
  listArqueosUsuarios,
  getArqueoUsuarioDetalle,
  listArqueoUsuarioClientes, // JSON listado de clientes por cobrador
  crearArqueoUsuario, // Crear arqueo/corte manual
  exportCollectorClientsCSV, // ⬅️ NUEVO: CSV de clientes por cobrador
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

export default router;
