// src/routes/users.routes.js
import { Router } from "express";
import cors from "cors";
import {
  listUsers,
  listRecentUsers,
  getUserById,
  updateUser,
  setUserRole,
  setUserCobrador,
  setUserVendedor,
  setCollectorCommission,
  setCollectorCommissionGraceDays,
  setCollectorCommissionPenaltyPerDay,
} from "../controllers/users.controller.js";
import {
  requireSession,
  ensureUserLoaded,
  adminOnly,
  superAdminOnly,
} from "../middlewares/roles.js";

const router = Router();

// habilitar preflight explícito
router.options("/users", cors());
router.options("/users/*", cors());

// GET /api/users
router.get("/users", requireSession, ensureUserLoaded, adminOnly, listUsers);

// GET /api/users/recent
router.get(
  "/users/recent",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  listRecentUsers
);

// GET /api/users/:id
router.get(
  "/users/:id",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  getUserById
);

// PUT /api/users/:id
router.put(
  "/users/:id",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  updateUser
);

// PATCH /api/users/:id/role
router.patch(
  "/users/:id/role",
  requireSession,
  ensureUserLoaded,
  superAdminOnly,
  setUserRole
);

// PATCH /api/users/:id/cobrador
router.patch(
  "/users/:id/cobrador",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  setUserCobrador
);

// PATCH /api/users/:id/vendedor
router.patch(
  "/users/:id/vendedor",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  setUserVendedor
);

// ⬇️ nuevos
router.patch(
  "/users/:id/collector-commission",
  requireSession,
  setCollectorCommission
);
router.patch(
  "/users/:id/collector-commission-grace-days",
  requireSession,
  setCollectorCommissionGraceDays
);
router.patch(
  "/users/:id/collector-commission-penalty",
  requireSession,
  setCollectorCommissionPenaltyPerDay
);
export default router;
