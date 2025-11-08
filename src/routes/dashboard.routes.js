// src/routes/dashboard.routes.js
import { Router } from "express";
import { requireSession, ensureUserLoaded } from "../middlewares/roles.js";
import { checkDashboardAccess } from "../controllers/dashboard.controller.js";

const router = Router();

// Ruta única
router.get(
  "/dashboard",
  requireSession,
  ensureUserLoaded,
  checkDashboardAccess
);

export default router;
