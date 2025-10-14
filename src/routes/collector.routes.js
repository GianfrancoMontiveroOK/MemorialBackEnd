// src/routes/collector.routes.js
import { Router } from "express";
import {
  listCollectorClients,
  createCollectorPayment,
} from "../controllers/collector.controller.js";
import {
  requireSession,
  ensureUserLoaded,
  cobradorOnly,
} from "../middlewares/roles.js";
import { ensureCollectorLoaded } from "../middlewares/roles.js";

const router = Router();

// Listado de clientes del cobrador
router.get(
  "/clientes",
  requireSession,
  ensureUserLoaded, // => req.user: { _id, role }
  ensureCollectorLoaded, // => req.user.idCobrador
  cobradorOnly, // roles: vendedor|cobrador|admin|superAdmin
  listCollectorClients
);

// (opcional) registrar un pago
router.post(
  "/pagos",
  requireSession,
  ensureUserLoaded,
  ensureCollectorLoaded,
  cobradorOnly,
  createCollectorPayment
);

export default router;
