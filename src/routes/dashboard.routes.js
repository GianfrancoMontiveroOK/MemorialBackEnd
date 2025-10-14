import { Router } from "express";
import { getDashboard } from "../controllers/dashboard.controller.js";
import {
  requireSession,
  ensureUserLoaded,
  superAdminOnly,
  adminOnly,
  cobradorOnly,
} from "../middlewares/roles.js";

const router = Router();

router.get("/dashboard", requireSession, ensureUserLoaded, getDashboard); // 👈

router.get("/dashboard/admin", requireSession, adminOnly, (_req, res) =>
  res.json({ message: "Zona exclusiva de Admin/SuperAdmin" })
);
router.get("/dashboard/super", requireSession, superAdminOnly, (_req, res) =>
  res.json({ message: "Zona exclusiva de SuperAdmin" })
);
router.get("/dashboard/cobrador", requireSession, cobradorOnly, (_req, res) =>
  res.json({ message: "Zona para Cobradores/Admin/SuperAdmin" })
);

export default router;
