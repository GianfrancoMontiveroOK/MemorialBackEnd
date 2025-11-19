// src/routes/admin.items.routes.js
import { Router } from "express";
import {
  listAdminItems,
  getAdminItem,
  createAdminItem,
  updateAdminItem,
  deleteAdminItem,
} from "../controllers/admin.items.controller.js";

// ⚠️ Ajustá estos imports a tus middlewares reales
// import { requireSession, requireRole } from "../middlewares/auth.js";

const router = Router();

// Si usás RBAC, algo así:
// router.use(requireSession, requireRole("admin")); // o "superAdmin"

router.get("/", listAdminItems);
router.get("/:id", getAdminItem);
router.post("/", createAdminItem);
router.put("/:id", updateAdminItem);
router.delete("/:id", deleteAdminItem);

export default router;
