// src/routes/admin-receipts.routes.js
import { Router } from "express";
import {
  requireSession,
  ensureUserLoaded,
  adminOnly,
} from "../middlewares/roles.js"; // ⬅️ TU archivo

import { listAdminReceipts } from "../controllers/admin-receipts.controller.js";

const router = Router();

// ⚠️ Sin prefijo /api acá. Se agrega en app.use("/api", …)
router.get(
  "/adminReceipts/receipts",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  listAdminReceipts
);

export default router;
