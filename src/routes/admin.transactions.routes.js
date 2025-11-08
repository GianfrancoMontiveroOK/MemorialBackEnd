// src/routes/admin.transactions.routes.js
import { Router } from "express";
import { listAllPayments } from "../controllers/admin.transactions.controller.js";
import {
  requireSession,
  ensureUserLoaded,
  adminOnly,
} from "../middlewares/roles.js";

const router = Router();

router.get(
  "/transactions",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  listAllPayments
);

export default router;
