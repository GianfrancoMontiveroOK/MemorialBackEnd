// src/routes/admin.transactions.routes.js
import { Router } from "express";
import {
  listAllPayments,
  importNaranjaResultFile,
  importBancoNacionResultFile,
} from "../controllers/admin.transactions.controller.js";
import {
  requireSession,
  ensureUserLoaded,
  adminOnly,
} from "../middlewares/roles.js";

// 👇 ajustá este import al middleware real que uses para uploads
import upload from "../middlewares/upload.js";
// o, si usás multer directo:
// import multer from "multer";
// const upload = multer({ storage: multer.memoryStorage() });

const router = Router();

// Listado de todas las transacciones
router.get(
  "/transactions",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  listAllPayments
);

// Importar archivo de Naranja (débitos automáticos)
// espera campo "file" (FormData)
router.post(
  "/import-naranja",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  upload.single("file"),
  importNaranjaResultFile
);

// Importar archivo de Banco Nación (débitos automáticos)
// espera campo "file" (FormData)
router.post(
  "/import-bna",
  requireSession,
  ensureUserLoaded,
  adminOnly,
  upload.single("file"),
  importBancoNacionResultFile
);

export default router;
