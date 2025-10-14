import { Router } from "express";
import {
  listClientes,
  getClienteById,
  createCliente,
  updateCliente,
  deleteCliente,
  // 👇 nuevo
  getClientesStats,
} from "../controllers/clients.controller.js";

const router = Router();

// GET /api/clientes/stats  (activos, agregados globales p/ dashboard)
router.get("/stats", getClientesStats);

// GET /api/clientes?page=&limit=&q=
router.get("/", listClientes);

// GET /api/clientes/:id
router.get("/:id", getClienteById);

// POST /api/clientes
router.post("/", createCliente);

// PUT /api/clientes/:id
router.put("/:id", updateCliente);

// DELETE /api/clientes/:id
router.delete("/:id", deleteCliente);

export default router;
