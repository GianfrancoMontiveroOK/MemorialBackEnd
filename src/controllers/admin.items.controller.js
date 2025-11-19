// src/controllers/admin.items.controller.js
import mongoose from "mongoose";
import Item from "../models/item.model.js";

const toInt = (v, def = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};

const toDir = (v, def = -1) => {
  if (String(v).toLowerCase() === "asc") return 1;
  if (String(v).toLowerCase() === "desc") return -1;
  return def;
};

/* ============ GET /admin/items ============ */
/**
 * Listar ítems (cajones, etc.)
 *
 * Query:
 * - page, limit
 * - q: búsqueda por código / nombre / descripción
 * - activo: "1" solo activos, "0" solo inactivos, vacío = todos
 */
export async function listAdminItems(req, res) {
  try {
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.min(toInt(req.query.limit, 25), 100);
    const { q = "", activo, sortBy = "nombre", sortDir = "asc" } = req.query;

    const filter = {
      deletedAt: { $exists: false },
    };

    if (q.trim()) {
      const regex = new RegExp(q.trim(), "i");
      filter.$or = [
        { codigo: regex },
        { nombre: regex },
        { descripcion: regex },
      ];
    }

    if (activo === "1") filter.activo = true;
    if (activo === "0") filter.activo = false;

    const sort = {};
    sort[sortBy] = toDir(sortDir, 1);

    const [items, total] = await Promise.all([
      Item.find(filter)
        .sort(sort)
        .skip((page - 1) * limit)
        .limit(limit)
        .lean(),
      Item.countDocuments(filter),
    ]);

    return res.json({
      ok: true,
      data: items,
      total,
      page,
      limit,
    });
  } catch (err) {
    console.error("listAdminItems error:", err);
    return res.status(500).json({
      ok: false,
      message: "No se pudo listar los ítems.",
    });
  }
}

/* ============ GET /admin/items/:id ============ */
export async function getAdminItem(req, res) {
  try {
    const { id } = req.params;
    if (!mongoose.isValidObjectId(id)) {
      return res
        .status(400)
        .json({ ok: false, message: "ID de ítem inválido." });
    }

    const item = await Item.findById(id).lean();
    if (!item || item.deletedAt) {
      return res
        .status(404)
        .json({ ok: false, message: "Ítem no encontrado." });
    }

    return res.json({ ok: true, data: item });
  } catch (err) {
    console.error("getAdminItem error:", err);
    return res.status(500).json({
      ok: false,
      message: "No se pudo obtener el ítem.",
    });
  }
}

/* ============ POST /admin/items ============ */
/**
 * Crear ítem (cajón).
 *
 * Body esperado:
 * - codigo? (string)
 * - nombre (string)
 * - descripcion?
 * - tipo? (por ahora "cajon")
 * - stockActual?
 * - precioCompra, margenPct
 * - adicionalPct, adicionalValor
 * - activo?
 */
export async function createAdminItem(req, res) {
  try {
    const {
      codigo,
      nombre,
      descripcion,
      tipo = "cajon",
      stockActual,
      precioCompra,
      margenPct,
      adicionalPct,
      adicionalValor,
      activo,
    } = req.body || {};

    if (!nombre || !String(nombre).trim()) {
      return res
        .status(400)
        .json({ ok: false, message: "El nombre del ítem es obligatorio." });
    }

    const doc = new Item({
      codigo: codigo?.trim() || undefined,
      nombre: nombre.trim(),
      descripcion: descripcion?.trim() || undefined,
      tipo,
      stockActual: stockActual != null ? Number(stockActual) : undefined,
      precioCompra: precioCompra != null ? Number(precioCompra) : undefined,
      margenPct: margenPct != null ? Number(margenPct) : undefined,
      adicionalPct: adicionalPct != null ? Number(adicionalPct) : undefined,
      adicionalValor:
        adicionalValor != null ? Number(adicionalValor) : undefined,
      activo: activo !== undefined ? !!activo : undefined,
    });

    await doc.save(); // dispara el pre('save') y calcula precioLista

    return res.status(201).json({ ok: true, data: doc });
  } catch (err) {
    console.error("createAdminItem error:", err);

    if (err.code === 11000) {
      return res.status(409).json({
        ok: false,
        message: "Ya existe un ítem con ese código.",
      });
    }

    return res.status(500).json({
      ok: false,
      message: "No se pudo crear el ítem.",
    });
  }
}

/* ============ PUT /admin/items/:id ============ */
/**
 * Editar ítem.
 *
 * Recalcula precioLista cuando cambia precioCompra o margenPct
 */
export async function updateAdminItem(req, res) {
  try {
    const { id } = req.params;
    if (!mongoose.isValidObjectId(id)) {
      return res
        .status(400)
        .json({ ok: false, message: "ID de ítem inválido." });
    }

    const item = await Item.findById(id);
    if (!item || item.deletedAt) {
      return res
        .status(404)
        .json({ ok: false, message: "Ítem no encontrado." });
    }

    const {
      codigo,
      nombre,
      descripcion,
      tipo,
      stockActual,
      precioCompra,
      margenPct,
      adicionalPct,
      adicionalValor,
      activo,
    } = req.body || {};

    if (codigo !== undefined) item.codigo = codigo?.trim() || undefined;
    if (nombre !== undefined) item.nombre = nombre?.trim() || item.nombre;
    if (descripcion !== undefined)
      item.descripcion = descripcion?.trim() || undefined;
    if (tipo !== undefined) item.tipo = tipo;

    if (stockActual !== undefined) item.stockActual = Number(stockActual) || 0;

    if (precioCompra !== undefined)
      item.precioCompra = Number(precioCompra) || 0;
    if (margenPct !== undefined) item.margenPct = Number(margenPct) || 0;

    if (adicionalPct !== undefined)
      item.adicionalPct = Number(adicionalPct) || 0;
    if (adicionalValor !== undefined)
      item.adicionalValor = Number(adicionalValor) || 0;

    if (activo !== undefined) item.activo = !!activo;

    await item.save(); // recalcula precioLista si corresponde

    return res.json({ ok: true, data: item });
  } catch (err) {
    console.error("updateAdminItem error:", err);

    if (err.code === 11000) {
      return res.status(409).json({
        ok: false,
        message: "Ya existe un ítem con ese código.",
      });
    }

    return res.status(500).json({
      ok: false,
      message: "No se pudo actualizar el ítem.",
    });
  }
}

/* ============ DELETE /admin/items/:id ============ */
/**
 * Soft delete: marca deletedAt y activo=false
 *
 * ⚠️ Esto es importante porque más adelante estos ítems pueden estar
 * referenciados en:
 * - servicios (sepelios realizados)
 * - ledger entries (costo de mercadería)
 * - facturas ARCA (outbox/facturacion)
 */
export async function deleteAdminItem(req, res) {
  try {
    const { id } = req.params;
    if (!mongoose.isValidObjectId(id)) {
      return res
        .status(400)
        .json({ ok: false, message: "ID de ítem inválido." });
    }

    const item = await Item.findById(id);
    if (!item || item.deletedAt) {
      return res
        .status(404)
        .json({ ok: false, message: "Ítem no encontrado." });
    }

    item.activo = false;
    item.deletedAt = new Date();
    await item.save();

    return res.json({
      ok: true,
      message: "Ítem eliminado (soft delete).",
    });
  } catch (err) {
    console.error("deleteAdminItem error:", err);
    return res.status(500).json({
      ok: false,
      message: "No se pudo eliminar el ítem.",
    });
  }
}
