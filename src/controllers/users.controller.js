import mongoose from "mongoose";
import User from "../models/user.model.js";

/** Utils */
const isObjectId = (v) => mongoose.Types.ObjectId.isValid(String(v || ""));
const toObjectId = (v) =>
  isObjectId(v) ? new mongoose.Types.ObjectId(v) : null;

/** Campos seguros a retornar (tu toJSON ya limpia, igual acotamos) */
const SAFE_SELECT =
  "email name role emailVerified idCobrador idVendedor createdAt updatedAt";

/** Normaliza un rol a la forma EXACTA del enum del modelo */
const ALLOWED_ROLES = [
  "superAdmin",
  "admin",
  "user",
  "client",
  "cobrador",
  "vendedor",
];
function normalizeRole(input) {
  if (!input) return null;
  const low = String(input).toLowerCase();
  const found = ALLOWED_ROLES.find((r) => r.toLowerCase() === low);
  return found || null;
}

/** Construye filtro con { q, id, email } */
function buildFilter({ q, id, email }) {
  const filter = {};
  const or = [];

  if (id && isObjectId(id)) filter._id = toObjectId(id);

  if (email) filter.email = String(email).trim().toLowerCase();

  if (q) {
    const rx = new RegExp(String(q).trim(), "i");
    or.push({ name: rx }, { email: rx });
  }
  if (or.length) filter.$or = or;

  return filter;
}

/** GET /users  -> listado + búsqueda */
export const listUsers = async (req, res) => {
  try {
    const page = Math.max(1, Number(req.query.page) || 1);
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const sortBy = req.query.sortBy || "createdAt";
    const sortDir = req.query.sortDir === "asc" ? 1 : -1;

    const q = req.query.q?.trim() || "";
    const id = req.query.id?.trim() || "";
    const email = req.query.email?.trim() || "";

    const filter = buildFilter({ q, id, email });

    const [items, total] = await Promise.all([
      User.find(filter)
        .sort({ [sortBy]: sortDir, _id: -1 })
        .skip((page - 1) * limit)
        .limit(limit)
        .select(SAFE_SELECT)
        .lean(),
      User.countDocuments(filter),
    ]);

    return res.json({ ok: true, items, total, page, limit });
  } catch (err) {
    console.error("listUsers error:", err);
    return res
      .status(500)
      .json({ ok: false, message: "Error listando usuarios" });
  }
};

/** GET /users/recent  -> últimos creados */
export const listRecentUsers = async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const items = await User.find({})
      .sort({ createdAt: -1, _id: -1 })
      .limit(limit)
      .select(SAFE_SELECT)
      .lean();

    return res.json({ ok: true, items });
  } catch (err) {
    console.error("listRecentUsers error:", err);
    return res
      .status(500)
      .json({ ok: false, message: "Error listando recientes" });
  }
};

/** GET /users/:id */
export const getUserById = async (req, res) => {
  try {
    const { id } = req.params;
    if (!isObjectId(id))
      return res.status(400).json({ ok: false, message: "ID inválido" });

    const item = await User.findById(id).select(SAFE_SELECT).lean();
    if (!item)
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });

    return res.json({ ok: true, item });
  } catch (err) {
    console.error("getUserById error:", err);
    return res
      .status(500)
      .json({ ok: false, message: "Error obteniendo usuario" });
  }
};

/** PUT /users/:id  -> actualización general (sin password) */
export const updateUser = async (req, res) => {
  try {
    const { id } = req.params;
    if (!isObjectId(id))
      return res.status(400).json({ ok: false, message: "ID inválido" });

    // Campos permitidos según tu modelo actual
    const allowed = ["name", "email", "emailVerified"];
    const payload = {};
    for (const k of allowed) {
      if (req.body[k] !== undefined) payload[k] = req.body[k];
    }
    if (payload.email)
      payload.email = String(payload.email).trim().toLowerCase();

    const updated = await User.findByIdAndUpdate(id, payload, {
      new: true,
      runValidators: true,
    })
      .select(SAFE_SELECT)
      .lean();

    if (!updated)
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });

    return res.json({ ok: true, item: updated });
  } catch (err) {
    console.error("updateUser error:", err);
    if (err?.code === 11000 && err?.keyPattern?.email) {
      return res
        .status(409)
        .json({ ok: false, message: "Email ya registrado" });
    }
    return res
      .status(500)
      .json({ ok: false, message: "Error actualizando usuario" });
  }
};

/** PATCH /users/:id/role -> { role } (enum: superAdmin|admin|user|client) */
export const setUserRole = async (req, res) => {
  try {
    const { id } = req.params;
    const rawRole = req.body?.role;

    if (!isObjectId(id))
      return res.status(400).json({ ok: false, message: "ID inválido" });

    const role = normalizeRole(rawRole);
    if (!role)
      return res.status(400).json({ ok: false, message: "Rol inválido" });

    const updated = await User.findByIdAndUpdate(
      id,
      { role },
      { new: true, runValidators: true }
    )
      .select(SAFE_SELECT)
      .lean();

    if (!updated)
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });

    return res.json({ ok: true, item: updated });
  } catch (err) {
    console.error("setUserRole error:", err);
    return res.status(500).json({ ok: false, message: "Error cambiando rol" });
  }
};

// PATCH /users/:id/cobrador
export const setUserCobrador = async (req, res, next) => {
  try {
    const { id } = req.params;
    const { idCobrador } = req.body;

    const isString = typeof idCobrador === "string";
    const isNullish = idCobrador === null || idCobrador === undefined;
    if (!isString && !isNullish) {
      return res.status(400).json({
        ok: false,
        message: "idCobrador debe ser string, null o estar ausente.",
      });
    }

    const update =
      isNullish || idCobrador === ""
        ? { $unset: { idCobrador: "" } }
        : { $set: { idCobrador: idCobrador.trim() } };

    const updated = await User.findByIdAndUpdate(id, update, { new: true })
      .select(SAFE_SELECT)
      .lean();
    if (!updated)
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });

    return res.json({ ok: true, item: updated }); // <- unificado
  } catch (err) {
    next(err);
  }
};

// PATCH /users/:id/vendedor
export const setUserVendedor = async (req, res, next) => {
  try {
    const { id } = req.params;
    const { idVendedor } = req.body;

    const isString = typeof idVendedor === "string";
    const isNullish = idVendedor === null || idVendedor === undefined;
    if (!isString && !isNullish) {
      return res.status(400).json({
        ok: false,
        message: "idVendedor debe ser string, null o estar ausente.",
      });
    }

    const update =
      isNullish || idVendedor === ""
        ? { $unset: { idVendedor: "" } }
        : { $set: { idVendedor: idVendedor.trim() } };

    const updated = await User.findByIdAndUpdate(id, update, { new: true })
      .select(SAFE_SELECT)
      .lean();
    if (!updated)
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });

    return res.json({ ok: true, item: updated }); // <- unificado
  } catch (err) {
    next(err);
  }
};
