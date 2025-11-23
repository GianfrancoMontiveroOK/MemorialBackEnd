import mongoose from "mongoose";
import User from "../models/user.model.js";

/** Utils */
const isObjectId = (v) => mongoose.Types.ObjectId.isValid(String(v || ""));
const toObjectId = (v) =>
  isObjectId(v) ? new mongoose.Types.ObjectId(v) : null;

/** Campos seguros a retornar (tu toJSON ya limpia, igual acotamos) */
const SAFE_SELECT =
  "email name role emailVerified idCobrador idVendedor createdAt updatedAt porcentajeCobrador commissionGraceDays commissionPenaltyPerDay";

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

    // allowlist simple para evitar campos raros
    const SORT_ALLOWLIST = new Set([
      "createdAt",
      "updatedAt",
      "name",
      "email",
      "role",
      "porcentajeCobrador", // ⬅️ ahora también podés ordenar por esto si querés
    ]);
    const _sortBy = SORT_ALLOWLIST.has(req.query.sortBy)
      ? req.query.sortBy
      : "createdAt";
    const _sortDir = req.query.sortDir === "asc" ? 1 : -1;

    const q = req.query.q?.trim() || "";
    const id = req.query.id?.trim() || "";
    const email = req.query.email?.trim() || "";

    // Filtro base (tu helper actual)
    const baseFilter = buildFilter({ q, id, email }) || {};

    // 🔐 Guardia por rol del viewer
    const viewerRole = String(req.user?.role || "").trim();
    const guards = [];
    if (viewerRole === "admin") {
      // Admin NO ve superAdmins
      guards.push({ role: { $ne: "superAdmin" } });
    }
    // if (viewerRole === "cobrador") { guards.push({ role: { $in: ["cobrador"] } }); } // opcional

    const finalFilter =
      guards.length > 0 ? { $and: [baseFilter, ...guards] } : baseFilter;

    // ⬇️ armamos el select agregando los campos nuevos de forma segura
    const extraFields = {
      idCobrador: 1,
      idVendedor: 1,
      porcentajeCobrador: 1,
      commissionGraceDays: 1,
      commissionPenaltyPerDay: 1,
    };

    const [items, total] = await Promise.all([
      User.find(finalFilter)
        .sort({ [_sortBy]: _sortDir, _id: -1 })
        .skip((page - 1) * limit)
        .limit(limit)
        .select(
          typeof SAFE_SELECT === "string"
            ? SAFE_SELECT +
                " idCobrador idVendedor porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
            : { ...SAFE_SELECT, ...extraFields }
        )
        .lean(),
      User.countDocuments(finalFilter),
    ]);

    return res.json({
      ok: true,
      items,
      total,
      page,
      limit,
      sortBy: _sortBy,
      sortDir: _sortDir === 1 ? "asc" : "desc",
    });
  } catch (err) {
    console.error("listUsers error:", err);
    return res
      .status(500)
      .json({ ok: false, message: "Error listando usuarios" });
  }
};

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

export const setUserRole = async (req, res) => {
  try {
    const { id } = req.params;
    const rawRole = req.body?.role;

    if (!isObjectId(id)) {
      return res.status(400).json({ ok: false, message: "ID inválido" });
    }

    const targetId = new mongoose.Types.ObjectId(String(id));
    const viewer = req.user || {};
    const viewerId = viewer?._id
      ? new mongoose.Types.ObjectId(String(viewer._id))
      : null;
    const viewerRole = String(viewer?.role || "").trim();

    const role = normalizeRole(rawRole);
    if (!role) {
      return res.status(400).json({ ok: false, message: "Rol inválido" });
    }

    // --- Cargar usuario destino (para validar reglas por rol)
    const target = await User.findById(targetId).select("_id role").lean();
    if (!target) {
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });
    }

    // --- Guardias por rol del viewer
    if (viewerRole === "admin") {
      // 1) no puede modificarse a sí mismo
      if (viewerId && viewerId.equals(targetId)) {
        return res
          .status(403)
          .json({ ok: false, message: "No podés cambiar tu propio rol." });
      }

      // 2) no puede tocar admins ni superAdmins
      if (["admin", "superAdmin"].includes(String(target.role))) {
        return res.status(403).json({
          ok: false,
          message: "No tenés permiso para modificar este usuario.",
        });
      }

      // 3) solo puede asignar roles operativos
      const ALLOWED_BY_ADMIN = new Set(["cobrador", "vendedor"]);
      if (!ALLOWED_BY_ADMIN.has(role)) {
        return res.status(403).json({
          ok: false,
          message: "Solo podés asignar los roles: cobrador o vendedor.",
        });
      }
    } else if (viewerRole !== "superAdmin") {
      // otros roles no pueden cambiar roles
      return res
        .status(403)
        .json({ ok: false, message: "Sin permisos para cambiar roles." });
    }

    const updated = await User.findByIdAndUpdate(
      targetId,
      { role },
      { new: true, runValidators: true }
    )
      .select(SAFE_SELECT)
      .lean();

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

    if (!isObjectId(id)) {
      return res.status(400).json({ ok: false, message: "ID inválido" });
    }

    const viewer = req.user || {};
    const viewerId = viewer?._id
      ? new mongoose.Types.ObjectId(String(viewer._id))
      : null;
    const viewerRole = String(viewer?.role || "").trim();

    // Validación de tipo (string o null/undefined/"")
    const isString = typeof idCobrador === "string";
    const isNullish =
      idCobrador === null || idCobrador === undefined || idCobrador === "";
    if (!isString && !isNullish) {
      return res.status(400).json({
        ok: false,
        message: "idCobrador debe ser string o null/vacío.",
      });
    }

    // Cargar usuario destino
    const targetId = new mongoose.Types.ObjectId(String(id));
    const target = await User.findById(targetId).select("_id role").lean();
    if (!target) {
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });
    }

    // Guardias por rol
    if (viewerRole === "admin") {
      if (viewerId && viewerId.equals(targetId)) {
        return res
          .status(403)
          .json({ ok: false, message: "No podés modificarte a vos mismo." });
      }
      if (["admin", "superAdmin"].includes(String(target.role))) {
        return res.status(403).json({
          ok: false,
          message: "No tenés permiso para modificar este usuario.",
        });
      }
      if (String(target.role) !== "cobrador") {
        return res.status(403).json({
          ok: false,
          message: "Solo podés editar datos de cobradores.",
        });
      }
    } else if (viewerRole !== "superAdmin") {
      return res.status(403).json({
        ok: false,
        message: "Sin permisos para modificar cobradores.",
      });
    }

    const update = isNullish
      ? { $unset: { idCobrador: "" } }
      : { $set: { idCobrador: idCobrador.trim() } };

    const updated = await User.findByIdAndUpdate(targetId, update, {
      new: true,
    })
      .select(SAFE_SELECT)
      .lean();

    return res.json({ ok: true, item: updated });
  } catch (err) {
    next(err);
  }
};

// PATCH /users/:id/vendedor
export const setUserVendedor = async (req, res, next) => {
  try {
    const { id } = req.params;
    const { idVendedor } = req.body;

    if (!isObjectId(id)) {
      return res.status(400).json({ ok: false, message: "ID inválido" });
    }

    const viewer = req.user || {};
    const viewerId = viewer?._id
      ? new mongoose.Types.ObjectId(String(viewer._id))
      : null;
    const viewerRole = String(viewer?.role || "").trim();

    // Validación de tipo (string o null/undefined/"")
    const isString = typeof idVendedor === "string";
    const isNullish =
      idVendedor === null || idVendedor === undefined || idVendedor === "";
    if (!isString && !isNullish) {
      return res.status(400).json({
        ok: false,
        message: "idVendedor debe ser string o null/vacío.",
      });
    }

    // Cargar usuario destino
    const targetId = new mongoose.Types.ObjectId(String(id));
    const target = await User.findById(targetId).select("_id role").lean();
    if (!target) {
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado" });
    }

    // Guardias por rol
    if (viewerRole === "admin") {
      if (viewerId && viewerId.equals(targetId)) {
        return res
          .status(403)
          .json({ ok: false, message: "No podés modificarte a vos mismo." });
      }
      if (["admin", "superAdmin"].includes(String(target.role))) {
        return res.status(403).json({
          ok: false,
          message: "No tenés permiso para modificar este usuario.",
        });
      }
      if (String(target.role) !== "vendedor") {
        return res.status(403).json({
          ok: false,
          message: "Solo podés editar datos de vendedores.",
        });
      }
    } else if (viewerRole !== "superAdmin") {
      return res.status(403).json({
        ok: false,
        message: "Sin permisos para modificar vendedores.",
      });
    }

    const update = isNullish
      ? { $unset: { idVendedor: "" } }
      : { $set: { idVendedor: idVendedor.trim() } };

    const updated = await User.findByIdAndUpdate(targetId, update, {
      new: true,
    })
      .select(SAFE_SELECT)
      .lean();

    return res.json({ ok: true, item: updated });
  } catch (err) {
    next(err);
  }
};

// ⬇️ COMISIONES DE COBRADOR

// PATCH /users/:id/collector-commission
export const setCollectorCommission = async (req, res) => {
  try {
    const { id } = req.params;

    // Puede venir como collectorCommissionPct (nuevo) o porcentajeCobrador (viejo)
    const { collectorCommissionPct, porcentajeCobrador } = req.body;
    const rawValue =
      collectorCommissionPct !== undefined
        ? collectorCommissionPct
        : porcentajeCobrador;

    // Validación básica de ID (24 chars hex)
    if (!id || !/^[0-9a-fA-F]{24}$/.test(id)) {
      return res
        .status(400)
        .json({ ok: false, message: "ID de usuario inválido." });
    }

    const update = {};

    // Si viene vacío / null / undefined → limpiar campo con $unset
    if (rawValue === "" || rawValue === null || rawValue === undefined) {
      update.$unset = { porcentajeCobrador: 1 };
    } else {
      const n = Number(rawValue);
      if (!Number.isFinite(n)) {
        return res.status(400).json({
          ok: false,
          message: "porcentajeCobrador debe ser un número.",
        });
      }
      if (n < 0 || n > 100) {
        return res.status(400).json({
          ok: false,
          message: "porcentajeCobrador debe estar entre 0 y 100.",
        });
      }
      update.$set = { porcentajeCobrador: n };
    }

    const updated = await User.findByIdAndUpdate(id, update, {
      new: true,
      runValidators: true,
    }).lean();

    if (!updated) {
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado." });
    }

    return res.json({
      ok: true,
      item: updated,
    });
  } catch (err) {
    console.error("setCollectorCommission error:", err);
    return res.status(500).json({
      ok: false,
      message: "Error actualizando la comisión del cobrador.",
    });
  }
};

// PATCH /users/:id/collector-commission-grace-days
export const setCollectorCommissionGraceDays = async (req, res) => {
  try {
    const { id } = req.params;
    const { graceDays } = req.body;

    if (!id || !/^[0-9a-fA-F]{24}$/.test(id)) {
      return res
        .status(400)
        .json({ ok: false, message: "ID de usuario inválido." });
    }

    const update = {};

    if (graceDays === "" || graceDays === null || graceDays === undefined) {
      update.$unset = { commissionGraceDays: 1 };
    } else {
      const n = Number(graceDays);
      if (!Number.isFinite(n) || n < 0) {
        return res.status(400).json({
          ok: false,
          message: "graceDays debe ser un número mayor o igual a 0.",
        });
      }
      update.$set = { commissionGraceDays: Math.round(n) };
    }

    const updated = await User.findByIdAndUpdate(id, update, {
      new: true,
      runValidators: true,
    }).lean();

    if (!updated) {
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado." });
    }

    return res.json({ ok: true, item: updated });
  } catch (err) {
    console.error("setCollectorCommissionGraceDays error:", err);
    return res.status(500).json({
      ok: false,
      message: "Error actualizando los días de gracia del cobrador.",
    });
  }
};

// PATCH /users/:id/collector-commission-penalty
export const setCollectorCommissionPenaltyPerDay = async (req, res) => {
  try {
    const { id } = req.params;
    const { penaltyPerDay } = req.body;

    if (!id || !/^[0-9a-fA-F]{24}$/.test(id)) {
      return res
        .status(400)
        .json({ ok: false, message: "ID de usuario inválido." });
    }

    const update = {};

    if (
      penaltyPerDay === "" ||
      penaltyPerDay === null ||
      penaltyPerDay === undefined
    ) {
      update.$unset = { commissionPenaltyPerDay: 1 };
    } else {
      const n = Number(penaltyPerDay);
      if (!Number.isFinite(n) || n < 0) {
        return res.status(400).json({
          ok: false,
          message: "penaltyPerDay debe ser un número mayor o igual a 0.",
        });
      }
      update.$set = { commissionPenaltyPerDay: n };
    }

    const updated = await User.findByIdAndUpdate(id, update, {
      new: true,
      runValidators: true,
    }).lean();

    if (!updated) {
      return res
        .status(404)
        .json({ ok: false, message: "Usuario no encontrado." });
    }

    return res.json({ ok: true, item: updated });
  } catch (err) {
    console.error("setCollectorCommissionPenaltyPerDay error:", err);
    return res.status(500).json({
      ok: false,
      message: "Error actualizando la penalización diaria del cobrador.",
    });
  }
};
