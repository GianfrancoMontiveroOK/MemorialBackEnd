// src/middlewares/roles.js
import User from "../models/user.model.js";

/* ───────────────────────── Helpers ───────────────────────── */
const httpError = (res, status, message, extra) =>
  res
    .status(status)
    .json({ ok: false, message, ...(extra ? { details: extra } : {}) });

/** Toma el ID del usuario de la sesión (acepta _id o id) y lo normaliza a string */
const getSessionUserId = (req) => {
  const sessUser = req.session?.user;
  return sessUser?._id ?? sessUser?.id ?? null;
};

/** Pone req.user con forma canónica */
const setReqUser = (req, { id, role, idCobrador = null }) => {
  req.user = { _id: String(id), role, idCobrador };
};

/** Cachea en sesión (role y idCobrador si viene) sin romper la forma previa */
const cacheSessionUser = (req, { role, idCobrador }) => {
  if (!req.session.user) req.session.user = {};
  if (role) req.session.user.role = role;
  if (typeof idCobrador !== "undefined")
    req.session.user.idCobrador = idCobrador;
};

/* ────────────────────── Middlewares base ───────────────────── */

/** Verifica que haya sesión iniciada (sin cargar rol) */
export const requireSession = (req, res, next) => {
  const id = getSessionUserId(req);
  if (!id) return httpError(res, 401, "No hay sesión activa");
  next();
};

/**
 * Obtiene y cachea el rol del usuario (y opcionalmente idCobrador),
 * y deja req.user listo. No limita el acceso por rol.
 */
async function ensureRoleCached(req) {
  const id = getSessionUserId(req);
  if (!id) {
    const e = new Error("Sesión inválida: falta user.id");
    e.status = 401;
    throw e;
  }

  const sessUser = req.session.user; // { id/_id, role?, idCobrador? }

  // 1) Si ya viene poblado en req.user (algún middleware anterior), úsalo
  if (req.user?.role) return req.user.role;

  // 2) Si ya hay role cacheado en sesión, úsalo
  if (sessUser.role) {
    setReqUser(req, {
      id,
      role: sessUser.role,
      idCobrador: sessUser.idCobrador,
    });
    return sessUser.role;
  }

  // 3) Lookup en DB (role e idCobrador para no pegar de nuevo luego)
  const user = await User.findById(id).select("role idCobrador").lean();
  if (!user) {
    const e = new Error("Usuario no encontrado");
    e.status = 401;
    throw e;
  }
  if (!user.role) {
    const e = new Error("Rol no asignado");
    e.status = 403;
    throw e;
  }

  // Cachear y poblar
  cacheSessionUser(req, {
    role: user.role,
    idCobrador: user.idCobrador ?? null,
  });
  setReqUser(req, { id, role: user.role, idCobrador: user.idCobrador ?? null });
  return user.role;
}

/** Asegura req.user poblado sin restringir por rol (útil para dashboard, etc.) */
export const ensureUserLoaded = async (req, res, next) => {
  try {
    await ensureRoleCached(req);
    return next();
  } catch (e) {
    const code = e.status || 500;
    return httpError(res, code, e.message || "Error de autorización");
  }
};

/* ─────────────────────── Guards por rol ─────────────────────── */

/** Factor común para crear guards de roles */
const roleGuard = (allowedRoles) => async (req, res, next) => {
  try {
    const role = await ensureRoleCached(req);
    if (!allowedRoles.includes(role)) {
      const needed = allowedRoles.join(" o ");
      return httpError(res, 403, `Se requiere rol ${needed}`);
    }
    return next();
  } catch (e) {
    const code = e.status || 500;
    return httpError(res, code, e.message || "Error de autorización");
  }
};

/** Solo superAdmin */
export const superAdminOnly = roleGuard(["superAdmin"]);

/** Admin o superAdmin */
export const adminOnly = roleGuard(["admin", "superAdmin"]);

/** Cobrador o superior (incluye vendedor según tu definición original) */
export const cobradorOnly = roleGuard([
  "vendedor",
  "cobrador",
  "admin",
  "superAdmin",
]);

/* ────────────── Carga/validación de idCobrador del cobrador ────────────── */
/**
 * Si el usuario es cobrador:
 *  - Garantiza que req.user.idCobrador esté presente (sesión o DB)
 *  - Si no tiene idCobrador, devuelve 400 (para cortar a tiempo)
 * Si NO es cobrador: no hace nada (pasa).
 *
 * Requiere: requireSession + ensureUserLoaded antes.
 */
export const ensureCollectorLoaded = async (req, res, next) => {
  try {
    // Aseguramos req.user poblado
    const role = await ensureRoleCached(req);
    const sessUser = req.session.user;

    if (!sessUser || (!sessUser._id && !sessUser.id)) {
      return httpError(res, 401, "No autenticado");
    }

    if (role !== "cobrador") return next();

    // 1) Si ya está en req.user, ok
    if (
      typeof req.user.idCobrador !== "undefined" &&
      req.user.idCobrador !== null &&
      req.user.idCobrador !== ""
    ) {
      return next();
    }

    // 2) Si está en sesión, úsalo
    if (
      typeof sessUser.idCobrador !== "undefined" &&
      sessUser.idCobrador !== null &&
      sessUser.idCobrador !== ""
    ) {
      req.user.idCobrador = sessUser.idCobrador;
      return next();
    }

    // 3) Buscar en DB y cachear
    const id = getSessionUserId(req);
    const user = await User.findById(id).select("idCobrador").lean();
    if (!user) return httpError(res, 401, "Usuario no encontrado");

    // Normalizamos nulos
    const idCobrador = user.idCobrador ?? null;
    cacheSessionUser(req, { idCobrador });
    req.user.idCobrador = idCobrador;

    if (idCobrador === null || idCobrador === "") {
      // Cortamos aquí para que el controlador no tenga que validar otra vez
      return httpError(
        res,
        400,
        "El usuario cobrador no tiene idCobrador asociado."
      );
    }

    return next();
  } catch (err) {
    return httpError(
      res,
      err.status || 500,
      err.message || "Error cargando datos de cobrador"
    );
  }
};
  