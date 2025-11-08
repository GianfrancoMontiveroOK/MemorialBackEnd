// src/controllers/dashboard.controller.js

/**
 * Controlador único para /api/dashboard
 * - Requiere: requireSession + ensureUserLoaded (de src/middlewares/roles.js)
 * - No devuelve métricas ni datos pesados: sólo rol y flags.
 * - El rol lo toma de req.user (normalizado por ensureUserLoaded).
 *
 * Respuesta:
 *  { ok: true, role: "superadmin"|"admin"|"cobrador", flags: { isSuperAdmin, isAdmin, isCollector } }
 */
export const checkDashboardAccess = (req, res) => {
  // ensureUserLoaded garantiza req.user poblado si hay sesión válida
  const roleRaw = req.user?.role ?? req.session?.user?.role ?? null;

  if (!roleRaw) {
    return res.status(401).json({ ok: false, message: "No autenticado" });
  }

  // Normalizamos por las dudas (por si en DB quedó con mayúsculas)
  const role = String(roleRaw).toLowerCase();

  // Aceptamos sólo estos 3 (cualquier otro: 403)
  const allowed = new Set(["superadmin", "admin", "cobrador"]);
  if (!allowed.has(role)) {
    return res.status(403).json({ ok: false, message: "Rol no autorizado" });
  }

  // Flags para el frontend (habilitan secciones)
  const flags = {
    isSuperAdmin: role === "superadmin",
    isAdmin: role === "admin" || role === "superadmin",
    isCollector: role === "cobrador" || role === "superadmin",
  };

  return res.json({ ok: true, role, flags });
};
