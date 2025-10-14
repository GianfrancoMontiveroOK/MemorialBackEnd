// controllers/collector.controller.js
import mongoose from "mongoose";
// 👇 Ajustá la ruta si tu archivo se llama distinto
import Cliente from "../models/client.model.js";

/* -------------------------- Helpers & constants -------------------------- */
const isDigits = (s) => typeof s === "string" && /^\d+$/.test(s);

/** Campos permitidos para ordenar (evita errores y ataques) */
const ALLOWED_SORT_FIELDS = new Set([
  "idCliente",
  "nombre",
  "ciudad",
  "provincia",
  "createdAt",
  "updatedAt",
  "ingreso",
  "cuota",           // histórico/último cobrado
  "cuotaIdeal",      // ✅ nuevo
  "cuotaPisada",     // ✅ nuevo
  "cuotaVigente",    // ✅ nuevo (calculado en pipeline)
]);

/** Normaliza y valida query numérica segura */
const toIntOr = (val, fallback) => {
  const n = Number(val);
  return Number.isInteger(n) ? n : fallback;
};

/** Normaliza sortBy/sortDir con whitelist */
function normalizeSort(sortByRaw, sortDirRaw) {
  const sortBy = (sortByRaw || "idCliente").trim();
  const safeSortBy = ALLOWED_SORT_FIELDS.has(sortBy) ? sortBy : "idCliente";
  const safeSortDir = String(sortDirRaw).toLowerCase() === "asc" ? 1 : -1;
  return { sortBy: safeSortBy, sortDir: safeSortDir };
}

/** Respuesta consistente de error de validación */
function badRequest(res, message, details) {
  return res
    .status(400)
    .json({ ok: false, message, ...(details ? { details } : {}) });
}

/* -------------------------------- Endpoint ------------------------------- */
/**
 * GET /api/collector/clientes
 * Query:
 *  - page (1-based), limit
 *  - q (búsqueda por nombre o idCliente)
 *  - sortBy (default: "idCliente"), sortDir ("asc"|"desc")
 *  - idCobrador (admins/superadmin pueden forzar otro id)
 *
 * Responde: { items, total, page, pageSize, sortBy, sortDir, hasMore, nextPage }
 */
export async function listCollectorClients(req, res, next) {
  try {
    /* --------- Parsing/validación de query con límites razonables -------- */
    const page = Math.max(toIntOr(req.query.page, 1), 1);
    const limit = Math.min(Math.max(toIntOr(req.query.limit, 10), 1), 100);
    const q = (req.query.q || "").trim();

    const { sortBy, sortDir } = normalizeSort(
      req.query.sortBy,
      req.query.sortDir
    );

    /* ---------------- Determinar idCobrador efectivo/permitido ------------- */
    // Middleware previo debe setear req.user = { _id, role, idCobrador? }
    const role = (req.user?.role || "").toLowerCase();
    const userIdCobrador = req.user?.idCobrador;
    const queryIdCobrador = req.query?.idCobrador;

    if (!role) {
      return res.status(401).json({ ok: false, message: "Sesión no válida." });
    }

    let effectiveIdCobrador = null;

    if (role === "cobrador") {
      // Los cobradores solo pueden ver SU cartera
      const n = toIntOr(userIdCobrador, null);
      if (n == null) {
        return badRequest(
          res,
          "El usuario cobrador no tiene idCobrador asociado."
        );
      }
      effectiveIdCobrador = n;
    } else if (role === "admin" || role === "superadmin") {
      // Admin/Superadmin pueden filtrar por un idCobrador puntual o ver todo
      if (queryIdCobrador !== undefined && queryIdCobrador !== "") {
        const n = toIntOr(queryIdCobrador, null);
        if (n == null) {
          return badRequest(res, "idCobrador inválido.", {
            idCobrador: queryIdCobrador,
          });
        }
        effectiveIdCobrador = n;
      }
      // si no se pasa idCobrador como query => listado general (sin filtro)
    } else {
      return res.status(403).json({ ok: false, message: "No autorizado." });
    }

    /* --------------------------- Construcción filtro ----------------------- */
    const filter = {};
    if (effectiveIdCobrador != null) {
      filter.idCobrador = effectiveIdCobrador; // numérico según tu modelo
    }

    if (q) {
      // Evitamos regex costosos/sospechosos escapando el input de usuario
      const safeQ = q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      if (isDigits(q)) {
        // Búsqueda por idCliente exacto + nombre (fallback)
        filter.$or = [
          { idCliente: Number(q) },
          { nombre: { $regex: safeQ, $options: "i" } },
        ];
      } else {
        filter.$or = [{ nombre: { $regex: safeQ, $options: "i" } }];
      }
    }

    /* ------------------- Proyección y collation (opcional) ----------------- */
    const PROJECTION = {
      _id: 1,
      idCliente: 1,
      nombre: 1,
      domicilio: 1,
      ciudad: 1,
      provincia: 1,
      telefono: 1,
      documento: 1,
      idCobrador: 1,
      ingreso: 1,
      createdAt: 1,
      updatedAt: 1,
      // históricos
      cuota: 1,
      // 🔎 nuevos para pricing
      cuotaIdeal: 1,
      cuotaPisada: 1,
      usarCuotaPisada: 1,
      // cuotaVigente la agregamos con $addFields
    };

    const collation = { locale: "es", strength: 1 };

    /* ------------------------------- Querying ------------------------------ */
    // Orden estable con fallback por _id
    const sortStage =
      sortBy === "createdAt"
        ? { createdAtSafe: sortDir, _id: sortDir }
        : { [sortBy]: sortDir, _id: sortDir };

    const pipeline = [
      { $match: filter },
      {
        $addFields: {
          createdAtSafe: { $ifNull: ["$createdAt", { $toDate: "$_id" }] },
          // ✅ cuota vigente según pisada/ideal
          cuotaVigente: {
            $cond: [
              { $and: [{ $eq: ["$usarCuotaPisada", true] }, { $ne: ["$cuotaPisada", null] }] },
              "$cuotaPisada",
              "$cuotaIdeal",
            ],
          },
        },
      },
      { $sort: sortStage },
      { $skip: (page - 1) * limit },
      { $limit: limit },
      { $project: { ...PROJECTION, cuotaVigente: 1 } },
    ];

    const [itemsRaw, total] = await Promise.all([
      Cliente.aggregate(pipeline).collation(collation),
      Cliente.countDocuments(filter),
    ]);

    // Aseguramos 'id' para DataGrid/Tablas
    const items = itemsRaw.map((d) => ({
      id: String(d._id),
      ...d,
    }));

    const hasMore = page * limit < total;
    const nextPage = hasMore ? page + 1 : null;

    return res.status(200).json({
      items,
      total,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDir === 1 ? "asc" : "desc",
      hasMore,
      nextPage,
    });
  } catch (err) {
    if (err instanceof mongoose.Error.CastError) {
      return badRequest(res, "Parámetro con formato inválido.", {
        path: err.path,
        value: err.value,
        kind: err.kind,
      });
    }
    return next(err);
  }
}

/* ----------------------------- Placeholder pago ---------------------------- */
export async function createCollectorPayment(_req, res) {
  return res
    .status(501)
    .json({ ok: false, message: "Registrar pago no implementado aún." });
}
