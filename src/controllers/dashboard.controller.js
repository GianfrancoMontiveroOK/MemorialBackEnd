// src/controllers/dashboard.controller.js
import mongoose from "mongoose";
import User from "../models/user.model.js";
import Cliente from "../models/client.model.js";
import Transaction from "../models/transaction.model.js";

/**
 * Extrae credenciales del request (soporta JWT -> req.user y session -> req.session.user)
 */
function getAuthFromRequest(req) {
  const fromUser = req.user || {};
  const fromSession = (req.session && req.session.user) || {};

  const role = fromUser.role ?? fromSession.role ?? null;
  const rawId = fromUser._id ?? fromSession.id ?? null;
  const _id = rawId ? new mongoose.Types.ObjectId(rawId) : null;

  return { role, _id };
}

function normRole(role) {
  return String(role || "").toLowerCase(); // "superadmin" | "admin" | "cobrador"
}

const paymentFilter = {
  type: "payment",
  status: "completed",
};

/**
 * Controlador principal del dashboard.
 */
export const getDashboard = async (req, res) => {
  try {
    const { role, _id } = getAuthFromRequest(req);
    if (!role || !_id) {
      return res
        .status(401)
        .json({ message: "Sesión inválida o usuario no autenticado" });
    }

    const roleKey = normRole(role);
    let data = {};

    // 🔎 parámetros comunes de paginación/búsqueda
    const page = Math.max(1, Number(req.query.page) || 1);
    const limit = Math.min(Number(req.query.limit) || 10, 100);
    const sortBy = req.query.sortBy || "createdAt";
    const sortDir = req.query.sortDir === "asc" ? 1 : -1;
    const q = req.query.q?.trim() || "";

    // 🔎 filtro de búsqueda coherente con tu modelo
    const filter = q
      ? {
          $or: [
            { nombre: new RegExp(q, "i") },
            { idCliente: isNaN(Number(q)) ? undefined : Number(q) }, // si q es numérico busca por idCliente
            { domicilio: new RegExp(q, "i") },
            { idCobrador: isNaN(Number(q)) ? undefined : Number(q) },
          ].filter(Boolean),
        }
      : {};

    switch (roleKey) {
      case "superadmin": {
        const [
          totalUsuarios,
          totalClientes,
          totalPagos,
          ultimosPagos,
          ultimosUsuarios,
          clientes,
        ] = await Promise.all([
          User.countDocuments(),
          Cliente.countDocuments(),
          Transaction.countDocuments(paymentFilter),
          Transaction.find(paymentFilter)
            .sort({ effectiveAt: -1, createdAt: -1, _id: -1 })
            .limit(10)
            .lean(),
          User.find().sort({ createdAt: -1 }).limit(10).lean(),
          Cliente.find(filter)
            .sort({ [sortBy]: sortDir })
            .skip((page - 1) * limit)
            .limit(limit)
            .select(
              "idCliente nombre domicilio ciudad provincia cp telefono documento docTipo edad idCobrador cuota plan parcela activo ingreso baja createdAt"
            )
            .lean(),
        ]);

        data = {
          resumen: {
            usuarios: totalUsuarios,
            clientes: totalClientes,
            pagos: totalPagos,
          },
          recientes: { ultimosPagos, ultimosUsuarios },
          clientes: {
            items: clientes,
            total: totalClientes,
            page,
            limit,
          },
        };
        break;
      }

      case "admin": {
        const [clientesCount, pagosMensuales, clientes] = await Promise.all([
          Cliente.countDocuments(),
          Transaction.aggregate([
            { $match: paymentFilter },
            {
              $group: {
                _id: {
                  $dateToString: {
                    format: "%Y-%m",
                    date: { $ifNull: ["$effectiveAt", "$createdAt"] },
                  },
                },
                total: { $sum: { $ifNull: ["$amount", 0] } },
                cantidad: { $sum: 1 },
              },
            },
            { $sort: { _id: 1 } },
          ]),
          Cliente.find(filter)
            .sort({ [sortBy]: sortDir })
            .skip((page - 1) * limit)
            .limit(limit)
            .select(
              "idCliente nombre domicilio ciudad provincia cp telefono documento docTipo edad idCobrador cuota plan parcela activo ingreso baja createdAt"
            )
            .lean(),
        ]);

        data = {
          clientesCount,
          pagosMensuales,
          clientes: {
            items: clientes,
            total: clientesCount,
            page,
            limit,
          },
        };
        break;
      }

      case "cobrador": {
        const pagosPropios = await Transaction.find({
          ...paymentFilter,
          createdBy: _id,
        })
          .sort({ effectiveAt: -1, createdAt: -1, _id: -1 })
          .lean();

        const totalCobrado = pagosPropios.reduce(
          (acc, t) =>
            acc +
            (typeof t.amount === "number" ? t.amount : Number(t.amount) || 0),
          0
        );

        data = { totalCobrado, pagos: pagosPropios };
        break;
      }

      default:
        return res.status(403).json({ message: "Rol no autorizado" });
    }

    return res.json({ role, data });
  } catch (error) {
    console.error("Dashboard error:", error);
    return res.status(500).json({ message: error.message || "Error interno" });
  }
};
