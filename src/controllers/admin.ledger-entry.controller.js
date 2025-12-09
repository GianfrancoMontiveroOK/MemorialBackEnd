// controllers/admin.ledger-entries.controller.js
import mongoose from "mongoose";
import LedgerEntry from "../models/ledger-entry.model.js";
import Payment from "../models/payment.model.js";
import User from "../models/user.model.js";
import Cliente from "../models/client.model.js";

const toInt = (v, d = undefined) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const toFloat = (v) => {
  if (v === "" || v == null) return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
};
const toBool = (v, def = false) => {
  if (typeof v === "boolean") return v;
  const s = String(v).toLowerCase();
  if (["1", "true", "yes"].includes(s)) return true;
  if (["0", "false", "no"].includes(s)) return false;
  return def;
};
const isObjectIdLike = (s) => {
  try {
    new mongoose.Types.ObjectId(s);
    return true;
  } catch {
    return false;
  }
};

const SORT_ALLOWLIST = new Set([
  "postedAt",
  "amount",
  "side",
  "accountCode",
  "dimensions.idCobrador",
  "dimensions.idCliente",
  "createdAt",
  // para FE
  "fromUserName",
  "toUserName",
]);


export async function listAdminLedgerEntries(req, res) {
  try {
    const {
      q = "",
      dateFrom,
      dateTo,
      side,
      account,
      accountCode,
      currency,
      idCobrador,
      idCliente,
      minAmount,
      maxAmount,
      method,
      status,
      userId,
      includePayment = "0",
      sortBy = "postedAt",
      sortDir = "desc",
      page = 1,
      pageSize = 25,
      limit,
    } = req.query;

    const _page = Math.max(Number(page) || 1, 1);
    const _limit = Math.min(Number(pageSize ?? limit ?? 25) || 25, 200);
    const _skip = (_page - 1) * _limit;

    const matchAnd = [];

    // rango de fechas
    if (dateFrom) {
      const d = new Date(dateFrom);
      if (!Number.isNaN(d.getTime())) matchAnd.push({ postedAt: { $gte: d } });
    }
    if (dateTo) {
      const e = new Date(dateTo);
      if (!Number.isNaN(e.getTime())) {
        if (
          e.getHours() +
            e.getMinutes() +
            e.getSeconds() +
            e.getMilliseconds() ===
          0
        ) {
          e.setHours(23, 59, 59, 999);
        }
        matchAnd.push({ postedAt: { $lte: e } });
      }
    }

    // filtros básicos
    if (side === "debit" || side === "credit") matchAnd.push({ side });
    const _accountCode = accountCode || account;
    if (_accountCode) matchAnd.push({ accountCode: _accountCode });
    if (currency) matchAnd.push({ currency });

    const _idCob = toInt(idCobrador);
    if (Number.isFinite(_idCob))
      matchAnd.push({ "dimensions.idCobrador": _idCob });

    const _idCli = toInt(idCliente);
    if (Number.isFinite(_idCli))
      matchAnd.push({ "dimensions.idCliente": _idCli });

    const _min = toFloat(minAmount);
    if (_min !== undefined) matchAnd.push({ amount: { $gte: _min } });
    const _max = toFloat(maxAmount);
    if (_max !== undefined) {
      const prev = matchAnd.find((m) => m.amount)?.amount || {};
      matchAnd.push({ amount: { ...prev, $lte: _max } });
    }

    if (userId && isObjectIdLike(userId)) {
      matchAnd.push({ userId: new mongoose.Types.ObjectId(userId) });
    }

    // ⛔️ Seguridad por rol del viewer
    const viewerRole = String(req.user?.role || "").trim();
    if (viewerRole === "admin") {
      matchAnd.push({
        $nor: [
          { $and: [{ accountCode: "CAJA_GRANDE" }, { side: "credit" }] },
          { accountCode: "CAJA_SUPERADMIN" },
        ],
      });
    }

    const includePaymentFlag = toBool(includePayment, false);
    const needPaymentJoin = includePaymentFlag || !!method || !!status;

    const baseMatch = matchAnd.length ? { $and: matchAnd } : {};
    const _sortDir = String(sortDir).toLowerCase() === "asc" ? 1 : -1;
    const _sortBy = SORT_ALLOWLIST.has(sortBy) ? sortBy : "postedAt";

    const pipeline = [];
    if (Object.keys(baseMatch).length) pipeline.push({ $match: baseMatch });

    // Lookups
    pipeline.push({
      $lookup: {
        from: User.collection.name,
        localField: "userId",
        foreignField: "_id",
        as: "execUser",
        pipeline: [
          { $project: { _id: 1, name: 1, email: 1, role: 1, idCobrador: 1 } },
        ],
      },
    });

    pipeline.push({
      $lookup: {
        from: User.collection.name,
        let: { colIdStr: { $toString: "$dimensions.idCobrador" } },
        as: "cobradorUser",
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ["$role", "cobrador"] },
                  { $eq: ["$idCobrador", "$$colIdStr"] },
                ],
              },
            },
          },
          { $project: { _id: 1, name: 1, email: 1, idCobrador: 1 } },
          { $limit: 1 },
        ],
      },
    });

    pipeline.push({
      $lookup: {
        from: Cliente.collection.name,
        let: { grpId: "$dimensions.idCliente" },
        as: "titularCliente",
        pipeline: [
          { $match: { $expr: { $eq: ["$idCliente", "$$grpId"] } } },
          { $match: { rol: "TITULAR" } },
          { $project: { _id: 1, nombre: 1, idCliente: 1 } },
          { $limit: 1 },
        ],
      },
    });

    if (needPaymentJoin) {
      pipeline.push({
        $lookup: {
          from: Payment.collection.name,
          localField: "paymentId",
          foreignField: "_id",
          as: "payment",
          pipeline: [
            {
              $project: {
                _id: 1,
                method: 1,
                status: 1,
                currency: 1,
                amount: 1,
                postedAt: 1,
                createdAt: 1,
              },
            },
          ],
        },
      });
      const after = [];
      if (method) after.push({ payment: { $elemMatch: { method } } });
      if (status) after.push({ payment: { $elemMatch: { status } } });
      if (after.length) pipeline.push({ $match: { $and: after } });
    }

    // Nombres base + flags
    pipeline.push({
      $addFields: {
        _adminName: { $ifNull: [{ $first: "$execUser.name" }, "CAJA_ADMIN"] },
        _cobradorName: {
          $ifNull: [
            { $first: "$cobradorUser.name" },
            {
              $cond: [
                {
                  $or: [
                    {
                      $in: [
                        { $type: "$dimensions.idCobrador" },
                        ["missing", "null"],
                      ],
                    },
                    { $eq: ["$dimensions.idCobrador", 0] }, // idCobrador = 0 => sin cobrador
                  ],
                },
                null,
                {
                  $concat: [
                    "Cobrador #",
                    { $toString: "$dimensions.idCobrador" },
                  ],
                },
              ],
            },
          ],
        },

        _clienteName: {
          $ifNull: [
            { $first: "$titularCliente.nombre" },
            {
              $cond: [
                {
                  $in: [
                    { $type: "$dimensions.idCliente" },
                    ["missing", "null"],
                  ],
                },
                null,
                {
                  $concat: [
                    "Cliente #",
                    { $toString: "$dimensions.idCliente" },
                  ],
                },
              ],
            },
          ],
        },
        _hasCliente: {
          $cond: [
            { $in: [{ $type: "$dimensions.idCliente" }, ["missing", "null"]] },
            false,
            true,
          ],
        },
      },
    });

    // Reglas FROM/TO
    const G_CHICA = "CAJA_CHICA (GLOBAL)";
    const G_GRANDE = "CAJA_GRANDE (GLOBAL)";
    const G_SUPERADMIN = "CAJA_SUPERADMIN";

    pipeline.push({
      $addFields: {
        fromUserName: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_ADMIN"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: G_CHICA,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_CHICA"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                then: { $ifNull: ["$_adminName", "CAJA_ADMIN"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_CHICA"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: G_GRANDE,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_GRANDE"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                then: G_CHICA,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_GRANDE"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: G_SUPERADMIN,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_SUPERADMIN"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                then: G_GRANDE,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_COBRADOR"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: { $ifNull: ["$_adminName", "CAJA_ADMIN"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "INGRESOS_CUOTAS"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: { $ifNull: ["$_cobradorName", "$_adminName"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_ADMIN"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                // 👇 Débito de CAJA_ADMIN: del cliente → usuario (fallback cobrador/admin)
                then: {
                  $ifNull: [
                    "$_clienteName",
                    {
                      $ifNull: ["$_cobradorName", "$_adminName"],
                    },
                  ],
                },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_COBRADOR"] },
                    { $eq: ["$side", "debit"] },
                    { $eq: ["$_hasCliente", false] },
                  ],
                },
                then: { $ifNull: ["$_adminName", "CAJA_ADMIN"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_COBRADOR"] },
                    { $eq: ["$side", "debit"] },
                    { $eq: ["$_hasCliente", true] },
                  ],
                },
                then: { $ifNull: ["$_clienteName", "CLIENTE"] },
              },
            ],
            default: {
              $cond: [
                { $eq: ["$side", "debit"] },
                { $ifNull: ["$_adminName", "$_cobradorName"] },
                { $ifNull: ["$_cobradorName", "$_adminName"] },
              ],
            },
          },
        },
        toUserName: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_ADMIN"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: { $ifNull: ["$_adminName", "CAJA_ADMIN"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_CHICA"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                then: G_CHICA,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_CHICA"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: G_CHICA,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_GRANDE"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                then: G_GRANDE,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_GRANDE"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: G_GRANDE,
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_SUPERADMIN"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                then: { $ifNull: ["$_adminName", G_SUPERADMIN] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_COBRADOR"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: { $ifNull: ["$_cobradorName", "COBRADOR"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "INGRESOS_CUOTAS"] },
                    { $eq: ["$side", "credit"] },
                  ],
                },
                then: { $ifNull: ["$_clienteName", "$_adminName"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_ADMIN"] },
                    { $eq: ["$side", "debit"] },
                  ],
                },
                // 👇 acá vuelve a ser simplemente la caja/admin (entra a la caja del usuario)
                then: { $ifNull: ["$_adminName", "CAJA_ADMIN"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_COBRADOR"] },
                    { $eq: ["$side", "debit"] },
                    { $eq: ["$_hasCliente", false] },
                  ],
                },
                then: { $ifNull: ["$_cobradorName", "COBRADOR"] },
              },
              {
                case: {
                  $and: [
                    { $eq: ["$accountCode", "CAJA_COBRADOR"] },
                    { $eq: ["$side", "debit"] },
                    { $eq: ["$_hasCliente", true] },
                  ],
                },
                then: { $ifNull: ["$_cobradorName", "COBRADOR"] },
              },
            ],
            default: {
              $cond: [
                { $eq: ["$side", "debit"] },
                { $ifNull: ["$_cobradorName", "$_adminName"] },
                { $ifNull: ["$_adminName", "$_cobradorName"] },
              ],
            },
          },
        },
      },
    });

    // Búsqueda extendida
    if (q && q.trim()) {
      const re = new RegExp(
        q.trim().replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
        "i"
      );
      pipeline.push({
        $match: {
          $or: [
            { accountCode: re },
            { currency: re },
            { side: re },
            { fromUserName: re },
            { toUserName: re },
            { "dimensions.idCliente": toInt(q) || -999999 },
            { "dimensions.idCobrador": toInt(q) || -999999 },
          ],
        },
      });
    }

    // Proyección final
    const finalProject = {
      _id: 1,
      paymentId: 1,
      kind: 1,
      side: 1,
      accountCode: 1,
      amount: 1,
      currency: 1,
      postedAt: 1,
      createdAt: 1,
      dimensions: 1,
      fromUserName: 1,
      toUserName: 1,
    };
    if (needPaymentJoin) {
      if (includePaymentFlag) {
        finalProject.payment = {
          $cond: [
            { $gt: [{ $size: "$payment" }, 0] },
            { $first: "$payment" },
            null,
          ],
        };
      } else {
        finalProject["payment.method"] = {
          $ifNull: [{ $first: "$payment.method" }, null],
        };
        finalProject["payment.status"] = {
          $ifNull: [{ $first: "$payment.status" }, null],
        };
      }
    }
    pipeline.push({ $project: finalProject });

    // Ejecutar
    const itemsPipeline = [
      ...pipeline,
      { $sort: { [_sortBy]: _sortDir, _id: -1 } },
      { $skip: _skip },
      { $limit: _limit },
    ];
    const countPipeline = [...pipeline, { $count: "total" }];
    const statsPipeline = [
      ...pipeline,
      {
        $group: {
          _id: "$currency",
          lines: { $sum: 1 },
          debit: {
            $sum: { $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0] },
          },
          credit: {
            $sum: { $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0] },
          },
        },
      },
      {
        $project: {
          _id: 0,
          currency: "$_id",
          lines: 1,
          debit: 1,
          credit: 1,
          net: { $subtract: ["$credit", "$debit"] },
        },
      },
      { $sort: { currency: 1 } },
    ];

    const [items, totalArr, statsArr] = await Promise.all([
      LedgerEntry.aggregate(itemsPipeline),
      LedgerEntry.aggregate(countPipeline),
      LedgerEntry.aggregate(statsPipeline),
    ]);

    const total = totalArr?.[0]?.total || 0;
    const stats = {};
    for (const s of statsArr) {
      stats[s.currency || "-"] = {
        debit: s.debit || 0,
        credit: s.credit || 0,
        net: s.net || 0,
        lines: s.lines || 0,
      };
    }

    res.json({
      ok: true,
      page: _page,
      pageSize: _limit,
      total,
      items,
      stats,
      statsByCurrency: statsArr,
    });
  } catch (err) {
    console.error("listAdminLedgerEntries error:", err);
    res.status(500).json({
      ok: false,
      message: "Error al listar el libro mayor.",
      error: err?.message,
    });
  }
}

