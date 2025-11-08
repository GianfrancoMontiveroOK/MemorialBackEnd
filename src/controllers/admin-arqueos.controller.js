import mongoose from "mongoose";
import User from "../models/user.model.js";
import LedgerEntry from "../models/ledger-entry.model.js";
import Payment from "../models/payment.model.js";
import Client from "../models/client.model.js";

import { onlyDigits, yyyymmAR } from "./collector.shared.js";

// ───────────────────── Helpers ─────────────────────
const toInt = (v, def = 0) => {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
};
const toDir = (v) => (String(v || "").toLowerCase() === "asc" ? 1 : -1);
const parseISODate = (s, endOfDay = false) => {
  if (!s) return null;
  const dt = new Date(`${s}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return null;
  if (endOfDay) dt.setHours(23, 59, 59, 999);
  return dt;
};

// Cuentas
const DEST_DEFAULT = "CAJA_ADMIN";
const DEFAULT_ACCOUNTS = ["CAJA_COBRADOR", "A_RENDIR_COBRADOR"];

// Expr sumas condicionales
const debitExpr = {
  $sum: { $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0] },
};
const creditExpr = {
  $sum: { $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0] },
};

// Roles con caja
const BOX_ROLES = ["cobrador", "admin", "superAdmin"];

// Detecta si un string parece ObjectId
const asObjectId = (s) => {
  try {
    return new mongoose.Types.ObjectId(String(s));
  } catch {
    return null;
  }
};

// 🔧 Match UNIFICADO por usuario y cuentas
function buildUserLedgerMatch({ user, accounts, fromDt, toDt, side }) {
  const uid = new mongoose.Types.ObjectId(String(user._id));
  const byUserId = [
    { userId: uid },
    { "dimensions.fromUserId": uid },
    { "dimensions.toUserId": uid },
  ];

  // Para cobrador, además aceptar idCobrador (string/number)
  const byCollectorId =
    user.role === "cobrador" && user.idCobrador != null
      ? [
          {
            $expr: {
              $eq: [
                { $toString: "$dimensions.idCobrador" },
                { $toString: String(user.idCobrador) },
              ],
            },
          },
          {
            $expr: {
              $eq: [
                { $toString: "$dimensions.cobradorId" },
                { $toString: String(user.idCobrador) },
              ],
            },
          },
        ]
      : [];

  const match = {
    accountCode: { $in: accounts },
    $or: [...byUserId, ...byCollectorId],
  };

  if (side === "debit" || side === "credit") match.side = side;

  if (fromDt || toDt) {
    match.postedAt = {};
    if (fromDt) match.postedAt.$gte = fromDt;
    if (toDt) match.postedAt.$lte = toDt;
  }

  return match;
}

/* ======================= 1) Listado de usuarios (cajas) ======================= */
/**
 * GET /api/admin/arqueos/usuarios
 * Query: q, role, sortBy, sortDir, accountCodes, destAccountCode, dateFrom, dateTo, page, limit
 */
export async function listArqueosUsuarios(req, res, next) {
  try {
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.max(Math.min(toInt(req.query.limit || 25, 25), 200), 1);
    const q = String(req.query.q || "").trim();
    const roleFilter = String(req.query.role || "").trim();
    const sortByParam = String(req.query.sortBy || "totalBalance");
    const sortDirParam = toDir(req.query.sortDir || "desc");

    const viewerRole = String(req.user?.role || "").trim();
    const ALLOWED_BY_VIEWER =
      viewerRole === "superAdmin" ? ["admin", "cobrador"] : ["cobrador"];

    const destAccountCode =
      String(req.query.destAccountCode || DEST_DEFAULT).trim() || DEST_DEFAULT;

    const accountCodesOverride = (() => {
      const arr = String(req.query.accountCodes || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);
      return arr.length ? arr : null;
    })();

    const fromDt = parseISODate(req.query.dateFrom);
    const toDt = parseISODate(req.query.dateTo, true);

    // ---- Filtro de usuarios
    const userMatch = { role: { $in: ALLOWED_BY_VIEWER } };
    if (roleFilter && ALLOWED_BY_VIEWER.includes(roleFilter)) {
      userMatch.role = roleFilter;
    }
    if (q) {
      const oid = asObjectId(q);
      userMatch.$or = [
        { name: { $regex: q, $options: "i" } },
        { email: { $regex: q, $options: "i" } },
        ...(oid ? [{ _id: oid }] : []),
      ];
    }

    // ---- Aggregation
    const base = [
      { $match: userMatch },
      {
        $lookup: {
          from: LedgerEntry.collection.name,
          let: { uid: "$_id", urole: "$role", idCob: "$idCobrador" },
          pipeline: [
            {
              $match: {
                $expr: {
                  $and: [
                    // 1) Cuentas según rol (admin => solo destino)
                    {
                      $in: [
                        "$accountCode",
                        accountCodesOverride
                          ? accountCodesOverride
                          : {
                              $cond: [
                                { $eq: ["$$urole", "admin"] },
                                [destAccountCode],
                                DEFAULT_ACCOUNTS,
                              ],
                            },
                      ],
                    },

                    // 2) VINCULACIÓN POR USUARIO (arreglado):
                    //    - Si es admin: SOLO userId == uid (no dimensions, no idCobrador)
                    //    - Si es cobrador: userId|fromUserId|toUserId == uid  OR  dimensions.idCobrador/cobradorId == idCob
                    {
                      $cond: [
                        { $eq: ["$$urole", "admin"] },
                        {
                          $eq: [
                            { $toString: "$userId" },
                            { $toString: "$$uid" },
                          ],
                        },
                        {
                          $or: [
                            {
                              $eq: [
                                { $toString: "$userId" },
                                { $toString: "$$uid" },
                              ],
                            },
                            {
                              $eq: [
                                { $toString: "$dimensions.fromUserId" },
                                { $toString: "$$uid" },
                              ],
                            },
                            {
                              $eq: [
                                { $toString: "$dimensions.toUserId" },
                                { $toString: "$$uid" },
                              ],
                            },
                            {
                              $and: [
                                { $ne: ["$$idCob", null] },
                                {
                                  $eq: [
                                    { $toString: "$dimensions.idCobrador" },
                                    { $toString: "$$idCob" },
                                  ],
                                },
                              ],
                            },
                            {
                              $and: [
                                { $ne: ["$$idCob", null] },
                                {
                                  $eq: [
                                    { $toString: "$dimensions.cobradorId" },
                                    { $toString: "$$idCob" },
                                  ],
                                },
                              ],
                            },
                          ],
                        },
                      ],
                    },

                    // 3) Rango de fechas (opcional)
                    ...(fromDt || toDt
                      ? [
                          {
                            $and: [
                              fromDt
                                ? { $gte: ["$postedAt", fromDt] }
                                : { $gt: [1, 0] },
                              toDt
                                ? { $lte: ["$postedAt", toDt] }
                                : { $gt: [1, 0] },
                            ],
                          },
                        ]
                      : []),
                  ],
                },
              },
            },
            {
              $group: {
                _id: { currency: "$currency" },
                debits: debitExpr,
                credits: creditExpr,
                lastMovementAt: { $max: "$postedAt" },
                paymentsSet: { $addToSet: "$paymentId" },
              },
            },
            {
              $project: {
                _id: 0,
                currency: "$_id.currency",
                debits: 1,
                credits: 1,
                balance: { $subtract: ["$debits", "$credits"] },
                lastMovementAt: 1,
                paymentsCount: { $size: "$paymentsSet" },
              },
            },
          ],
          as: "boxes",
        },
      },
      {
        $addFields: {
          totalBalance: {
            $sum: { $map: { input: "$boxes", as: "b", in: "$$b.balance" } },
          },
          lastMovementAt: { $max: "$boxes.lastMovementAt" },
          paymentsCount: {
            $sum: {
              $map: { input: "$boxes", as: "b", in: "$$b.paymentsCount" },
            },
          },
        },
      },
      {
        $project: {
          _id: 1,
          name: 1,
          email: 1,
          role: 1,
          idCobrador: 1,
          boxes: 1,
          totalBalance: 1,
          lastMovementAt: 1,
          paymentsCount: 1,
        },
      },
    ];

    const SORTABLE = new Set([
      "name",
      "totalBalance",
      "lastMovementAt",
      "paymentsCount",
      "email",
      "role",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "totalBalance";
    const sortStage = { $sort: { [sortBy]: sortDirParam, _id: sortDirParam } };

    const dataPipeline = [
      ...base,
      sortStage,
      { $skip: (page - 1) * limit },
      { $limit: limit },
    ];
    const countPipeline = [{ $match: userMatch }, { $count: "n" }];

    const [items, countRes] = await Promise.all([
      User.aggregate(dataPipeline).allowDiskUse(true),
      User.aggregate(countPipeline).allowDiskUse(true),
    ]);

    const total = countRes?.[0]?.n || 0;

    return res.json({
      ok: true,
      items,
      total,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
      filters: {
        viewerRole,
        rolesListed: ALLOWED_BY_VIEWER,
        role:
          roleFilter && ALLOWED_BY_VIEWER.includes(roleFilter)
            ? roleFilter
            : null,
        accountCodes: accountCodesOverride || {
          admin: [destAccountCode],
          cobrador: DEFAULT_ACCOUNTS,
        },
        dateFrom: req.query.dateFrom || null,
        dateTo: req.query.dateTo || null,
        q,
        destAccountCode,
      },
    });
  } catch (err) {
    next(err);
  }
}

/* ======================= 2) Detalle de movimientos ======================= */
/**
 * GET /api/admin/arqueos/usuarios/detalle
 */
export async function getArqueoUsuarioDetalle(req, res, next) {
  try {
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.max(Math.min(toInt(req.query.limit || 25, 25), 200), 1);
    const sortByParam = String(req.query.sortBy || "postedAt");
    const sortDirParam = toDir(req.query.sortDir || "desc");

    const accountCodesOverride = String(req.query.accountCodes || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const destAccountCode =
      String(req.query.destAccountCode || "").trim() || DEST_DEFAULT;
    const fromDt = parseISODate(req.query.dateFrom);
    const toDt = parseISODate(req.query.dateTo, true);
    const side = String(req.query.side || "");

    // Resolver usuario
    let user = null;
    if (req.query.userId) {
      user = await User.findById(req.query.userId)
        .select("_id name email role idCobrador")
        .lean();
    } else if (req.query.idCobrador != null) {
      user =
        (await User.findOne({ idCobrador: String(req.query.idCobrador) })
          .select("_id name email role idCobrador")
          .lean()) ||
        (await User.findOne({ idCobrador: Number(req.query.idCobrador) })
          .select("_id name email role idCobrador")
          .lean());
    }
    if (!user)
      return res
        .status(400)
        .json({ ok: false, message: "Usuario no encontrado" });
    if (!BOX_ROLES.includes(user.role))
      return res
        .status(403)
        .json({ ok: false, message: "Rol sin caja habilitada" });

    const effectiveAccounts =
      accountCodesOverride.length > 0
        ? accountCodesOverride
        : user.role === "admin"
        ? [destAccountCode]
        : DEFAULT_ACCOUNTS;

    const match = buildUserLedgerMatch({
      user,
      accounts: effectiveAccounts,
      fromDt,
      toDt,
      side,
    });

    const base = [{ $match: match }];

    const totalsPipeline = [
      ...base,
      {
        $group: {
          _id: null,
          debits: debitExpr,
          credits: creditExpr,
          lastMovementAt: { $max: "$postedAt" },
          paymentsSet: { $addToSet: "$paymentId" },
        },
      },
      {
        $project: {
          _id: 0,
          debits: 1,
          credits: 1,
          balance: { $subtract: ["$debits", "$credits"] },
          lastMovementAt: 1,
          paymentsCount: { $size: "$paymentsSet" },
        },
      },
    ];

    const SORTABLE = new Set([
      "postedAt",
      "amount",
      "accountCode",
      "side",
      "currency",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "postedAt";
    const sortStage = { $sort: { [sortBy]: sortDirParam, _id: sortDirParam } };

    const itemsPipeline = [
      ...base,
      {
        $project: {
          _id: 1,
          paymentId: 1,
          side: 1,
          accountCode: 1,
          amount: 1,
          currency: 1,
          postedAt: 1,
          dimensions: 1,
          createdAt: 1,
        },
      },
      sortStage,
      { $skip: (page - 1) * limit },
      { $limit: limit },
    ];

    const countPipeline = [...base, { $count: "n" }];

    const [totalsArr, items, countRes] = await Promise.all([
      LedgerEntry.aggregate(totalsPipeline).allowDiskUse(true),
      LedgerEntry.aggregate(itemsPipeline).allowDiskUse(true),
      LedgerEntry.aggregate(countPipeline).allowDiskUse(true),
    ]);

    const totals = totalsArr?.[0] || {
      debits: 0,
      credits: 0,
      balance: 0,
      lastMovementAt: null,
      paymentsCount: 0,
    };
    const total = countRes?.[0]?.n || 0;

    return res.json({
      ok: true,
      header: { user, idCobrador: user.idCobrador ?? null, totals },
      items,
      total,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
      filters: {
        accountCodes: effectiveAccounts,
        dateFrom: req.query.dateFrom || null,
        dateTo: req.query.dateTo || null,
        side: side || null,
        destAccountCode,
      },
    });
  } catch (err) {
    next(err);
  }
}

/* ======================= 3) Crear arqueo (mueve fondos) ======================= */
/**
 * POST /api/admin/arqueos/usuarios/arqueo
 * Body: { userId | idCobrador, note, accountCodes?, dateFrom?, dateTo?, destAccountCode?, minAmount? }
 */
export async function crearArqueoUsuario(req, res, next) {
  try {
    const {
      userId: bodyUserId,
      idCobrador,
      note,
      accountCodes,
      dateFrom: df,
      dateTo: dt,
      destAccountCode,
      minAmount = 1,
    } = req.body || {};

    // 1) Cobrador origen
    let cobradorUser = null;
    if (bodyUserId) {
      cobradorUser = await User.findById(bodyUserId)
        .select("_id name email role idCobrador")
        .lean();
    } else if (idCobrador != null) {
      cobradorUser =
        (await User.findOne({ idCobrador: String(idCobrador) })
          .select("_id name email role idCobrador")
          .lean()) ||
        (await User.findOne({ idCobrador: Number(idCobrador) })
          .select("_id name email role idCobrador")
          .lean());
    }
    if (!cobradorUser)
      return res
        .status(400)
        .json({ ok: false, message: "Usuario cobrador no encontrado" });
    if (!BOX_ROLES.includes(cobradorUser.role))
      return res
        .status(403)
        .json({ ok: false, message: "Rol de cobrador sin caja habilitada" });

    // 2) Admin ejecutor
    const adminUserId = req.user?._id
      ? new mongoose.Types.ObjectId(String(req.user._id))
      : null;
    if (!adminUserId)
      return res
        .status(403)
        .json({ ok: false, message: "Sesión admin requerida" });

    // 3) Cuentas y ventana
    const accts =
      String(accountCodes || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean).length > 0
        ? String(accountCodes)
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean)
        : DEFAULT_ACCOUNTS;

    const destAcct = (destAccountCode || DEST_DEFAULT).trim() || DEST_DEFAULT;
    if (destAcct === "CAJA_COBRADOR") {
      return res
        .status(400)
        .json({ ok: false, message: "Cuenta destino inválida" });
    }

    const fromDt = parseISODate(df);
    const toDt = parseISODate(dt, true);

    // 4) Saldo por moneda del cobrador usando el MATCH UNIFICADO (coherente con listado/detalle)
    const matchCollector = buildUserLedgerMatch({
      user: cobradorUser,
      accounts: accts,
      fromDt,
      toDt,
      side: undefined,
    });

    const byCurrency = await LedgerEntry.aggregate([
      { $match: matchCollector },
      {
        $group: {
          _id: { currency: "$currency" },
          debits: debitExpr,
          credits: creditExpr,
        },
      },
      {
        $project: {
          _id: 0,
          currency: "$_id.currency",
          balance: { $subtract: ["$debits", "$credits"] },
        },
      },
    ]).allowDiskUse(true);

    const positives = byCurrency
      .map((r) => ({
        currency: r.currency || "ARS",
        balance: Number(r.balance || 0),
      }))
      .filter((r) => r.balance > 0 && r.balance >= Number(minAmount || 0));

    const totalPos = positives.reduce((a, r) => a + r.balance, 0);

    if (!positives.length || totalPos <= 0) {
      return res.status(409).json({
        ok: false,
        message:
          "No hay saldo positivo para transferir desde la caja del cobrador.",
        details: { perCurrency: byCurrency },
      });
    }

    // 5) Asientos (idempotentes) — ¡userId correcto por línea!
    const now = new Date();
    const created = [];

    for (const row of positives) {
      const { currency, balance } = row;

      const idemKey = `arqueo:${String(cobradorUser._id)}:${String(
        adminUserId
      )}:${destAcct}:${currency}:${Math.floor(now.getTime() / 60000)}`;

      const exists = await LedgerEntry.findOne({
        "dimensions.idemKey": idemKey,
        accountCode: destAcct,
        side: "debit",
        currency,
        userId: adminUserId,
      }).lean();
      if (exists) continue;

      const syntheticPaymentId = new mongoose.Types.ObjectId();

      const dimsCommon = {
        idCobrador: Number(cobradorUser.idCobrador ?? idCobrador) ?? null,
        performedBy: adminUserId,
        note: note || "",
        dateFrom: df || null,
        dateTo: dt || null,
        kind: "ARQUEO_MANUAL",
        idemKey,
        fromUserId: cobradorUser._id,
        toUserId: adminUserId,
      };

      // Línea 1: CREDIT en CAJA_COBRADOR con userId = cobrador
      const creditOut = {
        paymentId: syntheticPaymentId,
        userId: cobradorUser._id,
        accountCode: "CAJA_COBRADOR",
        side: "credit",
        amount: balance,
        currency,
        postedAt: now,
        dimensions: { ...dimsCommon },
      };

      // Línea 2: DEBIT en CAJA_ADMIN (u otra destino) con userId = admin
      const debitIn = {
        paymentId: syntheticPaymentId,
        userId: adminUserId,
        accountCode: destAcct,
        side: "debit",
        amount: balance,
        currency,
        postedAt: now,
        dimensions: { ...dimsCommon },
      };

      const entries = await LedgerEntry.insertMany([creditOut, debitIn], {
        ordered: true,
      });

      created.push({
        currency,
        balance,
        from: { userId: cobradorUser._id, accountCode: "CAJA_COBRADOR" },
        to: { userId: adminUserId, accountCode: destAcct },
        paymentId: syntheticPaymentId,
        entryIds: entries.map((e) => e._id),
      });
    }

    return res.status(201).json({
      ok: true,
      message: "Arqueo realizado: fondos movidos a la caja del admin.",
      created: created.length,
      perCurrency: created,
      snapshot: {
        totalPosAntes: totalPos,
        cuentasOrigen: accts,
        cuentaDestino: destAcct,
      },
    });
  } catch (err) {
    next(err);
  }
}

/**
 * GET /api/admin/arqueos/usuarios/clientes
 * Lista de “clientes del cobrador” con estado billing (paid/due).
 * Query:
 *  - userId (resuelve idCobrador) o idCobrador directo
 *  - page, limit, q, sortBy, sortDir, full=1
 */
export async function listArqueoUsuarioClientes(req, res, next) {
  try {
    const FULL = String(req.query.full || "") === "1";
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.min(toInt(req.query.limit, 25), 100);
    const qRaw = (req.query.q || "").trim();

    const SORTABLE = new Set([
      "createdAt",
      "idCliente",
      "nombre",
      "ingreso",
      "cuota",
      "cuotaIdeal",
      "updatedAt",
    ]);
    const sortByParam = (req.query.sortBy || "createdAt").toString();
    const sortDirParam = toDir(req.query.sortDir || req.query.order || "desc");
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "createdAt";

    // Resolver cobrador target
    let targetIdCobrador = null;
    if (req.query.userId) {
      const user = await User.findById(String(req.query.userId))
        .select("_id role idCobrador")
        .lean();
      if (!user)
        return res
          .status(404)
          .json({ ok: false, message: "Usuario no encontrado" });
      targetIdCobrador = Number(user.idCobrador);
    } else if (req.query.idCobrador != null) {
      targetIdCobrador = Number(req.query.idCobrador);
    }
    if (!Number.isFinite(targetIdCobrador)) {
      return res.json({
        ok: true,
        items: [],
        total: 0,
        page: 1,
        pageSize: 0,
        sortBy,
        sortDir: sortDirParam === 1 ? "asc" : "desc",
        full: FULL ? 1 : 0,
      });
    }

    // Scope base por cobrador
    const and = [{ idCobrador: targetIdCobrador }];

    // Búsqueda
    const or = [];
    if (qRaw) {
      const esc = qRaw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const isNumeric = /^\d+$/.test(qRaw);
      const qDigits = onlyDigits(qRaw);

      or.push({ nombre: { $regex: esc, $options: "i" } });
      or.push({ domicilio: { $regex: esc, $options: "i" } });
      if (isNumeric) {
        or.push({ idCliente: Number(qRaw) });
      } else if (qDigits.length >= 3) {
        or.push({ cp: { $regex: qDigits, $options: "i" } });
      }
    }
    const matchStage =
      or.length > 0 ? { $and: [...and, { $or: or }] } : { $and: and };

    // Período actual
    const now = new Date();
    const NOW_PERIOD = yyyymmAR(now);
    const NOW_NUM = now.getFullYear() * 100 + (now.getMonth() + 1);

    // Proyección final
    const pipeline = [
      { $match: matchStage },

      // Normalizaciones
      {
        $addFields: {
          createdAtSafe: { $ifNull: ["$createdAt", { $toDate: "$_id" }] },
          _rankTitular: { $cond: [{ $eq: ["$rol", "TITULAR"] }, 0, 1] },
          _rankIntegrante: {
            $cond: [
              {
                $and: [
                  { $isNumber: "$integrante" },
                  { $gte: ["$integrante", 0] },
                ],
              },
              "$integrante",
              9999,
            ],
          },
          _cuotaVigente: {
            $cond: [
              { $eq: [{ $ifNull: ["$usarCuotaIdeal", false] }, true] },
              { $ifNull: ["$cuotaIdeal", 0] },
              { $ifNull: ["$cuota", 0] },
            ],
          },
          __isActive: {
            $and: [
              { $ne: [{ $type: "$baja" }, "date"] },
              {
                $or: [
                  { $eq: ["$activo", true] },
                  { $not: [{ $eq: ["$activo", false] }] },
                ],
              },
            ],
          },
        },
      },

      // Orden para tomar el primer doc del grupo
      {
        $sort: {
          idCliente: 1,
          _rankTitular: 1,
          _rankIntegrante: 1,
          createdAtSafe: 1,
          _id: 1,
        },
      },

      // Agrupar por idCliente (grupo)
      {
        $group: {
          _id: "$idCliente",
          firstDoc: { $first: "$$ROOT" },
          integrantesCount: { $sum: { $cond: ["$__isActive", 1, 0] } },
          cremacionesCount: {
            $sum: {
              $cond: [
                { $and: ["$__isActive", { $toBool: "$cremacion" }] },
                1,
                0,
              ],
            },
          },
          edadMax: {
            $max: {
              $cond: [
                "$__isActive",
                {
                  $cond: [
                    { $isNumber: "$edad" },
                    "$edad",
                    { $ifNull: ["$edad", 0] },
                  ],
                },
                0,
              ],
            },
          },
          createdAtSafe: { $min: "$createdAtSafe" },
          updatedAtMax: { $max: "$updatedAt" },
        },
      },

      // Proyección base
      {
        $project: {
          _id: "$firstDoc._id",
          idCliente: "$_id",
          nombre: "$firstDoc.nombre",
          nombreTitular: "$firstDoc.nombreTitular",
          domicilio: "$firstDoc.domicilio",
          ciudad: "$firstDoc.ciudad",
          provincia: "$firstDoc.provincia",
          cp: "$firstDoc.cp",
          telefono: "$firstDoc.telefono",
          cuota: "$firstDoc.cuota",
          cuotaIdeal: "$firstDoc.cuotaIdeal",
          usarCuotaIdeal: "$firstDoc.usarCuotaIdeal",
          cuotaVigente: "$firstDoc._cuotaVigente",
          sexo: "$firstDoc.sexo",
          idCobrador: "$firstDoc.idCobrador",
          activo: "$firstDoc.activo",
          ingreso: "$firstDoc.ingreso",
          vigencia: "$firstDoc.vigencia",
          baja: "$firstDoc.baja",
          createdAt: "$firstDoc.createdAt",
          updatedAt: "$firstDoc.updatedAt",
          rol: "$firstDoc.rol",
          integrante: "$firstDoc.integrante",
          integrantesCount: 1,
          cremacionesCount: 1,
          edadMax: 1,
          createdAtSafe: 1,
          updatedAtMax: 1,
        },
      },

      // lookup pagos: último período y suma aplicada al actual
      {
        $lookup: {
          from: Payment.collection.name,
          let: { idCli: "$idCliente" },
          pipeline: [
            {
              $match: {
                $expr: { $eq: ["$cliente.idCliente", "$$idCli"] },
                status: { $in: ["posted", "settled"] },
              },
            },
            { $unwind: "$allocations" },
            {
              $group: {
                _id: "$cliente.idCliente",
                maxPeriodPaid: { $max: "$allocations.period" },
                paidNowSum: {
                  $sum: {
                    $cond: [
                      { $eq: ["$allocations.period", NOW_PERIOD] },
                      {
                        $ifNull: [
                          "$allocations.amountApplied",
                          "$allocations.amount",
                        ],
                      },
                      0,
                    ],
                  },
                },
              },
            },
          ],
          as: "billAgg",
        },
      },
      { $addFields: { billAgg: { $arrayElemAt: ["$billAgg", 0] } } },

      // maxPeriodPaid → número YYYYMM
      {
        $addFields: {
          __maxNum: {
            $cond: [
              { $ifNull: ["$billAgg.maxPeriodPaid", false] },
              {
                $let: {
                  vars: {
                    y: {
                      $toInt: {
                        $substrBytes: ["$billAgg.maxPeriodPaid", 0, 4],
                      },
                    },
                    m: {
                      $toInt: {
                        $substrBytes: ["$billAgg.maxPeriodPaid", 5, 2],
                      },
                    },
                  },
                  in: { $add: [{ $multiply: ["$$y", 100] }, "$$m"] },
                },
              },
              { $subtract: [NOW_NUM, 1] },
            ],
          },
        },
      },

      // Campos de billing
      {
        $addFields: {
          "billing.lastPaidPeriod": "$billAgg.maxPeriodPaid",
          "billing.paidNow": { $ifNull: ["$billAgg.paidNowSum", 0] },
          "billing.chargeNow": { $ifNull: ["$cuotaVigente", 0] },
          "billing.arrearsCount": {
            $max: [{ $subtract: [NOW_NUM, "$__maxNum"] }, 0],
          },
          "billing.aheadCount": {
            $max: [{ $subtract: ["$__maxNum", NOW_NUM] }, 0],
          },
        },
      },
      {
        $addFields: {
          "billing.current": {
            $cond: [
              {
                $or: [
                  { $gte: ["$billing.paidNow", "$billing.chargeNow"] },
                  { $gt: ["$__maxNum", NOW_NUM] },
                ],
              },
              "paid",
              "due",
            ],
          },
        },
      },

      // Orden final
      {
        $sort:
          sortBy === "createdAt"
            ? { createdAtSafe: sortDirParam, _id: sortDirParam }
            : { [sortBy]: sortDirParam, _id: sortDirParam },
      },

      // Paginación si no es full
      ...(!FULL ? [{ $skip: (page - 1) * limit }, { $limit: limit }] : []),
    ];

    const [items, totalRes] = await Promise.all([
      Client.aggregate(pipeline).allowDiskUse(true),
      Client.aggregate([
        { $match: matchStage },
        { $group: { _id: "$idCliente" } },
        { $count: "n" },
      ]),
    ]);
    const total = totalRes?.[0]?.n || 0;

    return res.json({
      ok: true,
      items,
      total,
      page: FULL ? 1 : page,
      pageSize: FULL ? items.length : limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
      full: FULL ? 1 : 0,
    });
  } catch (err) {
    next(err);
  }
}

// ⬇️ NUEVO: CSV de clientes por cobrador (titulares agrupados, FULL EXPORT)
export async function exportCollectorClientsCSV(req, res, next) {
  try {
    const qUserId = (req.query.userId || "").trim();
    const qIdCobrador = (req.query.idCobrador || "").trim();
    const activeOnly = String(req.query.activeOnly || "").trim() === "1";

    // 1) Resolver idCobrador
    let cid = null;
    if (qIdCobrador) {
      cid = String(qIdCobrador);
    } else {
      if (!qUserId) {
        return res
          .status(400)
          .json({ ok: false, message: "Falta userId o idCobrador" });
      }
      const user = await User.findById(qUserId)
        .select("_id role idCobrador")
        .lean();
      if (!user)
        return res
          .status(404)
          .json({ ok: false, message: "Usuario no encontrado" });
      if (!["cobrador", "admin", "superAdmin"].includes(user.role)) {
        return res.status(403).json({ ok: false, message: "Rol sin permisos" });
      }
      if (user.idCobrador == null || user.idCobrador === "") {
        return res
          .status(400)
          .json({
            ok: false,
            message: "El usuario no tiene idCobrador asignado",
          });
      }
      cid = String(user.idCobrador);
    }

    // 2) Match base: TODOS los TITULARES del cobrador (sin activo por defecto)
    const baseMatch = {
      rol: "TITULAR",
      $expr: { $eq: [{ $toString: "$idCobrador" }, cid] },
      ...(activeOnly ? { activo: true } : {}),
    };

    const rows = await Client.aggregate([
      { $match: baseMatch },

      // Normalizaciones / defaults
      {
        $addFields: {
          telefonoStr: {
            $cond: [
              {
                $or: [
                  { $eq: ["$telefono", 0] },
                  { $eq: ["$telefono", "0"] },
                  { $eq: ["$telefono", null] },
                ],
              },
              "",
              { $toString: "$telefono" },
            ],
          },
          cuotaNum: { $ifNull: ["$cuota", 0] },
          cuotaIdealNum: { $ifNull: ["$cuotaIdeal", 0] },
          usarIdealBool: {
            $cond: [
              {
                $in: [
                  { $type: "$usarCuotaIdeal" },
                  ["bool", "int", "long", "double"],
                ],
              },
              { $toBool: "$usarCuotaIdeal" },
              false,
            ],
          },
        },
      },

      // Contar integrantes del grupo (si activeOnly=1, sólo activos; si no, todos)
      {
        $lookup: {
          from: Client.collection.name,
          let: { gid: "$idCliente" },
          pipeline: [
            {
              $match: {
                $expr: { $eq: ["$idCliente", "$$gid"] },
                ...(activeOnly ? { activo: true } : {}), // 👈 opcional
              },
            },
            { $count: "n" },
          ],
          as: "membersCount",
        },
      },
      {
        $addFields: {
          integrantes: {
            $ifNull: [{ $arrayElemAt: ["$membersCount.n", 0] }, 1],
          },
        },
      },

      // Proyección final
      {
        $project: {
          _id: 0,
          idCliente: 1,
          titular: { $ifNull: ["$nombre", ""] }, // nombre del documento TITULAR
          documento: { $ifNull: ["$documento", ""] },
          telefono: "$telefonoStr",
          domicilio: { $ifNull: ["$domicilio", ""] },
          ciudad: { $ifNull: ["$ciudad", ""] },
          integrantes: 1,
          cuota: "$cuotaNum",
          cuotaIdeal: "$cuotaIdealNum",
          cuotaVigente: {
            $cond: ["$usarIdealBool", "$cuotaIdealNum", "$cuotaNum"],
          },
        },
      },

      { $sort: { titular: 1, idCliente: 1 } },
    ]).allowDiskUse(true);

    // 3) CSV con BOM
    const headers = [
      "idCliente",
      "Titular",
      "Documento",
      "Teléfono",
      "Domicilio",
      "Ciudad",
      "Integrantes",
      "Cuota",
      "CuotaIdeal",
      "CuotaVigente",
    ];
    const esc = (v) => {
      if (v == null) return "";
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const to2 = (n) => Number(n || 0).toFixed(2);

    const lines = [
      headers.join(","),
      ...rows.map((r) =>
        [
          r.idCliente ?? "",
          r.titular ?? "",
          r.documento ?? "",
          r.telefono ?? "",
          r.domicilio ?? "",
          r.ciudad ?? "",
          Number(r.integrantes ?? 0),
          to2(r.cuota),
          to2(r.cuotaIdeal),
          to2(r.cuotaVigente),
        ]
          .map(esc)
          .join(",")
      ),
    ];

    const BOM = "\uFEFF";
    const csv = BOM + lines.join("\n");
    const filename = `clientes_cobrador_${cid}_${
      activeOnly ? "activos_" : ""
    }${new Date().toISOString().slice(0, 10)}.csv`;

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    return res.status(200).send(csv);
  } catch (err) {
    next(err);
  }
}
