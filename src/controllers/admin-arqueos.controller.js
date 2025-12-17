import mongoose from "mongoose";
import User from "../models/user.model.js";
import LedgerEntry from "../models/ledger-entry.model.js";
import Payment from "../models/payment.model.js";
import Cliente from "../models/client.model.js";

// ───────────────────── Helpers ─────────────────────
const toInt = (v, d = 0) => {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : d;
};

function yyyymmAR(date) {
  const d = date instanceof Date ? date : new Date(date);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}`; // "YYYY-MM"
}

const toDir = (v) => (String(v || "").toLowerCase() === "asc" ? 1 : -1);

const parseISODate = (s, endOfDay = false) => {
  if (!s) return null;
  const dt = new Date(`${s}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return null;
  if (endOfDay) dt.setHours(23, 59, 59, 999);
  return dt;
};

const debitExpr = {
  $sum: { $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0] },
};
const creditExpr = {
  $sum: { $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0] },
};

// Cuentas
const DEST_DEFAULT = "CAJA_ADMIN";
const DEFAULT_ACCOUNTS = ["CAJA_COBRADOR", "A_RENDIR_COBRADOR"];
const CAJA_CHICA = "CAJA_CHICA"; // global
const CAJA_GRANDE = "CAJA_GRANDE"; // global
const CAJA_SUPERADMIN = "CAJA_SUPERADMIN"; // personal SA
const SUPERADMIN_ACCOUNTS = [CAJA_GRANDE, CAJA_SUPERADMIN, CAJA_CHICA];
const BOX_ROLES = ["cobrador", "admin", "superAdmin"];

// Detecta si un string parece ObjectId
const asObjectId = (s) => {
  try {
    const str = String(s);
    if (!/^[0-9a-fA-F]{24}$/.test(str)) return null;
    return new mongoose.Types.ObjectId(str);
  } catch {
    return null;
  }
};
const monthNamesEs = [
  "enero",
  "febrero",
  "marzo",
  "abril",
  "mayo",
  "junio",
  "julio",
  "agosto",
  "septiembre",
  "octubre",
  "noviembre",
  "diciembre",
];
const normalizeDateStart = (v) => {
  if (!v) return null;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  d.setHours(0, 0, 0, 0);
  return d;
};
const normalizeDateEnd = (v) => {
  if (!v) return null;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  d.setHours(23, 59, 59, 999);
  return d;
};
const diffInDays = (from, to) => {
  const a = new Date(from);
  const b = new Date(to);
  if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return 0;
  const ms = b.getTime() - a.getTime();
  return Math.max(0, Math.floor(ms / (1000 * 60 * 60 * 24)));
};
const isObjectIdLike = (v) => {
  if (!v) return false;
  try {
    new mongoose.Types.ObjectId(String(v));
    return true;
  } catch {
    return false;
  }
};
async function getBalance({
  userId,
  accountCode,
  currency = "ARS",
  fromDt,
  toDt,
}) {
  const match = {
    userId: new mongoose.Types.ObjectId(String(userId)),
    accountCode,
    currency,
    ...(fromDt || toDt
      ? {
          postedAt: {
            ...(fromDt ? { $gte: fromDt } : {}),
            ...(toDt ? { $lte: toDt } : {}),
          },
        }
      : {}),
  };

  const r = await LedgerEntry.aggregate([
    { $match: match },
    { $group: { _id: null, debits: debitExpr, credits: creditExpr } },
    { $project: { _id: 0, balance: { $subtract: ["$debits", "$credits"] } } },
  ]);

  return Number(r?.[0]?.balance || 0);
}
export {
  toInt,
  parseISODate,
  debitExpr,
  creditExpr,
  DEST_DEFAULT,
  DEFAULT_ACCOUNTS,
  CAJA_CHICA,
  CAJA_GRANDE,
  CAJA_SUPERADMIN,
  SUPERADMIN_ACCOUNTS,
  BOX_ROLES,
  asObjectId,
  getBalance,
};
export async function listArqueosUsuarios(req, res, next) {
  try {
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.max(Math.min(toInt(req.query.limit || 25, 25), 200), 1);

    const q = String(req.query.q || "").trim();
    const roleFilter = String(req.query.role || "").trim();

    const sortByParam = String(req.query.sortBy || "totalBalance");
    const sortDirParam = toDir(req.query.sortDir || "desc");
    const orderMode = String(req.query.orderMode || "default");

    const viewerRole = String(req.user?.role || "").trim();

    const ALLOWED_BY_VIEWER =
      viewerRole === "superAdmin"
        ? ["superAdmin", "admin", "cobrador"]
        : ["cobrador"];

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

    // ✅ IMPORTANTE: como ya corregiste los controllers,
    // ✅ toUser SIEMPRE ES EL DUEÑO DE LA LÍNEA.
    // => arqueos por usuario: match por toUser (normalizado)

    const base = [
      { $match: userMatch },

      // aliases del usuario (lowercase) para matchear toUser
      {
        $addFields: {
          __aliasesLower: {
            $setDifference: [
              {
                $filter: {
                  input: {
                    $setUnion: [
                      // name + email
                      [
                        {
                          $toLower: {
                            $trim: { input: { $ifNull: ["$name", ""] } },
                          },
                        },
                        {
                          $toLower: {
                            $trim: { input: { $ifNull: ["$email", ""] } },
                          },
                        },
                      ],

                      // cobrador #id
                      [
                        {
                          $cond: [
                            {
                              $and: [
                                { $eq: ["$role", "cobrador"] },
                                { $ne: ["$idCobrador", null] },
                              ],
                            },
                            {
                              $toLower: {
                                $concat: [
                                  "cobrador #",
                                  { $toString: "$idCobrador" },
                                ],
                              },
                            },
                            "",
                          ],
                        },
                      ],

                      // labels legacy típicos (por si quedaron movimientos viejos)
                      [
                        { $cond: [{ $eq: ["$role", "admin"] }, "admin", ""] },
                        {
                          $cond: [
                            { $eq: ["$role", "admin"] },
                            "administración",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            { $eq: ["$role", "admin"] },
                            "administracion",
                            "",
                          ],
                        },
                        {
                          $cond: [{ $eq: ["$role", "admin"] }, "caja_admin", ""],
                        },

                        {
                          $cond: [
                            { $eq: ["$role", "superAdmin"] },
                            "superadmin",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            { $eq: ["$role", "superAdmin"] },
                            "super admin",
                            "",
                          ],
                        },
                        {
                          $cond: [
                            { $eq: ["$role", "superAdmin"] },
                            "caja_superadmin",
                            "",
                          ],
                        },
                      ],
                    ],
                  },
                  as: "x",
                  cond: { $gt: [{ $strLenCP: "$$x" }, 0] },
                },
              },
              [""],
            ],
          },
        },
      },

      {
        $lookup: {
          from: LedgerEntry.collection.name,
          let: { urole: "$role", aliasesLower: "$__aliasesLower" },
          pipeline: [
            {
              $addFields: {
                __toLower: {
                  $toLower: {
                    $trim: { input: { $ifNull: ["$toUser", ""] } },
                  },
                },
              },
            },
            {
              $match: {
                $expr: {
                  $and: [
                    // 1) cuentas permitidas por rol (o override)
                    {
                      $in: [
                        "$accountCode",
                        accountCodesOverride
                          ? accountCodesOverride
                          : {
                              $cond: [
                                { $eq: ["$$urole", "admin"] },
                                [destAccountCode],
                                {
                                  $cond: [
                                    { $eq: ["$$urole", "superAdmin"] },
                                    ["CAJA_SUPERADMIN"],
                                    DEFAULT_ACCOUNTS,
                                  ],
                                },
                              ],
                            },
                      ],
                    },

                    // 2) dueño = toUser
                    { $in: ["$__toLower", "$$aliasesLower"] },

                    // 3) rango postedAt
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
                debits: {
                  $sum: {
                    $cond: [
                      { $eq: ["$side", "debit"] },
                      { $ifNull: ["$amount", 0] },
                      0,
                    ],
                  },
                },
                credits: {
                  $sum: {
                    $cond: [
                      { $eq: ["$side", "credit"] },
                      { $ifNull: ["$amount", 0] },
                      0,
                    ],
                  },
                },
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

    const dataPipeline = [...base, sortStage];
    const countPipeline = [{ $match: userMatch }, { $count: "n" }];

    const [realItems, countRes] = await Promise.all([
      User.aggregate(dataPipeline).allowDiskUse(true),
      User.aggregate(countPipeline).allowDiskUse(true),
    ]);

    const total = countRes?.[0]?.n || 0;

    const start = (page - 1) * limit;
    const end = start + limit;

    const finalItems =
      orderMode === "default"
        ? realItems.slice(start, end)
        : realItems.slice(start, end); // (si querés jerarquía acá, lo enchufamos después)

    return res.json({
      ok: true,
      items: finalItems,
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
            : roleFilter === "global"
            ? "global"
            : null,
        dateFrom: req.query.dateFrom || null,
        dateTo: req.query.dateTo || null,
        q,
        destAccountCode,
        orderMode,
        ownerMatch: "toUser (owner of line)",
      },
    });
  } catch (err) {
    next(err);
  }
}

export async function getArqueoUsuarioDetalle(req, res, next) {
  try {
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limit = Math.max(Math.min(toInt(req.query.limit || 25, 25), 200), 1);
    const sortByParam = String(req.query.sortBy || "postedAt");
    const sortDirParam = toDir(req.query.sortDir || "desc");

    const rawUserId = String(req.query.userId || "").trim();

    const accountCodesOverride = String(req.query.accountCodes || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const destAccountCode =
      String(req.query.destAccountCode || "").trim() || DEST_DEFAULT;

    const fromDt = parseISODate(req.query.dateFrom);
    const toDt = parseISODate(req.query.dateTo, true);
    const side = String(req.query.side || "");

    const SORTABLE = new Set([
      "postedAt",
      "amount",
      "accountCode",
      "side",
      "currency",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "postedAt";
    const sortStage = { $sort: { [sortBy]: sortDirParam, _id: sortDirParam } };

    const isGlobal = rawUserId.startsWith("GLOBAL:");

    // ───────────────────────── RAMA GLOBAL ─────────────────────────
    if (isGlobal) {
      const globalCode = rawUserId.replace(/^GLOBAL:/, "").trim();

      const effectiveAccounts =
        accountCodesOverride.length > 0 ? accountCodesOverride : [globalCode];

      const match = {
        accountCode: { $in: effectiveAccounts },
      };

      if (fromDt || toDt) {
        match.postedAt = {
          ...(fromDt ? { $gte: fromDt } : {}),
          ...(toDt ? { $lte: toDt } : {}),
        };
      }

      if (side === "debit" || side === "credit") {
        match.side = side;
      }

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

      const itemsPipeline = [
        ...base,
        {
          $project: {
            _id: 1,
            paymentId: 1,
            kind: 1,
            userId: 1,
            fromUser: 1,
            toUser: 1,
            fromAccountCode: 1,
            toAccountCode: 1,
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

      const user = {
        _id: rawUserId,
        name: `${globalCode} (GLOBAL)`,
        email: "",
        role: "global",
        idCobrador: null,
      };

      return res.json({
        ok: true,
        header: { user, idCobrador: null, totals },
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
          isGlobal: true,
          globalCode,
        },
      });
    }

    // ────────────────────── RAMA USUARIO REAL ──────────────────────
    const user = await User.findById(rawUserId)
      .select("_id name email role idCobrador")
      .lean();

    if (!user) {
      return res.status(400).json({ ok: false, message: "Usuario no encontrado" });
    }
    if (!BOX_ROLES.includes(user.role)) {
      return res.status(403).json({ ok: false, message: "Rol sin caja habilitada" });
    }

    const effectiveAccounts =
      accountCodesOverride.length > 0
        ? accountCodesOverride
        : user.role === "admin"
        ? [destAccountCode]
        : user.role === "superAdmin"
        ? SUPERADMIN_ACCOUNTS
        : DEFAULT_ACCOUNTS;

    const aliasesLower = [
      String(user.name || "").trim().toLowerCase(),
      String(user.email || "").trim().toLowerCase(),
      user.role === "cobrador" && user.idCobrador != null
        ? `cobrador #${String(user.idCobrador)}`.toLowerCase()
        : "",
      user.role === "admin" ? "admin" : "",
      user.role === "admin" ? "administración" : "",
      user.role === "admin" ? "administracion" : "",
      user.role === "admin" ? "caja_admin" : "",
      user.role === "superAdmin" ? "superadmin" : "",
      user.role === "superAdmin" ? "super admin" : "",
      user.role === "superAdmin" ? "caja_superadmin" : "",
    ].filter(Boolean);

    const baseMatch = {
      accountCode: { $in: effectiveAccounts },
      ...(fromDt || toDt
        ? {
            postedAt: {
              ...(fromDt ? { $gte: fromDt } : {}),
              ...(toDt ? { $lte: toDt } : {}),
            },
          }
        : {}),
      ...(side === "debit" || side === "credit" ? { side } : {}),
    };

    // ✅ owner = toUser
    const base = [
      { $match: baseMatch },
      {
        $addFields: {
          __toLower: {
            $toLower: { $trim: { input: { $ifNull: ["$toUser", ""] } } },
          },
        },
      },
      { $match: { __toLower: { $in: aliasesLower } } },
    ];

    const totalsPipeline = [
      ...base,
      {
        $group: {
          _id: null,
          debits: {
            $sum: {
              $cond: [
                { $eq: ["$side", "debit"] },
                { $ifNull: ["$amount", 0] },
                0,
              ],
            },
          },
          credits: {
            $sum: {
              $cond: [
                { $eq: ["$side", "credit"] },
                { $ifNull: ["$amount", 0] },
                0,
              ],
            },
          },
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

    const itemsPipeline = [
      ...base,
      {
        $project: {
          _id: 1,
          paymentId: 1,
          kind: 1,
          userId: 1,
          fromUser: 1,
          toUser: 1,
          fromAccountCode: 1,
          toAccountCode: 1,
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
        isGlobal: false,
        ownerMatch: "toUser (owner of line)",
        aliasesLower,
      },
    });
  } catch (err) {
    next(err);
  }
}
export async function crearArqueoUsuario(req, res, next) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const {
      userId: bodyUserId,
      idCobrador,
      note = "",
      accountCodes,
      dateFrom: df,
      dateTo: dt,
      destAccountCode,
      minAmount = 1,
    } = req.body || {};

    // 0) permisos mínimos
    const viewerRole = String(req.user?.role || "").trim();
    if (!["admin", "superAdmin"].includes(viewerRole)) {
      await session.abortTransaction();
      return res
        .status(403)
        .json({ ok: false, message: "Permisos insuficientes" });
    }

    // 1) Cobrador origen (puede ser cobrador/admin/superAdmin si tiene caja)
    let cobradorUser = null;
    if (bodyUserId) {
      cobradorUser = await User.findById(bodyUserId)
        .select("_id name email role idCobrador")
        .session(session)
        .lean();
    } else if (idCobrador != null) {
      cobradorUser =
        (await User.findOne({ idCobrador: String(idCobrador) })
          .select("_id name email role idCobrador")
          .session(session)
          .lean()) ||
        (await User.findOne({ idCobrador: Number(idCobrador) })
          .select("_id name email role idCobrador")
          .session(session)
          .lean());
    }

    if (!cobradorUser) {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "Usuario cobrador no encontrado" });
    }
    if (!BOX_ROLES.includes(cobradorUser.role)) {
      await session.abortTransaction();
      return res
        .status(403)
        .json({ ok: false, message: "Rol sin caja habilitada" });
    }

    // 2) Admin ejecutor (actor/dueño del asiento)
    const adminUserId = req.user?._id
      ? new mongoose.Types.ObjectId(String(req.user._id))
      : null;

    if (!adminUserId) {
      await session.abortTransaction();
      return res
        .status(403)
        .json({ ok: false, message: "Sesión admin requerida" });
    }

    // ✅ NEW: resolver nombre real del admin desde DB (evita fromUser/toUser = null)
    const adminUser = await User.findById(adminUserId)
      .select("_id name email role")
      .session(session)
      .lean();

    const executorName =
      String(req.user?.name || req.user?.email || "").trim() ||
      String(adminUser?.name || adminUser?.email || "").trim() ||
      "ADMINISTRACIÓN";

    const idCobNum = Number(cobradorUser?.idCobrador ?? idCobrador);
    const safeIdCob = Number.isFinite(idCobNum) ? idCobNum : null;

    const cobradorName =
      String(cobradorUser?.name || cobradorUser?.email || "").trim() ||
      (Number.isFinite(idCobNum) ? `Cobrador #${idCobNum}` : "COBRADOR");

    // 3) Cuentas y ventana
    const parsed = String(accountCodes || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    const accts = parsed.length > 0 ? parsed : DEFAULT_ACCOUNTS;

    const destAcct =
      String(destAccountCode || DEST_DEFAULT).trim() || DEST_DEFAULT;

    if (!destAcct || destAcct === "CAJA_COBRADOR") {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "Cuenta destino inválida" });
    }

    const fromDt = parseISODate(df);
    const toDt = parseISODate(dt, true);

    // 4) Saldo por cuenta+moneda del ORIGEN
    // ✅ NEW-ONLY: calculamos saldo de caja del cobrador por dimensions.idCobrador (no por legacy matchers)
    const matchCollector = {
      accountCode: { $in: accts },
      ...(safeIdCob != null ? { "dimensions.idCobrador": safeIdCob } : {}),
      ...(fromDt || toDt
        ? {
            postedAt: {
              ...(fromDt ? { $gte: fromDt } : {}),
              ...(toDt ? { $lte: toDt } : {}),
            },
          }
        : {}),
    };

    const byAcctCurrency = await LedgerEntry.aggregate([
      { $match: matchCollector },
      {
        $group: {
          _id: { currency: "$currency", accountCode: "$accountCode" },
          debits: debitExpr,
          credits: creditExpr,
        },
      },
      {
        $project: {
          _id: 0,
          currency: "$_id.currency",
          accountCode: "$_id.accountCode",
          balance: { $subtract: ["$debits", "$credits"] },
        },
      },
    ]).allowDiskUse(true);

    const minAmt = Number(minAmount || 0);

    const positives = (byAcctCurrency || [])
      .map((r) => ({
        currency: r.currency || "ARS",
        accountCode: String(r.accountCode || "").trim(),
        balance: Number(r.balance || 0),
      }))
      .filter(
        (r) =>
          r.accountCode &&
          Number.isFinite(r.balance) &&
          r.balance > 0 &&
          r.balance >= (Number.isFinite(minAmt) ? minAmt : 0)
      );

    const totalPos = positives.reduce((a, r) => a + r.balance, 0);

    if (!positives.length || totalPos <= 0) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message:
          "No hay saldo positivo para transferir desde la caja del cobrador.",
        details: { perAcctCurrency: byAcctCurrency },
      });
    }

    // 5) Escribir doble partida por cada (accountCode, currency) con saldo positivo
    const created = [];
    const postedAt = new Date();

    for (const row of positives) {
      const { currency, balance, accountCode } = row;
      const amtAbs = Math.abs(Number(balance) || 0);
      if (!(amtAbs > 0)) continue;

      // paymentId correlativo (no hay Payment real)
      const paymentId = new mongoose.Types.ObjectId();

      // dimensions new schema (sin performedBy/cobradorId)
      const dims = {
        idCobrador: safeIdCob,
        idCliente: null,
        plan: null,
        canal: "ARQUEO_USUARIO",
        note: String(note || "").trim(),
      };

      const docs = [
        // ✅ DEBIT: ORIGEN -> DESTINO (cobrador -> admin)
        {
          paymentId,
          userId: adminUserId, // actor/dueño (admin ejecutor) - consistente
          kind: "ARQUEO_MANUAL",
          side: "debit",
          accountCode: destAcct,
          amount: amtAbs,
          currency,
          postedAt,
          fromUser: cobradorName,
          toUser: executorName,
          fromAccountCode: accountCode,
          toAccountCode: destAcct,
          dimensions: dims,
        },

        // ✅ CREDIT: inverso (admin -> cobrador)
        {
          paymentId,
          userId: adminUserId, // mismo actor/dueño
          kind: "ARQUEO_MANUAL",
          side: "credit",
          accountCode: accountCode,
          amount: amtAbs,
          currency,
          postedAt,
          fromUser: executorName,
          toUser: cobradorName,
          fromAccountCode: destAcct,
          toAccountCode: accountCode,
          dimensions: dims,
        },
      ];

      const ins = await LedgerEntry.insertMany(docs, {
        session,
        ordered: true,
      });

      created.push({
        currency,
        amount: amtAbs,
        from: { accountCode },
        to: { accountCode: destAcct },
        paymentId: String(paymentId),
        entryIds: ins.map((x) => String(x._id)),
      });
    }

    await session.commitTransaction();

    return res.status(201).json({
      ok: true,
      message: "Arqueo realizado: fondos movidos a la caja del admin.",
      created: created.length,
      perCurrency: created,
      snapshot: {
        totalPosAntes: totalPos,
        cuentasOrigen: accts,
        cuentaDestino: destAcct,
        ventana: { dateFrom: df || null, dateTo: dt || null },
        cobrador: {
          userId: String(cobradorUser._id),
          idCobrador: safeIdCob,
          name: cobradorName,
        },
        executedBy: {
          userId: String(adminUserId),
          name: executorName,
        },
      },
    });
  } catch (err) {
    try {
      await session.abortTransaction();
    } catch {}
    return next(err);
  } finally {
    session.endSession();
  }
}
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

    // Resolver cobrador target (userId -> idCobrador) o idCobrador directo
    let targetIdCobrador = null;
    if (req.query.userId) {
      const user = await User.findById(String(req.query.userId))
        .select("_id role idCobrador")
        .lean();
      if (!user) {
        return res
          .status(404)
          .json({ ok: false, message: "Usuario no encontrado" });
      }
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
    const NOW_PERIOD = yyyymmAR(now); // "YYYY-MM"
    const NOW_NUM = now.getFullYear() * 100 + (now.getMonth() + 1); // YYYYMM numérico

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
            {
              $unwind: {
                path: "$allocations",
                preserveNullAndEmptyArrays: true,
              },
            },
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
      Cliente.aggregate(pipeline).allowDiskUse(true),
      Cliente.aggregate([
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
    console.error("listArqueoUsuarioClientes error:", err);
    next(err);
  }
}
export async function exportCollectorClientsCSV(req, res, next) {
  try {
    const qUserId = String(req.query.userId || "").trim();
    const qIdCobrador = String(req.query.idCobrador || "").trim();
    const activeOnly = String(req.query.activeOnly || "") === "1";

    // ── resolver idCobrador (cid)
    let cid = null;
    if (qIdCobrador) {
      cid = qIdCobrador;
    } else if (qUserId) {
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
        return res.status(400).json({
          ok: false,
          message: "El usuario no tiene idCobrador asignado",
        });
      }
      cid = String(user.idCobrador);
    } else {
      return res
        .status(400)
        .json({ ok: false, message: "Falta userId o idCobrador" });
    }

    // ── permisos: cobrador solo su propia cartera
    const viewerRole = String(req.user?.role || "");
    const viewerIdCobrador =
      req.user?.idCobrador != null ? String(req.user.idCobrador) : null;
    if (viewerRole === "cobrador") {
      if (!viewerIdCobrador) {
        return res
          .status(403)
          .json({ ok: false, message: "Cobrador sin idCobrador en sesión" });
      }
      if (viewerIdCobrador !== String(cid)) {
        return res
          .status(403)
          .json({ ok: false, message: "No autorizado para este idCobrador" });
      }
    } else if (!["admin", "superAdmin"].includes(viewerRole)) {
      return res
        .status(403)
        .json({ ok: false, message: "Permisos insuficientes" });
    }

    // ── pipeline: TODOS los miembros del cobrador; agrupamos por grupo (idCliente)
    const rows = await Cliente.aggregate([
      {
        $match: {
          $expr: { $eq: [{ $toString: "$idCobrador" }, String(cid)] },
        },
      },
      {
        $addFields: {
          __isActiveMember: {
            $cond: [
              {
                $or: [
                  { $eq: [{ $type: "$baja" }, "missing"] },
                  { $eq: ["$baja", null] },
                ],
              },
              { $cond: [{ $eq: ["$activo", false] }, false, true] },
              false,
            ],
          },
          __rankTitular: {
            $cond: [
              {
                $regexMatch: {
                  input: { $ifNull: ["$rol", ""] },
                  regex: /^titular$/i,
                },
              },
              0,
              1,
            ],
          },
          __rankIntegrante: {
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
      {
        $sort: {
          idCliente: 1,
          __rankTitular: 1,
          __rankIntegrante: 1,
          createdAt: 1,
          _id: 1,
        },
      },
      {
        $group: {
          _id: "$idCliente",
          rep: { $first: "$$ROOT" },
          activosEnGrupo: { $sum: { $cond: ["$__isActiveMember", 1, 0] } },
        },
      },
      ...(activeOnly ? [{ $match: { activosEnGrupo: { $gt: 0 } } }] : []),
      {
        $project: {
          _id: 0,
          idCliente: "$_id",
          titular: { $ifNull: ["$rep.nombre", ""] },
          documento: { $ifNull: ["$rep.documento", ""] },
          telefono: "$rep.telefonoStr",
          domicilio: { $ifNull: ["$rep.domicilio", ""] },
          ciudad: { $ifNull: ["$rep.ciudad", ""] },
          integrantes: "$activosEnGrupo",
          cuota: "$rep.cuotaNum",
          cuotaIdeal: "$rep.cuotaIdealNum",
          cuotaVigente: {
            $cond: [
              "$rep.usarIdealBool",
              "$rep.cuotaIdealNum",
              "$rep.cuotaNum",
            ],
          },
        },
      },
      { $sort: { titular: 1, idCliente: 1 } },
    ]).allowDiskUse(true);

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
export async function depositoCajaChica(req, res, next) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const { adminUserId, currency = "ARS", note = "" } = req.body || {};
    if (!adminUserId) {
      await session.abortTransaction();
      return res.status(400).json({ ok: false, message: "Falta adminUserId" });
    }

    const admin = await User.findById(adminUserId)
      .select("_id role name email")
      .session(session)
      .lean();

    if (!admin || admin.role !== "admin") {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "adminUserId inválido" });
    }

    const viewerRole = String(req.user?.role || "").trim();
    const isSelf = String(req.user?._id) === String(admin._id);
    if (!(isSelf || viewerRole === "superAdmin")) {
      await session.abortTransaction();
      return res.status(403).json({ ok: false, message: "Sin permisos" });
    }

    const executorOid = req.user?._id
      ? new mongoose.Types.ObjectId(String(req.user._id))
      : new mongoose.Types.ObjectId(String(admin._id));

    const executorUser = await User.findById(executorOid)
      .select("_id name email role")
      .session(session)
      .lean();

    const adminName =
      String(admin?.name || admin?.email || "").trim() || "ADMIN";
    const executorName =
      String(req.user?.name || req.user?.email || "").trim() ||
      String(executorUser?.name || executorUser?.email || "").trim() ||
      adminName;

    const saldoAdmin = await getBalance({
      userId: admin._id,
      accountCode: DEST_DEFAULT, // CAJA_ADMIN
      currency,
    });

    const amount = Number(saldoAdmin || 0);
    if (!Number.isFinite(amount) || amount <= 0) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message: "No hay saldo disponible en CAJA_ADMIN para mover.",
        balance: saldoAdmin,
      });
    }

    const postedAt = new Date();
    const amtAbs = Math.abs(amount);
    const paymentId = new mongoose.Types.ObjectId();

    const dims = {
      idCobrador: null,
      idCliente: null,
      plan: null,
      canal: "CAJA_ADMIN->CAJA_CHICA",
      note: String(note || "").trim(),
      executedByUserId: String(executorOid),
      executedByName: executorName,
    };

    // Dirección real del movimiento
    const realFromAccount = DEST_DEFAULT; // CAJA_ADMIN (usuario)
    const realToAccount = CAJA_CHICA; // caja física

    // ✅ DEBIT: entra a CAJA_CHICA (dueño = CAJA_CHICA)
    const debitDoc = {
      paymentId,
      userId: new mongoose.Types.ObjectId(String(admin._id)), // custodio (tu esquema)
      kind: "CAJA_CHICA_DEPOSITO_ALL",
      side: "debit",
      accountCode: CAJA_CHICA,
      amount: amtAbs,
      currency,
      postedAt,

      fromUser: adminName,
      toUser: "CAJA_CHICA",
      fromAccountCode: realFromAccount,
      toAccountCode: realToAccount,

      dimensions: dims,
    };

    // ✅ CREDIT: sale de CAJA_ADMIN (dueño = ADMIN) => INVERTIMOS para que toUser sea el dueño
    const creditDoc = {
      paymentId,
      userId: new mongoose.Types.ObjectId(String(admin._id)),
      kind: "CAJA_CHICA_DEPOSITO_ALL",
      side: "credit",
      accountCode: DEST_DEFAULT, // CAJA_ADMIN
      amount: amtAbs,
      currency,
      postedAt,

      fromUser: "CAJA_CHICA",
      toUser: adminName,
      fromAccountCode: CAJA_CHICA,
      toAccountCode: DEST_DEFAULT,

      dimensions: dims,
    };

    const ins = await LedgerEntry.insertMany([debitDoc, creditDoc], {
      session,
      ordered: true,
    });

    await session.commitTransaction();
    return res.status(201).json({
      ok: true,
      movedAll: true,
      amount: amtAbs,
      currency,
      paymentId: String(paymentId),
      entryIds: ins.map((x) => String(x._id)),
      executedBy: { userId: String(executorOid), name: executorName },
    });
  } catch (err) {
    try {
      await session.abortTransaction();
    } catch {}
    return next(err);
  } finally {
    session.endSession();
  }
}
export async function ingresoCajaGrande(req, res, next) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    if (String(req.user?.role || "").trim() !== "superAdmin") {
      await session.abortTransaction();
      return res.status(403).json({ ok: false, message: "Sólo superAdmin" });
    }

    const {
      amount: amountRaw,
      moveAll,
      currency = "ARS",
      toSuperAdminUserId,
      note = "",
    } = req.body || {};

    const vaultOwnerId = toSuperAdminUserId || req.user._id;

    const sa = await User.findById(vaultOwnerId)
      .select("_id role name email")
      .session(session)
      .lean();

    if (!sa || sa.role !== "superAdmin") {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "toSuperAdminUserId inválido" });
    }

    const executorOid = new mongoose.Types.ObjectId(String(req.user._id));
    const executorUser = await User.findById(executorOid)
      .select("_id name email role")
      .session(session)
      .lean();

    const executorName =
      String(req.user?.name || req.user?.email || "").trim() ||
      String(executorUser?.name || executorUser?.email || "").trim() ||
      "SUPERADMIN";

    const saName = String(sa?.name || sa?.email || "").trim() || "SUPERADMIN";

    // 1) balances por userId en CAJA_CHICA (por moneda)
    const byUser = await LedgerEntry.aggregate([
      { $match: { accountCode: CAJA_CHICA, currency } },
      { $group: { _id: "$userId", debits: debitExpr, credits: creditExpr } },
      {
        $project: {
          _id: 0,
          userId: "$_id",
          balance: { $subtract: ["$debits", "$credits"] },
        },
      },
      { $match: { balance: { $gt: 0 } } },
      { $sort: { balance: -1 } },
    ]).allowDiskUse(true);

    if (!byUser.length) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message: "No hay saldo disponible en CAJA_CHICA (GLOBAL) para mover.",
        available: 0,
      });
    }

    // 1.1) solo admins
    const userIds = byUser.map((x) => x.userId).filter(Boolean);

    const admins = await User.find({ _id: { $in: userIds }, role: "admin" })
      .select("_id name email role")
      .session(session)
      .lean();

    const adminIdSet = new Set((admins || []).map((u) => String(u._id)));
    const byAdmin = (byUser || []).filter((it) =>
      adminIdSet.has(String(it.userId))
    );

    const totalAvailable = byAdmin.reduce(
      (acc, it) => acc + Number(it.balance || 0),
      0
    );

    let requested = Number(amountRaw);
    const wantAll = String(moveAll || "") === "1" || !Number(requested);
    if (wantAll) requested = totalAvailable;

    if (!Number.isFinite(requested) || requested <= 0 || totalAvailable <= 0) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message: "No hay saldo disponible en CAJA_CHICA (GLOBAL) para mover.",
        available: totalAvailable,
        requestedAll: wantAll ? 1 : 0,
      });
    }

    if (requested > totalAvailable + 1e-9) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message: "El monto solicitado excede el saldo en CAJA_CHICA (GLOBAL).",
        available: totalAvailable,
        requested,
      });
    }

    // 2) Transferir CHICA(admin) -> GRANDE(SA)
    let remaining = requested;
    const transfers = [];
    const postedAt = new Date();

    for (const it of byAdmin) {
      if (remaining <= 0) break;

      const ownerId = it.userId; // admin dueño de esa caja chica
      const ownerIdStr = String(ownerId);

      const move = Math.min(remaining, Number(it.balance || 0));
      if (!Number.isFinite(move) || move <= 0) continue;

      const amtAbs = Math.abs(move);
      const paymentId = new mongoose.Types.ObjectId();

      const dims = {
        idCobrador: null,
        idCliente: null,
        plan: null,
        canal: "CAJA_CHICA->CAJA_GRANDE",
        note: String(note || "").trim(),
        fromAdminUserId: ownerIdStr,
        executedByUserId: String(executorOid),
        executedByName: executorName,
      };

      // ✅ DEBIT: entra a CAJA_GRANDE (dueño = CAJA_GRANDE)
      const debitDoc = {
        paymentId,
        userId: new mongoose.Types.ObjectId(String(sa._id)), // custodio del “vault”
        kind: wantAll ? "CAJA_GRANDE_INGRESO_ALL" : "CAJA_GRANDE_INGRESO",
        side: "debit",
        accountCode: CAJA_GRANDE,
        amount: amtAbs,
        currency,
        postedAt,

        fromUser: "CAJA_CHICA",
        toUser: "CAJA_GRANDE",
        fromAccountCode: CAJA_CHICA,
        toAccountCode: CAJA_GRANDE,

        dimensions: dims,
      };

      // ✅ CREDIT: sale de CAJA_CHICA (dueño = CAJA_CHICA) => INVERTIMOS para que toUser sea el dueño
      const creditDoc = {
        paymentId,
        userId: new mongoose.Types.ObjectId(String(ownerId)), // de dónde salió (por tu esquema)
        kind: wantAll ? "CAJA_GRANDE_INGRESO_ALL" : "CAJA_GRANDE_INGRESO",
        side: "credit",
        accountCode: CAJA_CHICA,
        amount: amtAbs,
        currency,
        postedAt,

        fromUser: "CAJA_GRANDE",
        toUser: "CAJA_CHICA",
        fromAccountCode: CAJA_GRANDE,
        toAccountCode: CAJA_CHICA,

        dimensions: dims,
      };

      const ins = await LedgerEntry.insertMany([debitDoc, creditDoc], {
        session,
        ordered: true,
      });

      transfers.push({
        moved: amtAbs,
        fromAdminUserId: ownerIdStr,
        toSuperAdminUserId: String(sa._id),
        paymentId: String(paymentId),
        entryIds: ins.map((x) => String(x._id)),
      });

      remaining -= amtAbs;
    }

    await session.commitTransaction();

    return res.status(201).json({
      ok: true,
      movedAll: wantAll,
      amount: requested,
      currency,
      transfersCount: transfers.length,
      requestedAll: wantAll ? 1 : 0,
      available: totalAvailable,
      transfers,
      executedBy: { userId: String(executorOid), name: executorName },
    });
  } catch (err) {
    try {
      await session.abortTransaction();
    } catch {}
    return next(err);
  } finally {
    session.endSession();
  }
}
export async function extraccionCajaGrande(req, res, next) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const role = String(req.user?.role || "").trim();
    if (role !== "superAdmin") {
      await session.abortTransaction();
      return res.status(403).json({ ok: false, message: "Sólo superAdmin" });
    }

    const {
      amount: amountRaw,
      moveAll,
      currency = "ARS",
      note = "",
      superAdminUserId,
    } = req.body || {};

    const ownerId = superAdminUserId || req.user._id;

    const sa = await User.findById(ownerId)
      .select("_id role name email")
      .session(session)
      .lean();

    if (!sa || sa.role !== "superAdmin") {
      await session.abortTransaction();
      return res
        .status(400)
        .json({ ok: false, message: "superAdminUserId inválido" });
    }

    const executorOid = new mongoose.Types.ObjectId(String(req.user._id));
    const executorUser = await User.findById(executorOid)
      .select("_id name email role")
      .session(session)
      .lean();

    const executorName =
      String(req.user?.name || req.user?.email || "").trim() ||
      String(executorUser?.name || executorUser?.email || "").trim() ||
      "SUPERADMIN";

    const saName = String(sa?.name || sa?.email || "").trim() || "SUPERADMIN";

    let amount = Number(amountRaw);
    const shouldMoveAll = String(moveAll || "") === "1" || !Number(amount);

    if (shouldMoveAll) {
      const saldoGrande = await getBalance({
        userId: sa._id,
        accountCode: CAJA_GRANDE,
        currency,
      });
      amount = Number(saldoGrande || 0);
    }

    if (!Number.isFinite(amount) || amount <= 0) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message: "No hay saldo disponible en CAJA_GRANDE para extraer.",
        requestedAll: shouldMoveAll ? 1 : 0,
      });
    }

    const postedAt = new Date();
    const amtAbs = Math.abs(amount);
    const paymentId = new mongoose.Types.ObjectId();

    const dims = {
      idCobrador: null,
      idCliente: null,
      plan: null,
      canal: "CAJA_GRANDE->CAJA_SUPERADMIN",
      note: String(note || "").trim(),
      executedByUserId: String(executorOid),
      executedByName: executorName,
    };

    // ✅ DEBIT: entra al usuario (dueño = SA)
    const debitDoc = {
      paymentId,
      userId: new mongoose.Types.ObjectId(String(sa._id)),
      kind: shouldMoveAll
        ? "CAJA_GRANDE_EXTRACCION_ALL"
        : "CAJA_GRANDE_EXTRACCION",
      side: "debit",
      accountCode: CAJA_SUPERADMIN,
      amount: amtAbs,
      currency,
      postedAt,

      fromUser: "CAJA_GRANDE",
      toUser: saName,
      fromAccountCode: CAJA_GRANDE,
      toAccountCode: CAJA_SUPERADMIN,

      dimensions: dims,
    };

    // ✅ CREDIT: sale de CAJA_GRANDE (dueño = CAJA_GRANDE) => INVERTIMOS para que toUser sea el dueño
    const creditDoc = {
      paymentId,
      userId: new mongoose.Types.ObjectId(String(sa._id)),
      kind: shouldMoveAll
        ? "CAJA_GRANDE_EXTRACCION_ALL"
        : "CAJA_GRANDE_EXTRACCION",
      side: "credit",
      accountCode: CAJA_GRANDE,
      amount: amtAbs,
      currency,
      postedAt,

      fromUser: saName,
      toUser: "CAJA_GRANDE",
      fromAccountCode: CAJA_SUPERADMIN,
      toAccountCode: CAJA_GRANDE,

      dimensions: dims,
    };

    const ins = await LedgerEntry.insertMany([debitDoc, creditDoc], {
      session,
      ordered: true,
    });

    await session.commitTransaction();

    return res.status(201).json({
      ok: true,
      movedAll: !!shouldMoveAll,
      amount: amtAbs,
      currency,
      paymentId: String(paymentId),
      entryIds: ins.map((x) => String(x._id)),
      executedBy: { userId: String(executorOid), name: executorName },
    });
  } catch (err) {
    try {
      await session.abortTransaction();
    } catch {}
    return next(err);
  } finally {
    session.endSession();
  }
}
export async function getGlobalCajasBalance(req, res, next) {
  try {
    if (String(req.user?.role || "").trim() !== "superAdmin") {
      return res.status(403).json({ ok: false, message: "Sólo superAdmin" });
    }

    const fromDt = parseISODate(req.query.dateFrom);
    const toDt = parseISODate(req.query.dateTo, true);

    const matchBase = (acc) => ({
      accountCode: acc,
      ...(fromDt || toDt
        ? {
            postedAt: {
              ...(fromDt ? { $gte: fromDt } : {}),
              ...(toDt ? { $lte: toDt } : {}),
            },
          }
        : {}),
    });

    const [grandeAgg, chicaAgg] = await Promise.all([
      LedgerEntry.aggregate([
        { $match: matchBase(CAJA_GRANDE) },
        { $group: { _id: null, debits: debitExpr, credits: creditExpr } },
        {
          $project: { _id: 0, balance: { $subtract: ["$debits", "$credits"] } },
        },
      ]).allowDiskUse(true),
      LedgerEntry.aggregate([
        { $match: matchBase(CAJA_CHICA) },
        { $group: { _id: null, debits: debitExpr, credits: creditExpr } },
        {
          $project: { _id: 0, balance: { $subtract: ["$debits", "$credits"] } },
        },
      ]).allowDiskUse(true),
    ]);

    const balances = {
      CAJA_GRANDE: Number(grandeAgg?.[0]?.balance || 0),
      CAJA_CHICA: Number(chicaAgg?.[0]?.balance || 0),
    };

    return res.json({
      ok: true,
      balances,
      dateFrom: req.query.dateFrom || null,
      dateTo: req.query.dateTo || null,
    });
  } catch (err) {
    next(err);
  }
}
export async function getArqueoGlobalTotals(req, res, next) {
  try {
    const accts = String(req.query.accountCodes || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    if (!accts.length) {
      return res
        .status(400)
        .json({ ok: false, message: "accountCodes requerido" });
    }

    const fromDt = parseISODate(req.query.dateFrom);
    const toDt = parseISODate(req.query.dateTo, true);

    const match = {
      accountCode: { $in: accts },
      ...(fromDt || toDt
        ? {
            postedAt: {
              ...(fromDt ? { $gte: fromDt } : {}),
              ...(toDt ? { $lte: toDt } : {}),
            },
          }
        : {}),
    };

    const [totals] = await LedgerEntry.aggregate([
      { $match: match },
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
    ]).allowDiskUse(true);

    res.json({
      ok: true,
      totals: totals || {
        debits: 0,
        credits: 0,
        balance: 0,
        lastMovementAt: null,
        paymentsCount: 0,
      },
      filters: {
        accountCodes: accts,
        dateFrom: req.query.dateFrom || null,
        dateTo: req.query.dateTo || null,
      },
    });
  } catch (err) {
    next(err);
  }
}
export async function getCollectorCommissionSummaryAdmin(req, res) {
  try {
    const {
      userId,
      idCobrador,
      dateFrom,
      dateTo,
      currency = "ARS", // CAMBIO: asegurar resumen por moneda (evita mezclar ARS/USD)
    } = req.query;

    const viewerRole = String(req.user?.role || "").trim();
    if (!["admin", "superAdmin"].includes(viewerRole)) {
      return res.status(403).json({
        ok: false,
        message: "Solo admin / superAdmin pueden ver resumen de comisiones.",
      });
    }

    // ───────────────── Buscar cobrador ─────────────────
    let collectorUser = null;

    if (userId && isObjectIdLike(userId)) {
      collectorUser = await User.findById(userId)
        .select(
          "_id name email role idCobrador porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .lean();
    } else if (idCobrador != null) {
      const idCobStr = String(idCobrador);
      collectorUser = await User.findOne({
        role: "cobrador",
        idCobrador: idCobStr,
      })
        .select(
          "_id name email role idCobrador porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .lean();
    }

    if (!collectorUser) {
      return res
        .status(404)
        .json({ ok: false, message: "No se encontró el cobrador indicado." });
    }

    if (collectorUser.role !== "cobrador") {
      return res
        .status(400)
        .json({ ok: false, message: "El usuario indicado no es cobrador." });
    }

    const collectorOid = new mongoose.Types.ObjectId(String(collectorUser._id));
    const collectorOidStr = String(collectorOid);

    const myCollectorId = Number(collectorUser.idCobrador);
    if (!Number.isFinite(myCollectorId) || myCollectorId <= 0) {
      return res.status(400).json({
        ok: false,
        message: "El cobrador no tiene un idCobrador numérico válido.",
      });
    }

    // ───────────────── Rango fechas / período ─────────────────
    const now = new Date();

    let rangeStart = normalizeDateStart(dateFrom);
    let rangeEnd = normalizeDateEnd(dateTo);

    if (!rangeStart || !rangeEnd) {
      const y = now.getFullYear();
      const m = now.getMonth();
      rangeStart = new Date(y, m, 1, 0, 0, 0, 0);
      rangeEnd = new Date(y, m + 1, 0, 23, 59, 59, 999);
    }

    const period = yyyymmAR(rangeStart);
    const year = rangeStart.getFullYear();
    const month = rangeStart.getMonth();
    const label = `${monthNamesEs[month] || "Mes"} ${year}`.replace(
      /^\w/,
      (c) => c.toUpperCase()
    );

    const daysInPeriod = new Date(year, month + 1, 0).getDate();
    const daysElapsed =
      now.getFullYear() === year && now.getMonth() === month
        ? now.getDate()
        : daysInPeriod;
    const daysRemaining = Math.max(daysInPeriod - daysElapsed, 0);

    const countWorkingDays = () => {
      let total = 0;
      let elapsed = 0;

      for (let d = 1; d <= daysInPeriod; d++) {
        const dt = new Date(year, month, d);
        const day = dt.getDay();
        const isWorking = day >= 1 && day <= 6;
        if (!isWorking) continue;

        total++;
        if (
          now.getFullYear() === year &&
          now.getMonth() === month &&
          d <= now.getDate()
        )
          elapsed++;
        else if (now.getFullYear() > year || now.getMonth() > month)
          elapsed = total;
      }
      return { total, elapsed, remaining: Math.max(total - elapsed, 0) };
    };

    const {
      total: workingDaysTotal,
      elapsed: workingDaysElapsed,
      remaining: workingDaysRemaining,
    } = countWorkingDays();

    // ───────────────── Config comisión ─────────────────
    let baseCommissionRate = 0;
    let graceDays = 7;
    let penaltyPerDay = 0;

    const rawPercent = collectorUser.porcentajeCobrador;
    if (typeof rawPercent === "number" && rawPercent > 0) {
      baseCommissionRate = rawPercent <= 1 ? rawPercent : rawPercent / 100;
    }

    if (
      collectorUser?.commissionGraceDays != null &&
      Number.isFinite(Number(collectorUser.commissionGraceDays))
    ) {
      graceDays = Number(collectorUser.commissionGraceDays);
    }

    const rawPenalty = collectorUser.commissionPenaltyPerDay;
    if (typeof rawPenalty === "number" && rawPenalty > 0) {
      penaltyPerDay = rawPenalty <= 1 ? rawPenalty : rawPenalty / 100;
    }

    // ───────────────── Clientes asignados + cuota vigente (esperado) ─────────────────
    const clientsAgg = await Cliente.aggregate([
      {
        $match: {
          $or: [
            { idCobrador: myCollectorId },
            { idCobrador: String(myCollectorId) },
          ],
        },
      },
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
      {
        $sort: {
          idCliente: 1,
          _rankTitular: 1,
          _rankIntegrante: 1,
          createdAtSafe: 1,
          _id: 1,
        },
      },
      {
        $group: {
          _id: "$idCliente",
          firstDoc: { $first: "$$ROOT" },
          cuotaVigente: { $first: "$_cuotaVigente" },
          isActive: { $max: "$__isActive" },
        },
      },
      { $match: { isActive: true } },
      {
        $group: {
          _id: null,
          assignedClients: { $sum: 1 },
          totalChargeNow: { $sum: "$cuotaVigente" },
        },
      },
    ]).allowDiskUse(true);

    const assignedClients = clientsAgg?.[0]?.assignedClients || 0;
    const totalChargeNow = clientsAgg?.[0]?.totalChargeNow || 0;

    // ───────────────── Movimientos cliente → cobrador (base real) ─────────────────
    // ✅ NUEVO ESQUEMA: lo que ENTRA a la caja del cobrador = CAJA_COBRADOR debit por userId del cobrador
    const ledgerAgg = await LedgerEntry.aggregate([
      {
        $match: {
          accountCode: "CAJA_COBRADOR",
          side: "debit",
          userId: collectorOid,
          currency, // CAMBIO: filtrar por moneda
          "dimensions.idCliente": { $exists: true, $ne: null },
          postedAt: { $gte: rangeStart, $lte: rangeEnd },
        },
      },
      {
        $addFields: {
          __dt: { $ifNull: ["$postedAt", "$createdAt"] },
          __pid: { $ifNull: ["$paymentId", "$_id"] },
        },
      },
      {
        $group: {
          _id: "$__pid",
          amount: { $sum: "$amount" },
          collectedAt: { $min: "$__dt" },
          clients: { $addToSet: "$dimensions.idCliente" },
        },
      },
    ]).allowDiskUse(true);

    const totalCollectedThisPeriod = (ledgerAgg || []).reduce(
      (s, r) => s + Number(r?.amount || 0),
      0
    );

    const clientsSet = new Set();
    for (const r of ledgerAgg || []) {
      for (const c of r?.clients || []) clientsSet.add(String(c));
    }

    const clientsWithPayment = clientsSet.size;
    const clientsWithoutPayment = Math.max(
      assignedClients - clientsWithPayment,
      0
    );

    // ───────────────── Saldo actual en mano del cobrador ─────────────────
    const cashAccounts = ["CAJA_COBRADOR", "A_RENDIR_COBRADOR"];

    const balanceAgg = await LedgerEntry.aggregate([
      {
        $match: {
          userId: collectorOid,
          accountCode: { $in: cashAccounts },
          currency, // CAMBIO: filtrar por moneda
        },
      },
      {
        $group: {
          _id: null,
          debits: debitExpr,
          credits: creditExpr,
        },
      },
      {
        $project: {
          _id: 0,
          balance: { $subtract: ["$debits", "$credits"] },
        },
      },
    ]).allowDiskUse(true);

    const collectorBalance = Number(balanceAgg?.[0]?.balance || 0);

    // ───────────────── Comisiones (NUEVO) ─────────────────
    const expectedCommission = totalChargeNow * baseCommissionRate;
    const totalCommissionNoPenalty =
      totalCollectedThisPeriod * baseCommissionRate;

    let totalCommission = 0;
    let totalPenaltyDaysApplied = 0;
    let avgEffectiveRate = baseCommissionRate;

    if (ledgerAgg?.length) {
      let weightedRateSum = 0;
      let weightedAmountSum = 0;

      for (const p of ledgerAgg) {
        const amt = Number(p?.amount || 0);
        const dt = p?.collectedAt ? new Date(p.collectedAt) : null;

        if (!amt || !dt || Number.isNaN(dt.getTime())) continue;

        const daysSince = diffInDays(dt, now);
        const penaltyDays = Math.max(
          0,
          Number(daysSince) - Number(graceDays || 0)
        );
        const effectiveRate = Math.max(
          0,
          baseCommissionRate - penaltyPerDay * penaltyDays
        );

        totalPenaltyDaysApplied += penaltyDays;
        totalCommission += amt * effectiveRate;

        weightedRateSum += effectiveRate * amt;
        weightedAmountSum += amt;
      }

      avgEffectiveRate =
        weightedAmountSum > 0
          ? weightedRateSum / weightedAmountSum
          : baseCommissionRate;
    }

    // ───────────────── Lo ya abonado (NUEVO) ─────────────────
    // ✅ FIX: ya NO existe dimensions.kind → usamos kind ROOT
    const paidAgg = await LedgerEntry.aggregate([
      {
        $match: {
          accountCode: "COMISION_COBRADOR",
          side: "debit",
          userId: collectorOid,
          currency, // CAMBIO: filtrar por moneda
          kind: "commission_payout", // FIX
          postedAt: { $gte: rangeStart, $lte: rangeEnd },
        },
      },
      { $group: { _id: null, totalPaid: { $sum: "$amount" } } },
    ]).allowDiskUse(true);

    const alreadyPaid = Number(paidAgg?.[0]?.totalPaid || 0);
    const pendingCommission = Math.max(totalCommission - alreadyPaid, 0);

    const rootAmounts = {
      expectedCommission,
      totalCommission,
      totalCommissionNoPenalty,
      alreadyPaid,
      pendingCommission,
    };

    return res.json({
      ok: true,
      data: {
        collector: {
          userId: collectorUser._id,
          name: collectorUser.name || null,
          email: collectorUser.email || null,
          idCobrador: myCollectorId,
        },
        month: {
          period,
          label,
          daysInPeriod,
          daysElapsed,
          daysRemaining,
          workingDaysTotal,
          workingDaysElapsed,
          workingDaysRemaining,
          totalChargeNow,
          totalCollectedThisPeriod,
          clientsWithPayment,
          clientsWithoutPayment,
        },
        balance: { collectorBalance },

        amounts: rootAmounts,

        commissions: {
          config: {
            basePercent: baseCommissionRate,
            graceDays,
            penaltyPerDay,
          },
          amounts: rootAmounts,
          meta: {
            avgEffectiveRate,
            totalPenaltyDaysApplied,
            paymentsBuckets: ledgerAgg?.length || 0,
          },
        },
        debug: {
          collectorOid: collectorOidStr,
          currency, // CAMBIO: sumar a debug para ver qué moneda se calculó
          rangeStart,
          rangeEnd,
        },
      },
    });
  } catch (err) {
    console.error("getCollectorCommissionSummaryAdmin error:", err);
    return res.status(500).json({
      ok: false,
      message: "Error al calcular la comisión del cobrador.",
      error: err?.message,
    });
  }
}
export async function payCollectorCommissionAdmin(req, res) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const {
      userId, // _id Mongo del cobrador (User)
      idCobrador, // opcional: idCobrador numérico
      dateFrom, // opcional: YYYY-MM-DD
      dateTo, // opcional: YYYY-MM-DD
      amount: amountRaw, // opcional
      currency = "ARS",
      note = "",
      sourceAccountCode, // opcional
    } = req.body || {};

    // ───────────────────────── Seguridad básica ─────────────────────────
    const viewerRole = String(req.user?.role || "").trim();
    if (!["admin", "superAdmin"].includes(viewerRole)) {
      await session.abortTransaction();
      return res.status(403).json({
        ok: false,
        message: "Solo admin / superAdmin pueden pagar comisiones.",
      });
    }

    // ───────────────── Cuenta/usuario ejecutor ─────────────────
    const sourceUserId = req.user?._id
      ? new mongoose.Types.ObjectId(String(req.user._id))
      : null;

    if (!sourceUserId) {
      await session.abortTransaction();
      return res
        .status(403)
        .json({ ok: false, message: "Sesión inválida para pagar comisión." });
    }

    // ✅ FIX: nombre del ejecutor SIEMPRE por DB (req.user a veces no trae name/email)
    const executorUser = await User.findById(sourceUserId)
      .select("_id name email role")
      .session(session)
      .lean();

    const executorName =
      String(req.user?.name || req.user?.email || "").trim() ||
      String(executorUser?.name || executorUser?.email || "").trim() ||
      "ADMINISTRACIÓN";

    // ───────────────────────── Buscar cobrador ─────────────────────────
    let collectorUser = null;

    if (userId && isObjectIdLike(userId)) {
      collectorUser = await User.findById(userId)
        .select(
          "_id name email role idCobrador porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .session(session)
        .lean();
    } else if (idCobrador != null) {
      const idCobStr = String(idCobrador);
      collectorUser = await User.findOne({
        role: "cobrador",
        idCobrador: idCobStr,
      })
        .select(
          "_id name email role idCobrador porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .session(session)
        .lean();
    }

    if (!collectorUser) {
      await session.abortTransaction();
      return res.status(404).json({
        ok: false,
        message: "No se encontró el cobrador indicado.",
      });
    }

    if (String(collectorUser.role || "") !== "cobrador") {
      await session.abortTransaction();
      return res.status(400).json({
        ok: false,
        message: "El usuario indicado no es cobrador.",
      });
    }

    const collectorOid = new mongoose.Types.ObjectId(String(collectorUser._id));

    const myCollectorId = Number(collectorUser.idCobrador);
    if (!Number.isFinite(myCollectorId) || myCollectorId <= 0) {
      await session.abortTransaction();
      return res.status(400).json({
        ok: false,
        message: "El cobrador no tiene un idCobrador numérico válido.",
      });
    }

    const collectorDisplayName =
      String(collectorUser.name || collectorUser.email || "").trim() ||
      `Cobrador #${myCollectorId}`;

    // ─────────────────────── Rango de fechas / período ───────────────────────
    const now = new Date();

    let rangeStart = normalizeDateStart(dateFrom);
    let rangeEnd = normalizeDateEnd(dateTo);

    if (!rangeStart || !rangeEnd) {
      const yearNow = now.getFullYear();
      const monthNow = now.getMonth();
      rangeStart = new Date(yearNow, monthNow, 1, 0, 0, 0, 0);
      rangeEnd = new Date(yearNow, monthNow + 1, 0, 23, 59, 59, 999);
    }

    const period = yyyymmAR(rangeStart); // "YYYY-MM"

    // ───────────────── Config de comisión (User) ─────────────────
    let baseCommissionRate = 0; // decimal

    const rawPercent = collectorUser.porcentajeCobrador;
    if (typeof rawPercent === "number" && rawPercent > 0) {
      baseCommissionRate = rawPercent <= 1 ? rawPercent : rawPercent / 100;
    }

    if (!baseCommissionRate) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message:
          "El cobrador no tiene porcentaje de comisión configurado (> 0).",
      });
    }

    // ───────────────── Base real de comisión (NUEVA lógica) ─────────────────
    const ledgerAgg = await LedgerEntry.aggregate([
      {
        $match: {
          accountCode: "CAJA_COBRADOR",
          side: "debit",
          userId: collectorOid,
          "dimensions.idCliente": { $exists: true, $ne: null },
          postedAt: { $gte: rangeStart, $lte: rangeEnd },
          currency,
        },
      },
      { $group: { _id: null, totalCollected: { $sum: "$amount" } } },
    ])
      .session(session)
      .allowDiskUse(true);

    const totalCollectedThisPeriod = Number(
      ledgerAgg?.[0]?.totalCollected || 0
    );
    const totalCommission = totalCollectedThisPeriod * baseCommissionRate;

    if (!Number.isFinite(totalCommission) || totalCommission <= 0) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message:
          "No hay base de cobros del cobrador para calcular comisión en este período.",
        details: {
          totalCollectedThisPeriod,
          commissionRate: baseCommissionRate,
        },
      });
    }

    // ───────────────── Lo ya pagado como comisión (NUEVA lógica) ─────────────────
    // ✅ usamos kind ROOT (no dimensions.kind)
    const paidAgg = await LedgerEntry.aggregate([
      {
        $match: {
          accountCode: "COMISION_COBRADOR",
          side: "debit",
          userId: collectorOid,
          kind: "commission_payout",
          postedAt: { $gte: rangeStart, $lte: rangeEnd },
          currency,
        },
      },
      { $group: { _id: null, totalPaid: { $sum: "$amount" } } },
    ])
      .session(session)
      .allowDiskUse(true);

    const alreadyPaid = Number(paidAgg?.[0]?.totalPaid || 0);
    const pendingCommission = Math.max(totalCommission - alreadyPaid, 0);

    if (pendingCommission <= 0) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message: "No hay comisión pendiente para pagar en el período indicado.",
        details: { totalCommission, alreadyPaid },
      });
    }

    // ───────────────── Cuánto pagar ahora ─────────────────
    let payNow = Number(amountRaw);
    const payAll =
      !Number.isFinite(payNow) || payNow <= 0 || payNow >= pendingCommission;

    if (payAll) payNow = pendingCommission;
    payNow = Number(payNow);

    if (!Number.isFinite(payNow) || payNow <= 0) {
      await session.abortTransaction();
      return res.status(400).json({
        ok: false,
        message: "Monto inválido para pagar comisión.",
      });
    }

    // ───────────────── Cuenta origen (admin/superAdmin) ─────────────────
    const allowedForAdmin = ["CAJA_ADMIN", "CAJA_CHICA"];
    const allowedForSuper = ["CAJA_CHICA", "CAJA_GRANDE", "CAJA_ADMIN"];

    const defaultSourceAccount =
      viewerRole === "superAdmin" ? "CAJA_GRANDE" : "CAJA_ADMIN";

    let fromAccount = String(sourceAccountCode || defaultSourceAccount).trim();
    const allowedAccounts =
      viewerRole === "superAdmin" ? allowedForSuper : allowedForAdmin;

    if (!allowedAccounts.includes(fromAccount)) {
      fromAccount = defaultSourceAccount;
    }

    // Chequeo de saldo en cuenta origen
    const sourceBalance = await getBalance({
      userId: sourceUserId,
      accountCode: fromAccount,
      currency,
    });

    if (Number(sourceBalance || 0) + 1e-6 < payNow) {
      await session.abortTransaction();
      return res.status(409).json({
        ok: false,
        message:
          "Saldo insuficiente en la cuenta origen para pagar la comisión.",
        available: sourceBalance,
        requested: payNow,
      });
    }

    // ───────────────── Asiento ledger (NEW ONLY) + idempotencia ─────────────────
    const postedAt = new Date();
    const amtAbs = Math.abs(Number(payNow) || 0);

    // ✅ idempotencia determinística por scope del pago
    const idemKey = `commission_payout:${String(
      collectorUser._id
    )}:${period}:${fromAccount}:${currency}:${amtAbs}`;

    // ✅ FIX: idempotencia por paymentId (no por note)
    // Buscamos cualquier asiento previo que tenga EXACTAMENTE este idemKey.
    // Como no tenemos campo idemKey en schema, lo guardamos dentro de dimensions.note con prefijo estable.
    const cleanNote = String(note || "").trim();
    const noteFinal = cleanNote ? `${cleanNote} | ${idemKey}` : idemKey;

    const dup = await LedgerEntry.findOne(
      {
        kind: "commission_payout",
        currency,
        // buscamos por dims.note exacto para evitar falsos positivos
        "dimensions.note": noteFinal,
      },
      { _id: 1, paymentId: 1 }
    )
      .session(session)
      .lean();

    let transferResult = null;
    let paidNowReal = amtAbs;

    if (dup) {
      // duplicado → no creamos nada
      paidNowReal = 0;
      transferResult = {
        ok: true,
        skipped: true,
        paymentId: dup.paymentId || null,
        entryIds: [dup._id],
        idemKey,
      };
    } else {
      const payoutId = new mongoose.Types.ObjectId();

      const dims = {
        idCobrador: myCollectorId,
        idCliente: null,
        plan: null,
        canal: "COMMISSION_PAYOUT",
        note: noteFinal,
      };

      // ✅ CAMBIO: userId por línea = dueño de la cuenta afectada
      // - DEBIT suma COMISION_COBRADOR al cobrador (userId=cobrador)
      // - CREDIT resta fromAccount al admin/super (userId=admin/super)

      const debitDoc = {
        paymentId: payoutId,
        userId: collectorOid,
        kind: "commission_payout",
        side: "debit",
        accountCode: "COMISION_COBRADOR",
        amount: amtAbs,
        currency,
        postedAt,

        // origen → destino
        fromUser: executorName,
        toUser: collectorDisplayName,
        fromAccountCode: fromAccount,
        toAccountCode: "COMISION_COBRADOR",

        dimensions: dims,
      };

      const creditDoc = {
        paymentId: payoutId,
        userId: sourceUserId,
        kind: "commission_payout",
        side: "credit",
        accountCode: fromAccount,
        amount: amtAbs,
        currency,
        postedAt,

        // inverso para UI
        fromUser: collectorDisplayName,
        toUser: executorName,
        fromAccountCode: "COMISION_COBRADOR",
        toAccountCode: fromAccount,

        dimensions: dims,
      };

      const docs = await LedgerEntry.insertMany([debitDoc, creditDoc], {
        session,
        ordered: true,
      });

      transferResult = {
        ok: true,
        skipped: false,
        paymentId: payoutId,
        entryIds: docs.map((d) => d._id),
        idemKey,
      };
    }

    const newAlreadyPaid = alreadyPaid + paidNowReal;
    const pendingAfter = Math.max(totalCommission - newAlreadyPaid, 0);

    await session.commitTransaction();

    return res.status(201).json({
      ok: true,
      message: transferResult?.skipped
        ? "Pago duplicado detectado (idempotencia): no se creó un nuevo asiento."
        : "Comisión abonada al cobrador.",
      data: {
        collector: {
          userId: collectorUser._id,
          idCobrador: myCollectorId,
          name: collectorUser.name || null,
          email: collectorUser.email || null,
        },
        period,
        range: { from: rangeStart, to: rangeEnd },
        amounts: {
          totalCommission,
          totalCommissionNoPenalty: totalCommission,
          alreadyPaidBefore: alreadyPaid,
          paidNow: paidNowReal,
          alreadyPaidAfter: newAlreadyPaid,
          pendingAfter,
        },
        commissions: {
          config: { basePercent: baseCommissionRate },
          amounts: {
            totalCommission,
            totalCommissionNoPenalty: totalCommission,
            alreadyPaid: newAlreadyPaid,
            pendingCommission: pendingAfter,
          },
        },
        ledger: transferResult,
        source: {
          userId: sourceUserId,
          accountCode: fromAccount,
          balanceBefore: sourceBalance,
          balanceAfter: Number(sourceBalance || 0) - paidNowReal,
        },
        executedBy: {
          userId: sourceUserId,
          name: executorName,
        },
        meta: {
          payAll: payAll ? 1 : 0,
          requestedAmount: Number.isFinite(Number(amountRaw))
            ? Number(amountRaw)
            : null,
        },
      },
    });
  } catch (err) {
    try {
      await session.abortTransaction();
    } catch {}
    console.error("payCollectorCommissionAdmin error:", err);
    return res.status(500).json({
      ok: false,
      message: "Error al pagar la comisión del cobrador.",
      error: err?.message,
    });
  } finally {
    session.endSession();
  }
}
