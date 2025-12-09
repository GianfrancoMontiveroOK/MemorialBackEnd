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
// ⬇️ Colocá esto arriba del archivo (o importá tu util real)
function yyyymmAR(date) {
  const d = date instanceof Date ? date : new Date(date);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  return `${y}-${m}`; // formato "YYYY-MM" como espera allocations.period
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

// Conjunto por defecto para superAdmin (incluye globales + su billetera personal)
const SUPERADMIN_ACCOUNTS = [CAJA_GRANDE, CAJA_SUPERADMIN, CAJA_CHICA];

// Roles con caja
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
// Helper local para validar si algo "parece" un ObjectId
const isObjectIdLike = (v) => {
  if (!v) return false;
  try {
    // fuerza a string por las dudas
    new mongoose.Types.ObjectId(String(v));
    return true;
  } catch {
    return false;
  }
};

// 🔧 Match unificado por usuario y cuentas
// - admin / superAdmin: match estricto por userId
// - cobrador: match por userId|fromUserId|toUserId ó por dimensions.idCobrador/cobradorId
function buildUserLedgerMatch({ user, accounts, fromDt, toDt, side }) {
  const uid = new mongoose.Types.ObjectId(String(user._id));

  const byUserId = [
    { userId: uid },
    { "dimensions.fromUserId": uid },
    { "dimensions.toUserId": uid },
  ];

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

  // Base por cuentas
  const match = { accountCode: { $in: accounts } };

  if (user.role === "admin" || user.role === "superAdmin") {
    // Vinculación estricta por userId (las globales se consultan por endpoints específicos)
    match.userId = uid;
  } else {
    // Cobrador: laxo
    match.$or = [...byUserId, ...byCollectorId];
  }

  if (side === "debit" || side === "credit") match.side = side;

  if (fromDt || toDt) {
    match.postedAt = {};
    if (fromDt) match.postedAt.$gte = fromDt;
    if (toDt) match.postedAt.$lte = toDt;
  }

  return match;
}

async function createLedgerTransfer({
  from, // { userId, accountCode }
  to, // { userId, accountCode }
  amount,
  currency = "ARS",
  kind, // ej. "CAJA_CHICA_DEPOSITO"
  note = "",
  extraDims = {}, // { idCobrador, dateFrom, dateTo, performedBy, ... }
  idemScope = "", // string para idempotencia contextual
}) {
  if (!from?.userId || !to?.userId) {
    throw new Error("createLedgerTransfer: faltan userId origen/destino");
  }
  if (!from?.accountCode || !to?.accountCode) {
    throw new Error("createLedgerTransfer: faltan cuentas origen/destino");
  }
  const amt = Number(amount);
  if (!Number.isFinite(amt) || amt <= 0) {
    throw new Error("createLedgerTransfer: monto inválido");
  }

  const now = new Date();
  const paymentId = new mongoose.Types.ObjectId();
  const idemKey = [
    "xfer",
    kind || "GENERIC",
    String(from.userId),
    from.accountCode,
    String(to.userId),
    to.accountCode,
    currency,
    Math.floor(now.getTime() / 60000), // granularidad minuto
    idemScope || "",
  ].join(":");

  // Evitar duplicados (chequea línea destino DEBIT con el idemKey)
  const exists = await LedgerEntry.findOne({
    "dimensions.idemKey": idemKey,
    userId: to.userId,
    accountCode: to.accountCode,
    side: "debit",
    currency,
  }).lean();
  if (exists) return { ok: true, skipped: true, paymentId, idemKey };

  const dims = {
    kind: kind || "GENERIC",
    note,
    idemKey,
    fromUserId: from.userId,
    toUserId: to.userId,
    ...extraDims,
  };

  const creditOut = {
    paymentId,
    userId: from.userId,
    accountCode: from.accountCode,
    side: "credit",
    amount: amt,
    currency,
    postedAt: now,
    dimensions: dims,
  };

  const debitIn = {
    paymentId,
    userId: to.userId,
    accountCode: to.accountCode,
    side: "debit",
    amount: amt,
    currency,
    postedAt: now,
    dimensions: dims,
  };

  const entries = await LedgerEntry.insertMany([creditOut, debitIn], {
    ordered: true,
  });

  return { ok: true, paymentId, idemKey, entryIds: entries.map((e) => e._id) };
}

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

// Opcional: exportar lo que uses desde otros módulos
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
  buildUserLedgerMatch,
  createLedgerTransfer,
  getBalance,
};

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
    const roleFilter = String(req.query.role || "").trim(); // "admin" | "superAdmin" | "cobrador" | "global" | ""
    const sortByParam = String(req.query.sortBy || "totalBalance");
    const sortDirParam = toDir(req.query.sortDir || "desc"); // 1 = asc, -1 = desc
    const orderMode = String(req.query.orderMode || "default"); // "default" | "hierarchy"

    const viewerRole = String(req.user?.role || "").trim();

    // Quiénes pueden listar "usuarios" reales (las cajas globales se inyectan aparte)
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

    // ---------- Filtro de usuarios reales
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

    // ---------- Pipeline base (no paginamos aún; inyectamos globales y luego paginamos)
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
                    // 1) Cuentas por rol (si no hay override)
                    {
                      $in: [
                        "$accountCode",
                        accountCodesOverride
                          ? accountCodesOverride
                          : {
                              $cond: [
                                // admin => sólo cuenta destino (p.ej. CAJA_ADMIN o DEST_DEFAULT)
                                { $eq: ["$$urole", "admin"] },
                                [destAccountCode],
                                {
                                  $cond: [
                                    // superAdmin => SOLO personal: CAJA_SUPERADMIN (NUNCA GRANDE acá)
                                    { $eq: ["$$urole", "superAdmin"] },
                                    ["CAJA_SUPERADMIN"],
                                    // cobrador => cuentas por defecto
                                    DEFAULT_ACCOUNTS,
                                  ],
                                },
                              ],
                            },
                      ],
                    },

                    // 2) Vinculación por usuario:
                    //    - admin/superAdmin: SOLO userId == uid
                    //    - cobrador: userId|fromUserId|toUserId == uid  OR  dimensions.idCobrador/cobradorId == idCob
                    {
                      $cond: [
                        { $in: ["$$urole", ["admin", "superAdmin"]] },
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

    const dataPipeline = [...base, sortStage];
    const countPipeline = [{ $match: userMatch }, { $count: "n" }];

    const [realItems, countRes] = await Promise.all([
      User.aggregate(dataPipeline).allowDiskUse(true),
      User.aggregate(countPipeline).allowDiskUse(true),
    ]);

    const totalReal = countRes?.[0]?.n || 0;

    // ---------- Inyección de "usuarios virtuales" (cajas globales) cuando el viewer es SA
    let merged = realItems;
    let globalsAdded = 0;

    const includeGlobals =
      viewerRole === "superAdmin" &&
      (!roleFilter || roleFilter === "" || roleFilter === "global");

    if (includeGlobals) {
      const matchDate =
        fromDt || toDt
          ? {
              postedAt: {
                ...(fromDt ? { $gte: fromDt } : {}),
                ...(toDt ? { $lte: toDt } : {}),
              },
            }
          : {};

      const makeGlobalBoxes = async (accCode) => {
        const boxes = await LedgerEntry.aggregate([
          { $match: { accountCode: accCode, ...matchDate } },
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
        ]);

        const totalBalance = (boxes || []).reduce(
          (s, b) => s + Number(b?.balance || 0),
          0
        );
        const lastMovementAt = (boxes || []).reduce((max, b) => {
          const d = b?.lastMovementAt ? new Date(b.lastMovementAt) : null;
          return d && !isNaN(d) && (!max || d > max) ? d : max;
        }, null);
        const paymentsCount = (boxes || []).reduce(
          (s, b) => s + Number(b?.paymentsCount || 0),
          0
        );

        return { boxes, totalBalance, lastMovementAt, paymentsCount };
      };

      // ⬅️ AHORA traemos también BANCO_NACION y TARJETA_NARANJA
      const [chica, grande, bancoNacion, tarjetaNaranja] = await Promise.all([
        makeGlobalBoxes("CAJA_CHICA"),
        makeGlobalBoxes("CAJA_GRANDE"),
        makeGlobalBoxes("BANCO_NACION"),
        makeGlobalBoxes("TARJETA_NARANJA"),
      ]);

      const globalRows = [
        {
          _id: "GLOBAL:CAJA_GRANDE",
          name: "CAJA_GRANDE (GLOBAL)",
          email: "",
          role: "global",
          boxes: grande.boxes,
          totalBalance: grande.totalBalance,
          lastMovementAt: grande.lastMovementAt,
          paymentsCount: grande.paymentsCount,
          isGlobal: true,
          globalCode: "CAJA_GRANDE",
        },
        {
          _id: "GLOBAL:CAJA_CHICA",
          name: "CAJA_CHICA (GLOBAL)",
          email: "",
          role: "global",
          boxes: chica.boxes,
          totalBalance: chica.totalBalance,
          lastMovementAt: chica.lastMovementAt,
          paymentsCount: chica.paymentsCount,
          isGlobal: true,
          globalCode: "CAJA_CHICA",
        },
        {
          _id: "GLOBAL:BANCO_NACION",
          name: "BANCO_NACION (GLOBAL)",
          email: "",
          role: "global",
          boxes: bancoNacion.boxes,
          totalBalance: bancoNacion.totalBalance,
          lastMovementAt: bancoNacion.lastMovementAt,
          paymentsCount: bancoNacion.paymentsCount,
          isGlobal: true,
          globalCode: "BANCO_NACION",
        },
        {
          _id: "GLOBAL:TARJETA_NARANJA",
          name: "TARJETA_NARANJA (GLOBAL)",
          email: "",
          role: "global",
          boxes: tarjetaNaranja.boxes,
          totalBalance: tarjetaNaranja.totalBalance,
          lastMovementAt: tarjetaNaranja.lastMovementAt,
          paymentsCount: tarjetaNaranja.paymentsCount,
          isGlobal: true,
          globalCode: "TARJETA_NARANJA",
        },
      ];

      const matchesQ = (row) =>
        !q ||
        String(row.name || "")
          .toLowerCase()
          .includes(q.toLowerCase()) ||
        String(row._id || "")
          .toLowerCase()
          .includes(q.toLowerCase());

      const filteredGlobals = globalRows.filter(matchesQ);
      globalsAdded = filteredGlobals.length;

      merged = [...realItems, ...filteredGlobals];
    }

    // ---------- Orden final (jerárquico si se solicita)
    function orderRank(row) {
      const id = String(row?._id || "");
      const role = String(row?.role || "");

      if (id === "GLOBAL:CAJA_GRANDE") return 0;
      if (id === "GLOBAL:CAJA_CHICA") return 1;
      if (id === "GLOBAL:BANCO_NACION") return 2;
      if (id === "GLOBAL:TARJETA_NARANJA") return 3;
      if (role === "superAdmin") return 4;
      if (role === "admin") return 5;
      if (role === "cobrador") return 6;
      return 7;
    }

    const finalSorted =
      orderMode === "hierarchy"
        ? [...merged].sort((a, b) => {
            const ra = orderRank(a);
            const rb = orderRank(b);
            if (ra !== rb) return ra - rb;

            // Dentro del bloque, usamos el sort original pedido por query
            const av = a[sortBy] ?? (sortBy === "name" ? "" : 0);
            const bv = b[sortBy] ?? (sortBy === "name" ? "" : 0);
            if (av < bv) return sortDirParam;
            if (av > bv) return -sortDirParam;
            return String(a._id).localeCompare(String(b._id)) * sortDirParam;
          })
        : merged;

    // ---------- Paginado final
    const total = totalReal + globalsAdded;
    const start = (page - 1) * limit;
    const end = start + limit;
    const paged = finalSorted.slice(start, end);

    return res.json({
      ok: true,
      items: paged,
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
        accountCodes: accountCodesOverride || {
          superAdmin: ["CAJA_SUPERADMIN"], // <- SA no suma GRANDE aquí
          admin: [destAccountCode],
          cobrador: DEFAULT_ACCOUNTS,
        },
        dateFrom: req.query.dateFrom || null,
        dateTo: req.query.dateTo || null,
        q,
        destAccountCode,
        injectedGlobals:
          viewerRole === "superAdmin"
            ? ["CAJA_CHICA", "CAJA_GRANDE", "BANCO_NACION", "TARJETA_NARANJA"]
            : [],
        orderMode,
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

    /* ────────────────────── RAMA: CAJAS GLOBALES ──────────────────────
       IDs como: "GLOBAL:CAJA_GRANDE", "GLOBAL:BANCO_NACION", etc.
       No hay usuario real, filtramos directamente por accountCode.
    ------------------------------------------------------------------ */
    const isGlobal = rawUserId.startsWith("GLOBAL:");

    if (isGlobal) {
      const globalCode = rawUserId.replace(/^GLOBAL:/, "").trim();

      // Cuentas efectivas para la caja global:
      // - Si viene override, se respetan.
      // - Si no, se toma el código global de la fila.
      const effectiveAccounts =
        accountCodesOverride.length > 0 ? accountCodesOverride : [globalCode];

      // Match básico por cuenta, fechas y side (si viene)
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

      // "Usuario" virtual para header
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

    /* ────────────────────── RAMA: USUARIOS REALES ────────────────────── */

    // Resolver usuario real
    let user = null;
    if (rawUserId) {
      user = await User.findById(rawUserId)
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
        ? [destAccountCode] // CAJA_ADMIN (u otra dest)
        : user.role === "superAdmin"
        ? SUPERADMIN_ACCOUNTS // ⬅️ CAJA_GRANDE + CAJA_SUPERADMIN (personal del SA)
        : DEFAULT_ACCOUNTS; // cobrador

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
        isGlobal: false,
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

    // 4) Saldo por moneda del cobrador (match coherente con listados)
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

    // 5) Asientos idempotentes — userId correcto por línea
    const now = new Date();
    const created = [];

    for (const row of positives) {
      const { currency, balance } = row;

      const idemKey = `arqueo:${String(cobradorUser._id)}:${String(
        adminUserId
      )}:${destAcct}:${currency}:${Math.floor(now.getTime() / 60000)}`;

      // idempotencia: chequeo en la línea destino (debit)
      const exists = await LedgerEntry.findOne({
        "dimensions.idemKey": idemKey,
        accountCode: destAcct,
        side: "debit",
        currency,
        userId: adminUserId,
      }).lean();
      if (exists) continue;

      const syntheticPaymentId = new mongoose.Types.ObjectId();

      const dimBase = {
        idCobrador: Number(cobradorUser.idCobrador ?? idCobrador) ?? null,
        performedBy: adminUserId,
        note: note || "",
        dateFrom: df || null,
        dateTo: dt || null,
        kind: "ARQUEO_MANUAL",
        idemKey,
      };

      // ⚠️ LÍNEA 1: CREDIT en CAJA_COBRADOR
      // - userId = admin (ejecutor) para que el listado muestre Admin → Cobrador
      // - vinculamos al cobrador por dimensions.idCobrador
      const creditOut = {
        paymentId: syntheticPaymentId,
        userId: adminUserId, // ← clave para naming correcto
        accountCode: "CAJA_COBRADOR",
        side: "credit",
        amount: balance,
        currency,
        postedAt: now,
        dimensions: {
          ...dimBase,
          fromUserId: adminUserId, // dirección: Admin → Cobrador (vista)
          toUserId: cobradorUser._id,
        },
      };

      // LÍNEA 2: DEBIT en cuenta destino (p.ej. CAJA_ADMIN) con userId = admin
      const debitIn = {
        paymentId: syntheticPaymentId,
        userId: adminUserId,
        accountCode: destAcct,
        side: "debit",
        amount: balance,
        currency,
        postedAt: now,
        dimensions: {
          ...dimBase,
          fromUserId: cobradorUser._id, // vista: Cobrador → Admin en CAJA_ADMIN (debit)
          toUserId: adminUserId,
        },
      };

      const entries = await LedgerEntry.insertMany([creditOut, debitIn], {
        ordered: true,
      });

      created.push({
        currency,
        balance,
        from: { userId: adminUserId, accountCode: "CAJA_COBRADOR" }, // (owner lógico mostrado)
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
              { $subtract: [NOW_NUM, 1] }, // si nunca pagó, consideramos "al mes anterior"
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
                  { $gt: ["$__maxNum", NOW_NUM] }, // tiene meses por adelantado
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

    // ⚠️ usar el modelo correcto: Cliente (no Client)
    const [items, totalRes] = await Promise.all([
      Cliente.aggregate(pipeline).allowDiskUse(true), // <- antes: Client.aggregate
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
    // log útil para depurar en server
    console.error("listArqueoUsuarioClientes error:", err);
    next(err);
  }
}
// GET /api/admin/arqueos/usuarios/clientes-csv
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
      // match por cobrador (sin rol)
      {
        $match: {
          $expr: { $eq: [{ $toString: "$idCobrador" }, String(cid)] },
        },
      },

      // normalizaciones + flags
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
          // rank: prioriza TITULAR (case-insensitive), luego integrante 0, luego el resto
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

      // ordenar para elegir representante del grupo (TITULAR primero)
      {
        $sort: {
          idCliente: 1,
          __rankTitular: 1,
          __rankIntegrante: 1,
          createdAt: 1,
          _id: 1,
        },
      },

      // agrupar por grupo; contar activos para aplicar activeOnly
      {
        $group: {
          _id: "$idCliente",
          rep: { $first: "$$ROOT" },
          activosEnGrupo: { $sum: { $cond: ["$__isActiveMember", 1, 0] } },
        },
      },

      // si activeOnly=1, quedate solo con grupos que tengan al menos 1 activo
      ...(activeOnly ? [{ $match: { activosEnGrupo: { $gt: 0 } } }] : []),

      // proyección CSV
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

    // ── CSV
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

/**
 * POST /api/caja/chica/deposito
 * body: { adminUserId, currency?, note? }
 * Permisos: admin (sobre sí mismo) o superAdmin (sobre cualquier admin)
 * Lógica: mueve TODO el saldo de CAJA_ADMIN → CAJA_CHICA del admin.
 */
export async function depositoCajaChica(req, res, next) {
  try {
    const { adminUserId, currency = "ARS", note = "" } = req.body || {};
    if (!adminUserId) {
      return res.status(400).json({ ok: false, message: "Falta adminUserId" });
    }

    const admin = await User.findById(adminUserId).select("_id role").lean();
    if (!admin || admin.role !== "admin") {
      return res
        .status(400)
        .json({ ok: false, message: "adminUserId inválido" });
    }

    // Seguridad: el propio admin o un superAdmin
    const viewerRole = String(req.user?.role || "");
    const isSelf = String(req.user?._id) === String(admin._id);
    if (!(isSelf || viewerRole === "superAdmin")) {
      return res.status(403).json({ ok: false, message: "Sin permisos" });
    }

    // 👉 Mover TODO el saldo de CAJA_ADMIN
    const saldoAdmin = await getBalance({
      userId: admin._id,
      accountCode: DEST_DEFAULT, // CAJA_ADMIN
      currency,
    });

    const amount = Number(saldoAdmin || 0);
    if (amount <= 0) {
      return res.status(409).json({
        ok: false,
        message: "No hay saldo disponible en CAJA_ADMIN para mover.",
        balance: saldoAdmin,
      });
    }

    const out = await createLedgerTransfer({
      from: { userId: admin._id, accountCode: DEST_DEFAULT }, // CAJA_ADMIN
      to: { userId: admin._id, accountCode: CAJA_CHICA },
      amount,
      currency,
      kind: "CAJA_CHICA_DEPOSITO_ALL",
      note,
      idemScope: `ALL:${admin._id}`,
      extraDims: { performedBy: req.user?._id || null },
    });

    return res.status(201).json({ ok: true, movedAll: true, amount, ...out });
  } catch (err) {
    next(err);
  }
}

/**
 * POST /api/caja/grande/ingreso
 * body: { amount?, moveAll?, currency="ARS", toSuperAdminUserId? }
 * Permisos: sólo superAdmin
 * Lógica: desde CAJA_CHICA (GLOBAL) → CAJA_GRANDE (del SA). Monto o TODO.
 */
export async function ingresoCajaGrande(req, res, next) {
  try {
    if (String(req.user?.role || "") !== "superAdmin") {
      return res.status(403).json({ ok: false, message: "Sólo superAdmin" });
    }

    const {
      amount: amountRaw,
      moveAll,
      currency = "ARS",
      toSuperAdminUserId,
      note = "",
    } = req.body || {};

    // Dueño de la bóveda (CAJA_GRANDE)
    const vaultOwnerId = toSuperAdminUserId || req.user._id;
    const sa = await User.findById(vaultOwnerId).select("_id role").lean();
    if (!sa || sa.role !== "superAdmin") {
      return res
        .status(400)
        .json({ ok: false, message: "toSuperAdminUserId inválido" });
    }

    // 1) Sumar CHICA como GLOBAL: por accountCode+currency, sin filtrar userId
    const byAdmin = await LedgerEntry.aggregate([
      { $match: { accountCode: CAJA_CHICA, currency } },
      { $group: { _id: "$userId", debits: debitExpr, credits: creditExpr } },
      {
        $project: {
          userId: "$_id",
          _id: 0,
          balance: { $subtract: ["$debits", "$credits"] },
        },
      },
      { $match: { balance: { $gt: 0 } } },
      { $sort: { balance: -1 } }, // dreno simple: primero el que más tiene
    ]);

    const totalAvailable = byAdmin.reduce(
      (acc, it) => acc + Number(it.balance || 0),
      0
    );

    let requested = Number(amountRaw);
    const wantAll = String(moveAll || "") === "1" || !Number(requested);
    if (wantAll) requested = totalAvailable;

    if (!Number.isFinite(requested) || requested <= 0) {
      return res.status(409).json({
        ok: false,
        message: "No hay saldo disponible en CAJA_CHICA (GLOBAL) para mover.",
        available: totalAvailable,
        requestedAll: wantAll ? 1 : 0,
      });
    }
    if (requested > totalAvailable + 1e-9) {
      return res.status(409).json({
        ok: false,
        message: `El monto solicitado excede el saldo en CAJA_CHICA (GLOBAL).`,
        available: totalAvailable,
        requested,
      });
    }

    // 2) Transferir CHICA(admin) -> GRANDE(SA) hasta cubrir 'requested'
    let remaining = requested;
    const transfers = []; // no exponemos nombres; sólo guardamos ids/montos si querés audit
    for (const it of byAdmin) {
      if (remaining <= 0) break;
      const move = Math.min(remaining, Number(it.balance || 0));
      if (move <= 0) continue;

      const out = await createLedgerTransfer({
        from: { userId: it.userId, accountCode: CAJA_CHICA },
        to: { userId: sa._id, accountCode: CAJA_GRANDE },
        amount: move,
        currency,
        kind: wantAll ? "CAJA_GRANDE_INGRESO_ALL" : "CAJA_GRANDE_INGRESO",
        note,
        extraDims: { performedBy: req.user._id, scope: "GLOBAL_CHICA" },
        idemScope: `CHICA_AGREGADA->GRANDE:${String(
          sa._id
        )}:${currency}:${move}:${String(it.userId)}`,
      });

      transfers.push({ moved: move, id: out?.id || null });
      remaining -= move;
    }

    return res.status(201).json({
      ok: true,
      movedAll: wantAll,
      amount: requested,
      currency,
      transfersCount: transfers.length,
    });
  } catch (err) {
    next(err);
  }
}

/**
 * POST /api/caja/grande/extraccion
 * body: { amount?, moveAll?, currency?, note?, superAdminUserId? }
 * Permisos: sólo superAdmin
 * Lógica: si moveAll=1 (o no se pasa amount), mueve TODO desde CAJA_GRANDE → CAJA_SUPERADMIN del SA.
 */
export async function extraccionCajaGrande(req, res, next) {
  try {
    const role = String(req.user?.role || "");
    if (role !== "superAdmin") {
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
    const sa = await User.findById(ownerId).select("_id role").lean();
    if (!sa || sa.role !== "superAdmin") {
      return res
        .status(400)
        .json({ ok: false, message: "superAdminUserId inválido" });
    }

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
      return res.status(409).json({
        ok: false,
        message: "No hay saldo disponible en CAJA_GRANDE para extraer.",
        requestedAll: shouldMoveAll ? 1 : 0,
      });
    }

    const out = await createLedgerTransfer({
      from: { userId: sa._id, accountCode: CAJA_GRANDE },
      to: { userId: sa._id, accountCode: CAJA_SUPERADMIN },
      amount,
      currency,
      kind: shouldMoveAll
        ? "CAJA_GRANDE_EXTRACCION_ALL"
        : "CAJA_GRANDE_EXTRACCION",
      note,
      extraDims: { performedBy: req.user._id },
      idemScope: `${shouldMoveAll ? "ALL:" : ""}${sa._id}`,
    });

    return res.status(201).json({
      ok: true,
      movedAll: shouldMoveAll ? true : false,
      amount,
      ...out,
    });
  } catch (err) {
    next(err);
  }
}

export async function getGlobalCajasBalance(req, res, next) {
  try {
    // Sólo superAdmin puede ver globales
    if (String(req.user?.role || "") !== "superAdmin") {
      return res.status(403).json({ ok: false, message: "Sólo superAdmin" });
    }

    const fromDt = parseISODate(req.query.dateFrom);
    const toDt = parseISODate(req.query.dateTo, true);

    // CAJA_GRANDE global = todas las entradas con accountCode CAJA_GRANDE (cualquier userId)
    // CAJA_CHICA  global = todas las entradas con accountCode CAJA_CHICA  (cualquier userId)
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
      ]),
      LedgerEntry.aggregate([
        { $match: matchBase(CAJA_CHICA) },
        { $group: { _id: null, debits: debitExpr, credits: creditExpr } },
        {
          $project: { _id: 0, balance: { $subtract: ["$debits", "$credits"] } },
        },
      ]),
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
    ]);

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
      userId, // _id Mongo del cobrador (User)
      idCobrador, // opcional: idCobrador numérico
      dateFrom, // opcional: YYYY-MM-DD
      dateTo, // opcional: YYYY-MM-DD
    } = req.query;

    // ───────────────────────── Seguridad básica ─────────────────────────
    const viewerRole = String(req.user?.role || "").trim();
    if (!["admin", "superAdmin"].includes(viewerRole)) {
      return res.status(403).json({
        ok: false,
        message: "Solo admin / superAdmin pueden ver resumen de comisiones.",
      });
    }

    // Helper local para días entre fechas (A → B, en enteros)
    const diffInDays = (from, to) => {
      if (!from || !to) return 0;
      const a = new Date(from);
      const b = new Date(to);
      if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return 0;
      const ms = b.getTime() - a.getTime();
      return Math.max(0, Math.floor(ms / (1000 * 60 * 60 * 24)));
    };

    // ───────────────────────── Buscar cobrador ─────────────────────────
    let collectorUser = null;

    if (userId && isObjectIdLike(userId)) {
      collectorUser = await User.findById(userId)
        .select(
          "name email role idCobrador porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .lean();
    } else if (idCobrador != null) {
      const idCobStr = String(idCobrador);
      collectorUser = await User.findOne({
        role: "cobrador",
        idCobrador: idCobStr,
      })
        .select(
          "name email role idCobrador porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .lean();
    }

    if (!collectorUser) {
      return res.status(404).json({
        ok: false,
        message: "No se encontró el cobrador indicado.",
      });
    }

    const myCollectorId = Number(collectorUser.idCobrador);
    if (!Number.isFinite(myCollectorId) || myCollectorId <= 0) {
      return res.status(400).json({
        ok: false,
        message: "El cobrador no tiene un idCobrador numérico válido.",
      });
    }

    // ─────────────────────── Rango de fechas / período ───────────────────────
    const now = new Date();

    let rangeStart = normalizeDateStart(dateFrom);
    let rangeEnd = normalizeDateEnd(dateTo);

    // Si no mandan rango, usamos el mes actual
    if (!rangeStart || !rangeEnd) {
      const yearNow = now.getFullYear();
      const monthNow = now.getMonth(); // 0–11
      rangeStart = new Date(yearNow, monthNow, 1, 0, 0, 0, 0);
      rangeEnd = new Date(yearNow, monthNow + 1, 0, 23, 59, 59, 999);
    }

    // El "period" lo tomamos del inicio del rango (YYYY-MM)
    const period = yyyymmAR(rangeStart);
    const year = rangeStart.getFullYear();
    const month = rangeStart.getMonth();
    const label = `${monthNamesEs[month] || "Mes"} ${year}`.replace(
      /^\w/,
      (c) => c.toUpperCase()
    );

    // Días del calendario del período (NO es crítico, pero ayuda en UI)
    const daysInPeriod = new Date(year, month + 1, 0).getDate();
    const daysElapsed =
      now.getFullYear() === year && now.getMonth() === month
        ? now.getDate()
        : daysInPeriod;
    const daysRemaining = Math.max(daysInPeriod - daysElapsed, 0);

    // Días hábiles (lun–sáb)
    const countWorkingDays = () => {
      let total = 0;
      let elapsed = 0;

      for (let d = 1; d <= daysInPeriod; d++) {
        const dt = new Date(year, month, d);
        const day = dt.getDay(); // 0 = dom, 1 = lun, ..., 6 = sáb
        const isWorking = day >= 1 && day <= 6; // lun–sáb

        if (!isWorking) continue;
        total++;

        if (
          now.getFullYear() === year &&
          now.getMonth() === month &&
          d <= now.getDate()
        ) {
          elapsed++;
        } else if (now.getFullYear() > year || now.getMonth() > month) {
          // período ya pasó por completo
          elapsed = total;
        }
      }
      const remaining = Math.max(total - elapsed, 0);
      return { total, elapsed, remaining };
    };

    const {
      total: workingDaysTotal,
      elapsed: workingDaysElapsed,
      remaining: workingDaysRemaining,
    } = countWorkingDays();

    // ───────────────────── Config de comisión (User) ─────────────────────
    let baseCommissionRate = 0; // decimal (0.05 = 5 %)
    let graceDays = 7;
    let penaltyPerDay = 0;

    const rawPercent = collectorUser.porcentajeCobrador;
    if (typeof rawPercent === "number" && rawPercent > 0) {
      // Soporta 5 o 0.05
      baseCommissionRate = rawPercent <= 1 ? rawPercent : rawPercent / 100;
    }

    if (
      collectorUser &&
      collectorUser.commissionGraceDays != null &&
      Number.isFinite(Number(collectorUser.commissionGraceDays))
    ) {
      graceDays = Number(collectorUser.commissionGraceDays);
    }

    const rawPenalty = collectorUser.commissionPenaltyPerDay;
    if (typeof rawPenalty === "number" && rawPenalty > 0) {
      // Soporta 0.1 = 10% de la TASA por día, o 10 = 10% también
      penaltyPerDay = rawPenalty <= 1 ? rawPenalty : rawPenalty / 100;
    }

    // ───────────────── Clientes asignados + cuota vigente ─────────────────
    const clientsAgg = await Cliente.aggregate([
      { $match: { idCobrador: myCollectorId } },

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

    // ───────────────── Pagos del período + comisión pago a pago ─────────────────
    const paymentsAgg = await Payment.aggregate([
      {
        $match: {
          "collector.idCobrador": myCollectorId,
          status: { $in: ["posted", "settled"] },
          $expr: {
            $and: [
              {
                $gte: [{ $ifNull: ["$postedAt", "$createdAt"] }, rangeStart],
              },
              { $lte: [{ $ifNull: ["$postedAt", "$createdAt"] }, rangeEnd] },
            ],
          },
        },
      },
      { $unwind: "$allocations" },
      {
        $match: {
          "allocations.period": period, // solo lo imputado a este período
        },
      },
      {
        $project: {
          _id: 1,
          // Fecha BASE para calcular la antigüedad del pago:
          // usamos createdAt explícitamente (lo que vos pediste),
          // y dejamos postedAt por si en algún momento lo necesitás en UI.
          paymentDate: "$createdAt",
          postedAt: { $ifNull: ["$postedAt", "$createdAt"] },
          amountApplied: "$allocations.amountApplied",
          "cliente.idCliente": 1,
        },
      },
    ]).allowDiskUse(true);

    const clientsSet = new Set();
    let totalCollectedThisPeriod = 0;
    let totalCommissionIdeal = 0; // sin penalidad
    let totalCommissionDiscounted = 0; // con penalidad pago a pago

    for (const p of paymentsAgg) {
      const clientId = p.cliente?.idCliente;
      if (clientId != null) clientsSet.add(clientId);

      const applied = Number(p.amountApplied) || 0;
      totalCollectedThisPeriod += applied;

      const idealRate = baseCommissionRate;
      const idealCommission = applied * idealRate;
      totalCommissionIdeal += idealCommission;

      // ── Cálculo del % efectivo para ESTE pago ──
      let effectiveRate = idealRate;

      if (idealRate > 0 && penaltyPerDay > 0) {
        const baseDate = p.paymentDate || p.postedAt || null; // createdAt como prioridad
        if (baseDate) {
          const daysHeld = diffInDays(baseDate, now);

          if (daysHeld > graceDays) {
            const extraDays = daysHeld - graceDays;

            // penaltyPerDay = % de la TASA por día
            // ej: tasa 5% (0.05), penaltyPerDay 0.1 = 10% de la tasa/día:
            //   día 1 después de gracia → factor = 1 - 0.1 = 0.9 → 4.5%
            //   día 2 después de gracia → factor = 1 - 0.2 = 0.8 → 4.0%
            const reductionFactor = penaltyPerDay * extraDays;
            const effectiveFactor = Math.max(0, 1 - reductionFactor);

            effectiveRate = idealRate * effectiveFactor;
          }
        }
      }

      const discountedCommission = applied * effectiveRate;
      totalCommissionDiscounted += discountedCommission;
    }

    const clientsWithPayment = clientsSet.size;
    const clientsWithoutPayment = Math.max(
      assignedClients - clientsWithPayment,
      0
    );

    // ───────────────── Saldo actual en mano del cobrador (caja) ─────────────────
    const cashAccounts = ["CAJA_COBRADOR", "A_RENDIR_COBRADOR"];

    const balanceAgg = await LedgerEntry.aggregate([
      {
        $match: {
          "dimensions.idCobrador": myCollectorId,
          accountCode: { $in: cashAccounts },
        },
      },
      {
        $group: {
          _id: null,
          debits: {
            $sum: {
              $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0],
            },
          },
          credits: {
            $sum: {
              $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0],
            },
          },
        },
      },
    ]).allowDiskUse(true);

    const debits = balanceAgg?.[0]?.debits || 0;
    const credits = balanceAgg?.[0]?.credits || 0;
    const collectorBalance = debits - credits;

    // ───────────────── Comisiones globales ─────────────────
    const expectedCommission = totalChargeNow * baseCommissionRate; // si cobrara toda la cartera al % completo
    const currentCommission = totalCommissionDiscounted; // con penalidad pago por pago

    // ───────────────── Lo ya pagado como comisión (ledger) ─────────────────
    const paidAgg = await LedgerEntry.aggregate([
      {
        $match: {
          kind: "commission_payout",
          "dimensions.idCobrador": myCollectorId,
          $expr: {
            $and: [
              { $gte: ["$postedAt", rangeStart] },
              { $lte: ["$postedAt", rangeEnd] },
            ],
          },
        },
      },
      {
        $group: {
          _id: null,
          totalPaid: { $sum: "$amount" },
        },
      },
    ]).allowDiskUse(true);

    const alreadyPaid = paidAgg?.[0]?.totalPaid || 0;
    const pendingCommission = Math.max(currentCommission - alreadyPaid, 0);

    // ───────────────── Respuesta ─────────────────
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
          period, // "YYYY-MM"
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
        balance: {
          collectorBalance,
        },
        commissions: {
          config: {
            basePercent: baseCommissionRate, // decimal (0.05 = 5%)
            graceDays,
            penaltyPerDay, // % de la TASA por día
          },
          amounts: {
            expectedCommission, // si cobrara toda la cartera en término
            totalCommission: currentCommission, // “debería cobrar” HOY, con castigo por demora
            totalCommissionNoPenalty: totalCommissionIdeal, // referencia sin castigo
            alreadyPaid, // ya pagado en este rango (kind=commission_payout)
            pendingCommission, // lo lógico a pagar ahora
          },
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
