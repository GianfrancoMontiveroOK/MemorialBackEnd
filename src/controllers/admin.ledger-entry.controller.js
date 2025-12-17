// controllers/admin.ledger-entries.controller.js
import mongoose from "mongoose";
import LedgerEntry from "../models/ledger-entry.model.js";
import Payment from "../models/payment.model.js";
import User from "../models/user.model.js";

const toInt = (v) => {
  if (v == null || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : undefined;
};

const toFloat = (v) => {
  if (v == null || v === "") return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
};

const toBool = (v, def = false) => {
  if (typeof v === "boolean") return v;
  if (v == null || v === "") return def;
  const s = String(v).toLowerCase().trim();
  if (["1", "true", "yes", "y", "si"].includes(s)) return true;
  if (["0", "false", "no", "n"].includes(s)) return false;
  return def;
};

const isObjectIdLike = (v) =>
  typeof v === "string" && /^[a-fA-F0-9]{24}$/.test(v);

const escapeRegex = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const debitExpr = {
  $sum: { $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0] },
};
const creditExpr = {
  $sum: { $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0] },
};

// sort fields válidos (incluye fromUser/toUser que están en root)
const SORT_ALLOWLIST = new Set([
  "postedAt",
  "createdAt",
  "amount",
  "currency",
  "accountCode",
  "side",
  "kind",
  "fromUser",
  "toUser",
  "fromAccountCode",
  "toAccountCode",
  "dimensions.idCobrador",
  "dimensions.idCliente",
  // calculado via lookup actor (si includeActor=1 o si lo necesitamos)
  "actorUserName",
]);

export async function listAdminLedgerEntries(req, res) {
  try {
    // ───────────────────────── Query params ─────────────────────────
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
      userId, // actor (userId del asiento)
      includePayment = "0",
      includeActor = "0", // 🆕 opcional: para devolver actorUserName siempre
      sortBy = "postedAt",
      sortDir = "desc",
      page = 1,
      pageSize = 25,
      limit,
    } = req.query;

    const _page = Math.max(Number(page) || 1, 1);
    const _limit = Math.min(Number(pageSize ?? limit ?? 25) || 25, 200);
    const _skip = (_page - 1) * _limit;

    const includePaymentFlag = toBool(includePayment, false);
    const includeActorFlag = toBool(includeActor, false);

    const needPaymentJoin = includePaymentFlag || !!method || !!status;

    const _sortDir = String(sortDir).toLowerCase() === "asc" ? 1 : -1;
    const _sortBy = SORT_ALLOWLIST.has(sortBy) ? sortBy : "postedAt";

    // ───────────────────────── Match base ─────────────────────────
    const matchAnd = [];

    // fechas
    if (dateFrom) {
      const d = new Date(dateFrom);
      if (!Number.isNaN(d.getTime())) matchAnd.push({ postedAt: { $gte: d } });
    }
    if (dateTo) {
      const e = new Date(dateTo);
      if (!Number.isNaN(e.getTime())) {
        // si viene solo YYYY-MM-DD, lo llevamos a fin de día
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

    if (side === "debit" || side === "credit") matchAnd.push({ side });

    const _accountCode = accountCode || account;
    if (_accountCode) matchAnd.push({ accountCode: String(_accountCode) });

    if (currency) matchAnd.push({ currency: String(currency) });

    const _idCob = toInt(idCobrador);
    if (Number.isFinite(_idCob))
      matchAnd.push({ "dimensions.idCobrador": _idCob });

    const _idCli = toInt(idCliente);
    if (Number.isFinite(_idCli))
      matchAnd.push({ "dimensions.idCliente": _idCli });

    const _min = toFloat(minAmount);
    const _max = toFloat(maxAmount);
    if (_min !== undefined || _max !== undefined) {
      const amountFilter = {};
      if (_min !== undefined) amountFilter.$gte = _min;
      if (_max !== undefined) amountFilter.$lte = _max;
      matchAnd.push({ amount: amountFilter });
    }

    if (userId && isObjectIdLike(String(userId))) {
      matchAnd.push({ userId: new mongoose.Types.ObjectId(String(userId)) });
    }

    // ⛔️ Seguridad por rol (tu regla)
    const viewerRole = String(req.user?.role || "").trim();
    if (viewerRole === "admin") {
      matchAnd.push({
        $nor: [
          { $and: [{ accountCode: "CAJA_GRANDE" }, { side: "credit" }] },
          { accountCode: "CAJA_SUPERADMIN" },
        ],
      });
    }

    const baseMatch = matchAnd.length ? { $and: matchAnd } : {};

    // ───────────────────────── Search (q) ─────────────────────────
    const qTrim = String(q || "").trim();
    const hasQ = qTrim.length > 0;

    const qNum = toInt(qTrim);
    const isPureNumber = hasQ && Number.isFinite(qNum) && qTrim === String(qNum);

    // texto => usamos regex
    const needsTextSearch = hasQ && !isPureNumber;

    // Si el user quiere buscar texto, podemos buscar:
    // - fromUser/toUser (root)
    // - accountCode, currency, side, kind, fromAccountCode/toAccountCode (root)
    // - actorUserName (requiere lookup)
    const needsActorJoinForSearch = needsTextSearch;

    // Orden por actorUserName => también requiere lookup
    const needsActorJoinForSort = _sortBy === "actorUserName";

    const needActorJoin =
      includeActorFlag || needsActorJoinForSearch || needsActorJoinForSort;

    // ───────────────────────── Pipeline ─────────────────────────
    const pipeline = [];
    if (Object.keys(baseMatch).length) pipeline.push({ $match: baseMatch });

    // Join payments solo si hace falta
    if (needPaymentJoin) {
      const payPipeline = [
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
      ];

      if (method) payPipeline.unshift({ $match: { method: String(method) } });
      if (status) payPipeline.unshift({ $match: { status: String(status) } });

      pipeline.push({
        $lookup: {
          from: Payment.collection.name,
          localField: "paymentId",
          foreignField: "_id",
          as: "payment",
          pipeline: payPipeline,
        },
      });

      // Si filtro por method/status, me quedo solo con los que matchearon
      if (method || status) {
        pipeline.push({
          $match: { $expr: { $gt: [{ $size: "$payment" }, 0] } },
        });
      }
    }

    // Actor join (solo si hace falta)
    if (needActorJoin) {
      pipeline.push(
        {
          $lookup: {
            from: User.collection.name,
            localField: "userId",
            foreignField: "_id",
            as: "actorUser",
            pipeline: [
              { $project: { _id: 1, name: 1, email: 1, role: 1, idCobrador: 1 } },
            ],
          },
        },
        {
          $addFields: {
            actorUserName: {
              $let: {
                vars: { u: { $first: "$actorUser" } },
                in: {
                  $ifNull: [
                    { $ifNull: ["$$u.name", "$$u.email"] },
                    null,
                  ],
                },
              },
            },
          },
        }
      );
    }

    // q numérico => match directo sin lookups extra
    if (isPureNumber) {
      pipeline.push({
        $match: {
          $or: [
            { "dimensions.idCliente": qNum },
            { "dimensions.idCobrador": qNum },
            { amount: qNum },
          ],
        },
      });
    }

    // q texto => match por root strings + actorUserName (si existe)
    if (needsTextSearch) {
      const re = new RegExp(escapeRegex(qTrim), "i");
      const or = [
        { accountCode: re },
        { currency: re },
        { side: re },
        { kind: re },
        { fromAccountCode: re },
        { toAccountCode: re },
        { fromUser: re },
        { toUser: re },
      ];
      if (needActorJoin) or.push({ actorUserName: re });

      pipeline.push({ $match: { $or: or } });
    }

    // ───────────────────────── Facet (items/total/stats) ─────────────────────────
    // OJO: si ordenás por actorUserName y no hicimos lookup, no se puede.
    // Ya lo forzamos con needActorJoinForSort.
    const facet = {
      items: [
        { $sort: { [_sortBy]: _sortDir, _id: -1 } },
        { $skip: _skip },
        { $limit: _limit },
      ],
      total: [{ $count: "total" }],
      statsByCurrency: [
        {
          $group: {
            _id: "$currency",
            lines: { $sum: 1 },
            debit: debitExpr,
            credit: creditExpr,
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
      ],
    };

    // Proyección final (solo NUEVO esquema)
    const finalProject = {
      _id: 1,
      paymentId: 1,
      kind: 1,

      // ✅ owner/actor del asiento
      userId: 1,

      // ✅ root strings
      fromUser: 1,
      toUser: 1,

      // ✅ cuentas lógicas root
      fromAccountCode: 1,
      toAccountCode: 1,

      side: 1,
      accountCode: 1,
      amount: 1,
      currency: 1,
      postedAt: 1,
      createdAt: 1,

      dimensions: 1,
    };

    if (needActorJoin) {
      finalProject.actorUserName = 1;
      finalProject.actorUser = includeActorFlag
        ? { $cond: [{ $gt: [{ $size: "$actorUser" }, 0] }, { $first: "$actorUser" }, null] }
        : undefined;
      // si no querés devolver el objeto actorUser nunca, borrá la línea de arriba.
      delete finalProject.actorUser; // <- por defecto NO devolvemos el doc entero
    }

    if (needPaymentJoin) {
      if (includePaymentFlag) {
        finalProject.payment = {
          $cond: [{ $gt: [{ $size: "$payment" }, 0] }, { $first: "$payment" }, null],
        };
      } else {
        finalProject["payment.method"] = { $ifNull: [{ $first: "$payment.method" }, null] };
        finalProject["payment.status"] = { $ifNull: [{ $first: "$payment.status" }, null] };
      }
    }

    facet.items.push({ $project: finalProject });

    pipeline.push({ $facet: facet });

    const [out] = await LedgerEntry.aggregate(pipeline).allowDiskUse(true);

    const items = out?.items || [];
    const total = out?.total?.[0]?.total || 0;
    const statsArr = out?.statsByCurrency || [];

    const stats = {};
    for (const s of statsArr) {
      stats[s.currency || "-"] = {
        debit: s.debit || 0,
        credit: s.credit || 0,
        net: s.net || 0,
        lines: s.lines || 0,
      };
    }

    return res.json({
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
    return res.status(500).json({
      ok: false,
      message: "Error al listar el libro mayor.",
      error: err?.message,
    });
  }
}
