// src/controllers/admin.outbox.controller.js
import Outbox from "../models/outbox.model.js";

const toInt = (v, def = 0) => {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
};
const toNum = (v, def = null) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};
const toDir = (v) => (String(v || "").toLowerCase() === "asc" ? 1 : -1);
const parseISODate = (s) => {
  if (!s) return null;
  const dt = new Date(`${s}T00:00:00`);
  return Number.isNaN(dt.getTime()) ? null : dt;
};

/**
 * GET /admin/outbox
 */
export async function listAdminOutbox(req, res, next) {
  try {
    const page = Math.max(toInt(req.query.page, 1), 1);
    const limitRaw = Math.min(
      toInt(req.query.limit || req.query.pageSize, 25),
      100
    );
    const limit = Math.max(limitRaw, 1);

    const qRaw = String(req.query.q || "").trim();
    const topic = String(req.query.topic || "").trim();
    const status = String(req.query.status || "").trim(); // pending|sent|failed (ajusta si tenés otros)
    const dateFrom = String(req.query.dateFrom || "");
    const dateTo = String(req.query.dateTo || "");
    const minAttempts = toNum(req.query.minAttempts, null);
    const maxAttempts = toNum(req.query.maxAttempts, null);

    const sortByParam = (req.query.sortBy || "createdAt").toString();
    const sortDirParam = toDir(req.query.sortDir || "desc");

    const fromDt = parseISODate(dateFrom);
    const toDt = parseISODate(dateTo);
    const toDtEnd = toDt
      ? new Date(new Date(toDt).setHours(23, 59, 59, 999))
      : null;

    const match = {};
    if (topic) match.topic = topic;
    if (status) match.status = status;
    if (minAttempts != null)
      match.attempts = { ...(match.attempts || {}), $gte: minAttempts };
    if (maxAttempts != null)
      match.attempts = { ...(match.attempts || {}), $lte: maxAttempts };
    if (fromDt || toDtEnd) {
      match.createdAt = {};
      if (fromDt) match.createdAt.$gte = fromDt;
      if (toDtEnd) match.createdAt.$lte = toDtEnd;
    }

    const pipeline = [
      // 🔐 Strings seguros para búsqueda (evita ConversionFailure)
      {
        $addFields: {
          payloadStr: {
            $ifNull: [
              {
                $convert: {
                  input: "$payload",
                  to: "string",
                  onError: "",
                  onNull: "",
                },
              },
              "",
            ],
          },
          lastErrorStr: {
            $ifNull: [
              {
                $convert: {
                  input: "$lastError",
                  to: "string",
                  onError: "",
                  onNull: "",
                },
              },
              "",
            ],
          },
          idStr: {
            $ifNull: [
              {
                $convert: {
                  input: "$_id",
                  to: "string",
                  onError: "",
                  onNull: "",
                },
              },
              "",
            ],
          },
        },
      },
      { $match: match },
    ];

    if (qRaw) {
      const rx = { $regex: qRaw, $options: "i" };
      pipeline.push({
        $match: {
          $or: [
            { topic: rx },
            { lastErrorStr: rx },
            { payloadStr: rx },
            { idStr: rx },
          ],
        },
      });
    }

    const SORTABLE = new Set([
      "createdAt",
      "updatedAt",
      "topic",
      "status",
      "attempts",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "createdAt";
    const sortStage = { $sort: { [sortBy]: sortDirParam, _id: sortDirParam } };

    const projectStage = {
      $project: {
        _id: 1,
        topic: 1,
        payload: 1,
        status: 1,
        attempts: 1,
        lastError: 1,
        createdAt: 1,
        updatedAt: 1,
        // opcional: si tenés nextAttemptAt en otro lado, proyectalo
        // nextAttemptAt: 1,
      },
    };

    const dataPipeline = [
      ...pipeline,
      projectStage,
      sortStage,
      { $skip: (page - 1) * limit },
      { $limit: limit },
    ];
    const countPipeline = [...pipeline, { $count: "n" }];

    // stats
    const statsByStatusPipeline = [
      ...pipeline,
      { $group: { _id: "$status", count: { $sum: 1 } } },
    ];
    const statsByTopicPipeline = [
      ...pipeline,
      { $group: { _id: "$topic", count: { $sum: 1 } } },
      { $sort: { count: -1, _id: 1 } },
      { $limit: 25 },
    ];

    const [items, countRes, byStatusRes, byTopicRes] = await Promise.all([
      Outbox.aggregate(dataPipeline).allowDiskUse(true),
      Outbox.aggregate(countPipeline).allowDiskUse(true),
      Outbox.aggregate(statsByStatusPipeline).allowDiskUse(true),
      Outbox.aggregate(statsByTopicPipeline).allowDiskUse(true),
    ]);

    const total = countRes?.[0]?.n || 0;
    const stats = {
      byStatus: byStatusRes.reduce((acc, r) => {
        if (r?._id != null) acc[r._id] = r.count || 0;
        return acc;
      }, {}),
      byTopic: byTopicRes.map((r) => ({
        topic: r?._id || "",
        count: r.count || 0,
      })),
    };

    return res.json({
      ok: true,
      items,
      total,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDirParam === 1 ? "asc" : "desc",
      stats,
    });
  } catch (err) {
    next(err);
  }
}
