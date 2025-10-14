// src/controllers/clientes.controller.js  (ESM)

import mongoose from "mongoose";
import Cliente from "../models/client.model.js";

// Servicio de pricing por grupo (usa cremacion por miembro)
import recomputeGroupPricing from "../services/pricing.services.js";

/* ===================== Helpers ===================== */

const isObjectId = (v) => mongoose.Types.ObjectId.isValid(String(v || ""));

const toBool = (v) => {
  if (typeof v === "boolean") return v;
  if (v === 1 || v === "1" || v === "true" || v === "TRUE" || v === "True")
    return true;
  if (v === 0 || v === "0" || v === "false" || v === "FALSE" || v === "False")
    return false;
  return Boolean(v);
};

const toNumOrUndef = (v) => {
  if (v === "" || v === null || v === undefined) return undefined;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : undefined;
};

// Acepta "9/5/1930", "01/01/2024", "10/27/2003"; limpia "  -   -" => undefined
const toDateOrUndef = (v) => {
  if (!v && v !== 0) return undefined;
  const s = String(v).trim();
  if (!s || s.replace(/-/g, "").replace(/\s/g, "") === "") return undefined;
  if (/^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(s)) {
    const [m, d, y] = s.split("/").map((x) => parseInt(x, 10));
    const yyyy = y < 100 ? 1900 + y : y;
    const dt = new Date(yyyy, m - 1, d);
    return Number.isNaN(dt.getTime()) ? undefined : dt;
  }
  const dt = v instanceof Date ? v : new Date(s);
  return Number.isNaN(dt.getTime()) ? undefined : dt;
};

// Edad desde fecha
const ageFromDate = (d) => {
  if (!d) return undefined;
  const dt = d instanceof Date ? d : new Date(d);
  if (Number.isNaN(dt.getTime())) return undefined;
  const today = new Date();
  let a = today.getFullYear() - dt.getFullYear();
  const m = today.getMonth() - dt.getMonth();
  if (m < 0 || (m === 0 && today.getDate() < dt.getDate())) a--;
  return Math.max(a, 0);
};

// Normalización principal (nuevo modelo: sin plan; con cremacion/parcela boolean)
function normalizePayload(p = {}) {
  const n = { ...p };

  // ==== STRINGS ====
  if (typeof n.nombre === "string") n.nombre = n.nombre.trim().toUpperCase();
  [
    "domicilio",
    "ciudad",
    "provincia",
    "observaciones",
    "cuil",
    "docTipo",
    "nombreTitular",
    "sexo",
    "tipoFactura",
  ].forEach((k) => {
    if (k in n && n[k] != null) n[k] = String(n[k]).trim();
  });

  // Tel/CP -> limpiar 0
  if ("telefono" in n)
    n.telefono = n.telefono === 0 ? "" : String(n.telefono ?? "").trim();
  if ("cp" in n) n.cp = n.cp === 0 ? "" : String(n.cp ?? "").trim();

  // ==== NÚMEROS ====
  [
    "idCliente",
    "edad",
    "idCobrador",
    "cuotaAnterior",
    "cuotaNueva",
    "cuota", // histórico/último cobrado
    "cuotaIdeal", // persistido para UI/consultas
    "cuotaPisada", // opcional
  ].forEach((k) => {
    if (k in n) n[k] = toNumOrUndef(n[k]);
  });

  // ==== BOOLEANS ====
  [
    "parcela",
    "cremacion", // NUEVO
    "factura",
    "tarjeta",
    "emergencia",
    "activo",
    "integrante",
    "usarCuotaPisada",
  ].forEach((k) => {
    if (k in n) n[k] = toBool(n[k]);
  });

  // ==== FECHAS ====
  ["fechaNac", "ingreso", "vigencia", "baja", "fechaAumento"].forEach((k) => {
    if (k in n) n[k] = toDateOrUndef(n[k]);
  });

  return n;
}

/* ===================================== LIST ===================================== */
// GET /api/clientes
export async function listClientes(req, res, next) {
  try {
    const page = Math.max(parseInt(req.query.page) || 1, 1);
    const limit = Math.min(parseInt(req.query.limit) || 10, 100);
    const qRaw = (req.query.q || "").trim();

    const byIdClienteRaw = req.query.byIdCliente;
    const byIdCliente =
      byIdClienteRaw !== undefined && byIdClienteRaw !== ""
        ? Number(byIdClienteRaw)
        : undefined;
    const hasByIdCliente = Number.isFinite(byIdCliente);

    const sortByParam = (req.query.sortBy || "createdAt").toString();
    const sortDirParam = (req.query.sortDir || req.query.order || "desc")
      .toString()
      .toLowerCase();
    const sortDir = sortDirParam === "asc" ? 1 : -1;

    const SORTABLE = new Set([
      "createdAt",
      "idCliente",
      "nombre",
      "idCobrador",
      "ingreso",
      "cuota",
      "cuotaIdeal",
      "cuotaPisada",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "createdAt";

    const filter = {};
    if (hasByIdCliente) {
      filter.idCliente = byIdCliente;
    } else if (qRaw) {
      const isNumeric = /^\d+$/.test(qRaw);
      const or = [
        { nombre: { $regex: qRaw, $options: "i" } },
        { domicilio: { $regex: qRaw, $options: "i" } },
      ];
      if (isNumeric) {
        or.push({ idCliente: Number(qRaw) });
        or.push({ idCobrador: Number(qRaw) });
      }
      filter.$or = or;
    }

    const total = await Cliente.countDocuments(filter);

    const sortStage =
      sortBy === "createdAt"
        ? { createdAtSafe: sortDir, _id: sortDir }
        : { [sortBy]: sortDir, _id: sortDir };

    const itemsAgg = Cliente.aggregate([
      { $match: filter },

      // createdAt seguro (por si faltara en algunos docs)
      {
        $addFields: {
          createdAtSafe: { $ifNull: ["$createdAt", { $toDate: "$_id" }] },
        },
      },

      // cuotaVigente según usarCuotaPisada
      {
        $addFields: {
          cuotaVigente: {
            $cond: [
              {
                $and: [
                  { $eq: ["$usarCuotaPisada", true] },
                  { $ne: ["$cuotaPisada", null] },
                ],
              },
              "$cuotaPisada",
              "$cuotaIdeal",
            ],
          },
        },
      },

      // Cantidad de integrantes ACTIVOS del grupo (idCliente)
      {
        $lookup: {
          from: "clientes",
          let: { groupId: "$idCliente" },
          pipeline: [
            { $match: { $expr: { $eq: ["$idCliente", "$$groupId"] } } },
            // Activo robusto: baja NO date
            {
              $match: {
                $expr: { $ne: [{ $type: "$baja" }, "date"] },
                activo: true,
              },
            },
            { $count: "n" },
          ],
          as: "grupo",
        },
      },
      {
        $addFields: {
          integrantesCount: { $ifNull: [{ $first: "$grupo.n" }, 0] },
        },
      },

      // Δ vs lo cobrado (histórico)
      {
        $addFields: {
          difIdealVsCobro: { $subtract: ["$cuotaIdeal", "$cuota"] },
          difVigenteVsCobro: { $subtract: ["$cuotaVigente", "$cuota"] },
        },
      },

      { $sort: sortStage },
      { $skip: (page - 1) * limit },
      { $limit: limit },

      // Proyección final
      {
        $project: {
          _id: 1,
          idCliente: 1,
          nombre: 1,
          domicilio: 1,
          ciudad: 1,
          provincia: 1,
          cp: 1,
          telefono: 1,
          documento: 1,
          docTipo: 1,
          edad: 1,
          idCobrador: 1,

          // históricos
          cuota: 1,

          // pricing
          cuotaIdeal: 1,
          cuotaPisada: 1,
          usarCuotaPisada: 1,
          cuotaVigente: 1,
          difIdealVsCobro: 1,
          difVigenteVsCobro: 1,

          // flags
          parcela: 1,
          cremacion: 1,

          // estado
          activo: 1,
          ingreso: 1,
          baja: 1,
          createdAt: 1,

          // agregado
          integrantesCount: 1,

          // helpers
          createdAtSafe: 1,
        },
      },
    ]).allowDiskUse(true);

    const items = await itemsAgg;

    res.json({
      items,
      total,
      page,
      pageSize: limit,
      sortBy,
      sortDir: sortDir === 1 ? "asc" : "desc",
    });
  } catch (err) {
    next(err);
  }
}

/* ===================================== SHOW ===================================== */
// GET /api/clientes/:id  (id = _id de Mongo SIEMPRE)
export async function getClienteById(req, res, next) {
  try {
    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ message: "ID inválido" });

    const doc = await Cliente.findById(id).lean();
    if (!doc) return res.status(404).json({ message: "Cliente no encontrado" });

    const payload = {
      data: {
        ...doc,
        cuotaVigente:
          doc.usarCuotaPisada && Number.isFinite(doc.cuotaPisada)
            ? doc.cuotaPisada
            : doc.cuotaIdeal,
      },
    };

    // expand=family -> grupo por idCliente
    const expand = String(req.query.expand || "").toLowerCase();
    const shouldExpandFamily =
      expand === "family" ||
      expand === "all" ||
      expand.split(",").includes("family");

    if (shouldExpandFamily) {
      const n = Number(doc.idCliente);
      if (Number.isFinite(n)) {
        const family = await Cliente.find({ idCliente: n })
          .select(
            "nombre documento edad activo idCliente cuota docTipo cuotaIdeal cuotaPisada usarCuotaPisada cremacion parcela"
          )
          .sort({ nombre: 1, _id: 1 })
          .lean();

        payload.family = Array.isArray(family)
          ? family.map((m) => ({
              ...m,
              cuotaVigente:
                m.usarCuotaPisada && Number.isFinite(m.cuotaPisada)
                  ? m.cuotaPisada
                  : m.cuotaIdeal,
            }))
          : [];
        payload.familyCount = payload.family.length;
      } else {
        payload.family = [];
        payload.familyCount = 0;
      }
    }

    return res.json(payload);
  } catch (err) {
    next(err);
  }
}

/* ===================================== CREATE ===================================== */
// POST /api/clientes
export async function createCliente(req, res, next) {
  const session = await Cliente.startSession();
  session.startTransaction();
  try {
    const payload = normalizePayload(req.body);

    // Aislamos integrantes (si vienen)
    const integrantesRaw = Array.isArray(req.body.integrantes)
      ? req.body.integrantes
      : [];
    const integrantes = integrantesRaw.map(normalizePayload);
    delete payload.integrantes;

    // Autonumerar idCliente si no viene
    if (!payload.idCliente && payload.idCliente !== 0) {
      const last = await Cliente.findOne({}, { idCliente: 1, _id: 0 })
        .sort({ idCliente: -1 })
        .lean();
      payload.idCliente = (last?.idCliente ?? 0) + 1;
    }

    // Edad titular (preferimos fechaNac)
    const edadTitular = payload.fechaNac
      ? ageFromDate(payload.fechaNac)
      : payload.edad;
    if (typeof edadTitular === "number") payload.edad = edadTitular;

    // Crear titular
    const titularDoc = await Cliente.create([payload], { session });
    const titular = titularDoc[0];

    // Campos permitidos en integrantes
    const pick = (o, keys) =>
      keys.reduce(
        (acc, k) => (o[k] !== undefined ? ((acc[k] = o[k]), acc) : acc),
        {}
      );
    const FIELDS = [
      "nombre",
      "domicilio",
      "ciudad",
      "provincia",
      "cp",
      "telefono",
      "documento",
      "docTipo",
      "edad",
      "sexo",
      "cuil",
      "fechaNac",
      "ingreso",
      "vigencia",
      "baja",
      "observaciones",
      "tipoFactura",
      "factura",
      "tarjeta",
      "emergencia",
      "activo",
      "parcela",
      "cremacion", // NUEVO
      "rol",
      "integrante",
      "nombreTitular",
    ];

    // Normalizamos familiares y copiamos idCliente del titular
    const familiaresDocs = integrantes
      .map((fam) => {
        const edad = fam.fechaNac ? ageFromDate(fam.fechaNac) : fam.edad;
        const base = { ...fam, edad };
        return { ...pick(base, FIELDS), idCliente: payload.idCliente };
      })
      .filter((d) => (d.nombre || "").toString().trim() !== "");

    if (familiaresDocs.length) {
      await Cliente.insertMany(familiaresDocs, { session, ordered: true });
    }

    await session.commitTransaction();
    session.endSession();

    // 🔁 Recalcular pricing del grupo completo con el nuevo esquema
    await recomputeGroupPricing(payload.idCliente, { debug: false });

    // Volver a leer el titular para responder con datos vigentes
    const titularFresh = await Cliente.findById(titular._id).lean();
    const cuotaVigente =
      titularFresh?.usarCuotaPisada &&
      Number.isFinite(titularFresh?.cuotaPisada)
        ? titularFresh.cuotaPisada
        : titularFresh?.cuotaIdeal ?? 0;

    res.status(201).json({
      data: { ...titularFresh, cuotaVigente },
      meta: {
        familiaresCreados: familiaresDocs.length,
        idCliente: payload.idCliente,
      },
    });
  } catch (err) {
    await session.abortTransaction().catch(() => {});
    session.endSession();
    next(err);
  }
}

/* ===================================== UPDATE ===================================== */
// PUT /api/clientes/:id  (id = _id de Mongo SIEMPRE)
export async function updateCliente(req, res, next) {
  try {
    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ message: "ID inválido" });

    const payloadRaw = { ...req.body };
    delete payloadRaw._id; // nunca permitir cambiar _id
    const payload = normalizePayload(payloadRaw);

    // Validar idCliente si lo dejás editable
    if (payload.hasOwnProperty("idCliente")) {
      const n = Number(payload.idCliente);
      if (!Number.isFinite(n))
        return res.status(400).json({ message: "idCliente debe ser numérico" });
      payload.idCliente = n;
    }

    // Recalcular edad si vino fechaNac pero no edad
    if (payload.fechaNac && !payload.edad) {
      const edad = ageFromDate(payload.fechaNac);
      if (typeof edad === "number") payload.edad = edad;
    }

    // Actualizar
    const updated = await Cliente.findByIdAndUpdate(
      id,
      { $set: payload },
      { new: true, runValidators: true }
    ).lean();
    if (!updated)
      return res.status(404).json({ message: "Cliente no encontrado" });

    // 🔁 Recalcular pricing de TODO el grupo (porque cremacion/parcela afectan al grupo)
    const gid = Number(updated.idCliente);
    if (Number.isFinite(gid)) {
      await recomputeGroupPricing(gid, { debug: false });
    }

    // Releer el doc actualizado después del recompute
    const fresh = await Cliente.findById(id).lean();
    const cuotaVigente =
      fresh?.usarCuotaPisada && Number.isFinite(fresh?.cuotaPisada)
        ? fresh.cuotaPisada
        : fresh?.cuotaIdeal ?? 0;

    return res.json({ data: { ...fresh, cuotaVigente } });
  } catch (err) {
    next(err);
  }
}

/* ===================================== DELETE ===================================== */
// DELETE /api/clientes/:id
export async function deleteCliente(req, res, next) {
  try {
    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ message: "ID inválido" });

    const doc = await Cliente.findOneAndDelete({ _id: id }).lean();
    if (!doc) return res.status(404).json({ message: "Cliente no encontrado" });

    // Recalcular grupo remanente
    const gid = Number(doc.idCliente);
    if (Number.isFinite(gid))
      await recomputeGroupPricing(gid, { debug: false });

    return res.json({ ok: true, _id: doc._id, idCliente: doc.idCliente });
  } catch (err) {
    next(err);
  }
}

/* =================================== STATS =================================== */
// GET /api/clientes/stats (activos, 1 doc por grupo: titular si existe)
export async function getClientesStats(req, res, next) {
  try {
    const wantDebug = String(req.query.debug || "") === "1";

    // --------- Métricas de depuración (opcional) ----------
    let debug = null;
    if (wantDebug) {
      const totalAll = await Cliente.countDocuments({});
      const totalActivoTrue = await Cliente.countDocuments({ activo: true });
      const totalBajaIsDate = await Cliente.countDocuments({
        $expr: { $eq: [{ $type: "$baja" }, "date"] },
      });
      const totalBajaNotDate = await Cliente.countDocuments({
        $expr: { $ne: [{ $type: "$baja" }, "date"] },
      });
      const totalStrict = await Cliente.countDocuments({
        activo: true,
        $or: [{ baja: { $exists: false } }, { baja: null }],
      });
      const totalRobust = await Cliente.countDocuments({
        activo: true,
        $expr: { $ne: [{ $type: "$baja" }, "date"] },
      });

      const sampleBajaString = await Cliente.find({
        $expr: {
          $and: [
            { $ne: [{ $type: "$baja" }, "date"] },
            { $eq: ["$activo", true] },
          ],
        },
      })
        .select("_id idCliente nombre activo baja")
        .limit(5)
        .lean();

      debug = {
        totals: {
          totalAll,
          totalActivoTrue,
          totalBajaIsDate,
          totalBajaNotDate,
          totalStrict,
          totalRobust,
        },
        samples: { activeWithBajaNonDate: sampleBajaString },
        hint: "Usamos criterio robusto: consideramos baja solo si es Date. Placeholders string ('  -   -', '-', '') NO cuentan como baja.",
      };
    }

    const robustMatchStage = {
      $match: { activo: true, $expr: { $ne: [{ $type: "$baja" }, "date"] } },
    };

    const normalizeNumbersStage = {
      $addFields: {
        cuota_num: {
          $cond: [
            { $isNumber: "$cuota" },
            "$cuota",
            { $toDouble: { $ifNull: ["$cuota", 0] } },
          ],
        },
        cuotaIdeal_num: {
          $cond: [
            { $isNumber: "$cuotaIdeal" },
            "$cuotaIdeal",
            { $toDouble: { $ifNull: ["$cuotaIdeal", 0] } },
          ],
        },
        cuotaPisada_num: {
          $cond: [
            { $isNumber: "$cuotaPisada" },
            "$cuotaPisada",
            {
              $cond: [
                { $eq: ["$cuotaPisada", null] },
                null,
                { $toDouble: { $ifNull: ["$cuotaPisada", 0] } },
              ],
            },
          ],
        },
        cremacion_num: { $cond: [{ $eq: ["$cremacion", true] }, 1, 0] },
        parcela_num: { $cond: [{ $eq: ["$parcela", true] }, 1, 0] },
      },
    };

    const pipeline = [
      robustMatchStage,
      normalizeNumbersStage,
      {
        $addFields: {
          cuotaVigente_num: {
            $cond: [
              {
                $and: [
                  { $eq: ["$usarCuotaPisada", true] },
                  { $ne: ["$cuotaPisada_num", null] },
                ],
              },
              "$cuotaPisada_num",
              "$cuotaIdeal_num",
            ],
          },
          titularRank: { $cond: [{ $eq: ["$rol", "TITULAR"] }, 0, 1] },
        },
      },
      { $sort: { idCliente: 1, titularRank: 1, _id: 1 } },
      {
        $group: {
          _id: "$idCliente",
          integrantesCount: { $sum: 1 },
          nombre: { $first: "$nombre" },
          idCobrador: { $first: "$idCobrador" },
          cuota: { $first: "$cuota_num" },
          cuotaIdeal: { $first: "$cuotaIdeal_num" },
          cuotaVigente: { $first: "$cuotaVigente_num" },
          ingreso: { $first: "$ingreso" },
          cremaciones: { $sum: "$cremacion_num" }, // NUEVO: por grupo
          anyParcela: { $max: "$parcela_num" }, // NUEVO: 1 si algún miembro tiene parcela
        },
      },
      {
        $addFields: {
          difIdealVsCobro: { $subtract: ["$cuotaIdeal", "$cuota"] },
          difVigenteVsCobro: { $subtract: ["$cuotaVigente", "$cuota"] },
        },
      },
      {
        $facet: {
          summary: [
            {
              $group: {
                _id: null,
                groups: { $sum: 1 },
                sumCuota: { $sum: "$cuota" },
                sumIdeal: { $sum: "$cuotaIdeal" },
                sumVigente: { $sum: "$cuotaVigente" },
                sumDiff: { $sum: "$difIdealVsCobro" },
                posCount: {
                  $sum: { $cond: [{ $gt: ["$difIdealVsCobro", 0] }, 1, 0] },
                },
                negCount: {
                  $sum: { $cond: [{ $lt: ["$difIdealVsCobro", 0] }, 1, 0] },
                },
                posSum: {
                  $sum: {
                    $cond: [
                      { $gt: ["$difIdealVsCobro", 0] },
                      "$difIdealVsCobro",
                      0,
                    ],
                  },
                },
                negSum: {
                  $sum: {
                    $cond: [
                      { $lt: ["$difIdealVsCobro", 0] },
                      "$difIdealVsCobro",
                      0,
                    ],
                  },
                },
                sumIntegrantes: { $sum: "$integrantesCount" },
                sumCremaciones: { $sum: "$cremaciones" },
                gruposConParcela: {
                  $sum: { $cond: [{ $gt: ["$anyParcela", 0] }, 1, 0] },
                },
              },
            },
            {
              $project: {
                _id: 0,
                groups: 1,
                sumCuota: 1,
                sumIdeal: 1,
                sumVigente: 1,
                sumDiff: 1,
                posCount: 1,
                negCount: 1,
                posSum: 1,
                negSum: 1,
                avgCuota: {
                  $cond: [
                    { $gt: ["$groups", 0] },
                    { $divide: ["$sumCuota", "$groups"] },
                    0,
                  ],
                },
                avgIntegrantes: {
                  $cond: [
                    { $gt: ["$groups", 0] },
                    { $divide: ["$sumIntegrantes", "$groups"] },
                    0,
                  ],
                },
                posPct: {
                  $cond: [
                    { $gt: ["$groups", 0] },
                    { $multiply: [{ $divide: ["$posCount", "$groups"] }, 100] },
                    0,
                  ],
                },
                sumCremaciones: 1,
                gruposConParcela: 1,
              },
            },
          ],
          byCobrador: [
            {
              $group: {
                _id: "$idCobrador",
                count: { $sum: 1 },
                diffSum: { $sum: "$difIdealVsCobro" },
                cuotaSum: { $sum: "$cuota" },
              },
            },
            { $sort: { diffSum: -1 } },
            { $limit: 10 },
            {
              $project: {
                _id: 0,
                idCobrador: "$_id",
                count: 1,
                diffSum: 1,
                cuotaSum: 1,
              },
            },
          ],
          diffHistogram: [
            {
              $bucket: {
                groupBy: "$difIdealVsCobro",
                boundaries: [
                  -1e9, -10000, -5000, -1000, -1, 0, 1, 1000, 5000, 10000, 1e9,
                ],
                default: "otros",
                output: { count: { $sum: 1 } },
              },
            },
          ],
          topPositive: [
            { $sort: { difIdealVsCobro: -1 } },
            { $limit: 20 },
            {
              $project: {
                _id: 0,
                idCliente: "$_id",
                nombre: 1,
                idCobrador: 1,
                integrantesCount: 1,
                cuota: 1,
                cuotaIdeal: 1,
                cuotaVigente: 1,
                difIdealVsCobro: 1,
                cremaciones: 1,
                anyParcela: 1,
              },
            },
          ],
          topNegative: [
            { $sort: { difIdealVsCobro: 1 } },
            { $limit: 20 },
            {
              $project: {
                _id: 0,
                idCliente: "$_id",
                nombre: 1,
                idCobrador: 1,
                integrantesCount: 1,
                cuota: 1,
                cuotaIdeal: 1,
                cuotaVigente: 1,
                difIdealVsCobro: 1,
                cremaciones: 1,
                anyParcela: 1,
              },
            },
          ],
        },
      },
    ];

    const agg = await Cliente.aggregate(pipeline).allowDiskUse(true);

    const [
      {
        summary = [],
        byCobrador = [],
        diffHistogram = [],
        topPositive = [],
        topNegative = [],
      } = {},
    ] = agg;

    res.json({
      data: {
        summary: summary[0] || {
          groups: 0,
          sumCuota: 0,
          sumIdeal: 0,
          sumVigente: 0,
          sumDiff: 0,
          posCount: 0,
          negCount: 0,
          posSum: 0,
          negSum: 0,
          avgCuota: 0,
          avgIntegrantes: 0,
          posPct: 0,
          sumCremaciones: 0,
          gruposConParcela: 0,
        },
        byCobrador,
        diffHistogram,
        topPositive,
        topNegative,
      },
      meta: {
        scope: "activos_robusto_titulares_por_grupo",
        generatedAt: new Date().toISOString(),
        debug,
      },
    });
  } catch (err) {
    next(err);
  }
}
