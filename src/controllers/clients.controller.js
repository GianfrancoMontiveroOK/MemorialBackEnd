// src/controllers/clientes.controller.js  (ESM)

import mongoose from "mongoose";
import Cliente from "../models/client.model.js";
import { recomputeGroupPricing } from "../services/pricing.services.js";
import Payment from "../models/payment.model.js";
import LedgerEntry from "../models/ledger-entry.model.js";
import User from "../models/user.model.js";
/* ===================== Helpers de parseo ===================== */

const isObjectId = (v) => mongoose.Types.ObjectId.isValid(String(v || ""));

const toBool = (v) => {
  if (typeof v === "boolean") return v;
  const s = String(v).trim().toLowerCase();
  if (s === "1" || s === "true" || s === "on") return true;
  if (s === "0" || s === "false" || s === "off") return false;
  return Boolean(v);
};

const toNumOrUndef = (v) => {
  if (v === "" || v === null || v === undefined) return undefined;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : undefined;
};

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

function normalizePayload(p = {}) {
  const n = { ...p };

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
    "rol",
  ].forEach((k) => {
    if (k in n && n[k] != null) n[k] = String(n[k]).trim();
  });

  if ("telefono" in n)
    n.telefono = n.telefono === 0 ? "" : String(n.telefono ?? "").trim();
  if ("cp" in n) n.cp = n.cp === 0 ? "" : String(n.cp ?? "").trim();

  [
    "idCliente",
    "edad",
    "idCobrador",
    "cuota",
    "cuotaIdeal",
    "integrante",
  ].forEach((k) => {
    if (k in n) n[k] = toNumOrUndef(n[k]);
  });

  [
    "parcela",
    "cremacion",
    "factura",
    "tarjeta",
    "emergencia",
    "activo",
    "usarCuotaIdeal",
  ].forEach((k) => {
    if (k in n) n[k] = toBool(n[k]);
  });

  ["fechaNac", "ingreso", "vigencia", "baja", "fechaAumento"].forEach((k) => {
    if (k in n) n[k] = toDateOrUndef(n[k]);
  });

  return n;
}

/* ===================== Helpers de grupo ===================== */

const ALLOWED_ROL = new Set(["TITULAR", "INTEGRANTE", "OTRO"]);

async function getGroupMembers(idCliente) {
  if (!Number.isFinite(Number(idCliente))) return [];
  return Cliente.find({ idCliente: Number(idCliente) }).lean();
}

function isActive(member) {
  if (member?.baja) {
    const d = new Date(member.baja);
    if (!Number.isNaN(d.getTime())) return false;
  }
  if (member?.activo === false) return false;
  return true;
}

function cmpEdadDesc(a, b) {
  const ea = Number(a?.edad ?? -1);
  const eb = Number(b?.edad ?? -1);
  return eb - ea;
}

async function getNextIntegranteIndex(idCliente) {
  const rows = await Cliente.find(
    { idCliente: Number(idCliente) },
    { integrante: 1 }
  ).lean();
  const used = new Set(
    rows.map((r) =>
      Number.isFinite(r.integrante) ? Number(r.integrante) : null
    )
  );
  let idx = 1;
  while (used.has(idx)) idx++;
  return idx;
}
async function setGroupHistoricalCuota(
  idCliente,
  newCuota,
  { onlyActive = true } = {}
) {
  const match = { idCliente: Number(idCliente) };
  if (onlyActive) {
    // Activo “robusto”: sin fecha de baja y activo !== false
    match.$and = [
      { $or: [{ activo: { $exists: false } }, { activo: true }] },
      { $expr: { $ne: [{ $type: "$baja" }, "date"] } },
    ];
  }
  await Cliente.updateMany(match, {
    $set: { cuota: Number(newCuota) },
  });
}
// === NUEVO helper: iguala la histórica a la ideal para 1 miembro ===
async function setMemberHistoricalToIdeal(memberId) {
  const doc = await Cliente.findById(memberId).lean();
  if (!doc) return;
  const ideal = Number(doc.cuotaIdeal) || 0;
  await Cliente.updateOne({ _id: memberId }, { $set: { cuota: ideal } });
}

async function resequenceIntegrantes(idCliente) {
  const all = await Cliente.find({ idCliente: Number(idCliente) }).lean();
  const titular = all.find((m) => m.rol === "TITULAR");
  const integrantes = all.filter(
    (m) => m._id?.toString() !== titular?._id?.toString()
  );
  integrantes.sort((a, b) => {
    const ia = Number.isFinite(a.integrante) ? a.integrante : 9999;
    const ib = Number.isFinite(b.integrante) ? b.integrante : 9999;
    return ia - ib;
  });
  let idx = 1;
  const bulk = [];
  for (const m of integrantes) {
    if (m.integrante !== idx) {
      bulk.push({
        updateOne: {
          filter: { _id: m._id },
          update: { $set: { integrante: idx } },
        },
      });
    }
    idx++;
  }
  if (bulk.length) await Cliente.bulkWrite(bulk);
}

async function propagateTitularName(idCliente, nombreTitular) {
  await Cliente.updateMany(
    { idCliente: Number(idCliente) },
    { $set: { nombreTitular: (nombreTitular || "").toString().trim() } }
  );
}

async function setAllActiveCuotaToIdeal(idCliente) {
  await Cliente.updateMany(
    {
      idCliente: Number(idCliente),
      $or: [{ activo: { $exists: false } }, { activo: true }],
      $expr: { $ne: [{ $type: "$baja" }, "date"] }, // baja no-date → activo
    },
    [
      {
        $set: {
          cuota: {
            $cond: [
              { $isNumber: "$cuotaIdeal" },
              "$cuotaIdeal",
              { $toDouble: { $ifNull: ["$cuotaIdeal", 0] } },
            ],
          },
        },
      },
    ]
  );
}

async function promoteOldestAsTitular(idCliente, excludeId) {
  const miembros = await getGroupMembers(idCliente);
  const candidatos = miembros
    .filter((m) => m._id?.toString() !== String(excludeId || ""))
    .filter(isActive)
    .sort(cmpEdadDesc);

  const nuevo = candidatos[0] || null;
  if (!nuevo) return null;

  // Demover titular actual si existe
  const oldTit = miembros.find((m) => m.rol === "TITULAR");
  if (oldTit && oldTit._id?.toString() !== String(nuevo._id)) {
    await Cliente.updateOne(
      { _id: oldTit._id },
      { $set: { rol: "INTEGRANTE" } }
    );
  }

  // Promover nuevo titular
  await Cliente.updateOne(
    { _id: nuevo._id },
    { $set: { rol: "TITULAR", integrante: 0 } }
  );

  // Resequence
  await resequenceIntegrantes(idCliente);

  // Propagar nombreTitular
  const nombreTit = (nuevo.nombre || "").toString().trim();
  await propagateTitularName(idCliente, nombreTit);

  return nuevo;
}

/* ===================================== SHOW ===================================== */

export async function getClienteById(req, res, next) {
  // ===== helpers locales =====
  const isValidDate = (v) => {
    if (!v) return false;
    const d = v instanceof Date ? v : new Date(v);
    return !Number.isNaN(d.getTime());
  };

  // Campos sensibles que removemos para el rol "cobrador"
  const SENSITIVE_KEYS = new Set([
    "domicilio",
    "ciudad",
    "provincia",
    "cp",
    "telefono",
    "documento",
    "docTipo",
    "cuil",
    "fechaNac",
    "observaciones",
    "tipoFactura",
    "factura",
    "tarjeta",
    "emergencia",
  ]);

  // Deja pasar únicamente un subconjunto (sin sensibles) para cobrador
  const allowForCollector = (o) => {
    if (!o || typeof o !== "object") return o;
    return {
      _id: o._id,
      idCliente: o.idCliente,
      nombre: o.nombre,
      rol: o.rol,
      integrante: o.integrante,
      nombreTitular: o.nombreTitular,
      sexo: o.sexo,
      edad: o.edad,
      activo: o.activo,
      baja: o.baja,
      // pricing (sin cuotaVigente)
      cuota: o.cuota,
      cuotaIdeal: o.cuotaIdeal,
      usarCuotaIdeal: o.usarCuotaIdeal,
      // flags de producto
      cremacion: o.cremacion,
      parcela: o.parcela,
      // opcionales neutrales
      idCobrador: o.idCobrador,
      ingreso: o.ingreso,
      vigencia: o.vigencia,
      updatedAt: o.updatedAt,
      createdAt: o.createdAt,
    };
  };

  const redactIfNeeded = (doc, redact) => {
    if (!doc) return doc;
    return redact ? allowForCollector(doc) : doc;
  };

  const redactFamilyIfNeeded = (arr, redact) => {
    const list = Array.isArray(arr) ? arr : [];
    return redact ? list.map((m) => allowForCollector(m)) : list;
  };

  // Activo robusto: baja es fecha válida ⇒ inactivo; activo === false ⇒ inactivo
  const isActiveRobust = (m) => {
    if (isValidDate(m?.baja)) return false;
    return m?.activo !== false;
  };

  try {
    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ message: "ID inválido" });

    const docRaw = await Cliente.findById(id).lean();
    if (!docRaw)
      return res.status(404).json({ message: "Cliente no encontrado" });

    const redact = !!req.redactSensitive;

    // data principal (con o sin redacción; sin cuotaVigente)
    const data = redactIfNeeded(docRaw, redact);
    const payload = { data };

    // expand=family | all
    const expand = String(req.query.expand || "").toLowerCase();
    const shouldExpandFamily =
      expand === "family" ||
      expand === "all" ||
      expand.split(",").includes("family");

    if (shouldExpandFamily) {
      const n = Number(docRaw.idCliente);
      if (Number.isFinite(n)) {
        // Leemos TODO el grupo de ese idCliente
        const list = await Cliente.find({ idCliente: n })
          .select(
            "_id idCliente nombre documento edad fechaNac activo cuota docTipo cuotaIdeal cremacion parcela rol integrante nombreTitular baja usarCuotaIdeal ingreso vigencia createdAt updatedAt sexo"
          )
          .sort({ rol: 1, integrante: 1, nombre: 1, _id: 1 })
          .lean();

        // family SIN el titular (para tablas/listas)
        const family = list.filter((m) => String(m._id) !== String(docRaw._id));

        // Activos robustos y SOLO póliza (TITULAR/INTEGRANTE)
        const ALLOWED = new Set(["TITULAR", "INTEGRANTE"]);
        const isValidDate = (v) => {
          if (!v) return false;
          const d = v instanceof Date ? v : new Date(v);
          return !Number.isNaN(d.getTime());
        };
        const isActiveRobust = (m) =>
          m?.activo !== false && !isValidDate(m?.baja);

        // ⚠️ MUY IMPORTANTE: incluir al titular en el cómputo
        const todos = [docRaw, ...family];
        const activosPoliza = todos
          .filter(isActiveRobust)
          .filter((m) => ALLOWED.has(m?.rol));

        const cremacionesCount = activosPoliza.reduce(
          (acc, m) => acc + (m?.cremacion ? 1 : 0),
          0
        );

        const edades = activosPoliza
          .map((m) =>
            Number.isFinite(Number(m?.edad))
              ? Number(m.edad)
              : m?.fechaNac
              ? (() => {
                  const d = new Date(m.fechaNac);
                  if (Number.isNaN(d.getTime())) return undefined;
                  const t = new Date();
                  let a = t.getFullYear() - d.getFullYear();
                  const mm = t.getMonth() - d.getMonth();
                  if (mm < 0 || (mm === 0 && t.getDate() < d.getDate())) a--;
                  return a;
                })()
              : undefined
          )
          .filter((x) => Number.isFinite(x));

        const edadMax = edades.length
          ? Math.max(...edades)
          : Number(docRaw.edad) || 0;

        payload.family = family; // sin titular
        payload.__groupInfo = {
          integrantesCount: activosPoliza.length, // titular + integrantes activos
          cremacionesCount,
          edadMax,
        };
      } else {
        payload.family = [];
        payload.__groupInfo = {
          integrantesCount: 1,
          cremacionesCount: docRaw.cremacion ? 1 : 0,
          edadMax: Number(docRaw.edad) || 0,
        };
      }
    }

    return res.json(payload);
  } catch (err) {
    next(err);
  }
}

/* ===================================== LIST ===================================== */

export async function listClientes(req, res, next) {
  try {
    const page = Math.max(parseInt(req.query.page) || 1, 1);
    const limit = Math.min(parseInt(req.query.limit) || 10, 100);
    const qRaw = (req.query.q || "").trim();

    const onlyDigits = (s = "") => String(s).replace(/\D+/g, "");

    // Filtros directos
    const byIdClienteRaw = req.query.byIdCliente;
    const byIdCliente =
      byIdClienteRaw !== undefined && byIdClienteRaw !== ""
        ? Number(byIdClienteRaw)
        : undefined;
    const hasByIdCliente = Number.isFinite(byIdCliente);

    // DNI/documento (prioridad máxima)
    const byDocumentoRaw = (req.query.byDocumento ?? "").toString().trim();
    const byDocumentoDigits = onlyDigits(byDocumentoRaw);
    const hasByDocumento = byDocumentoDigits.length >= 6;

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
      "cuotaVigente",
      "updatedAt",
    ]);
    const sortBy = SORTABLE.has(sortByParam) ? sortByParam : "createdAt";

    // ===== ¿Necesitamos normalizar documento? =====
    const needsDocDigits =
      hasByDocumento || (qRaw && onlyDigits(qRaw).length >= 6);

    // _docDigits = documento en string sin . - espacio /
    const DOC_CLEAN_STR = { $toString: { $ifNull: ["$documento", ""] } }; // ← FIX clave
    const DOC_DIGITS_EXPR = {
      $replaceAll: {
        input: {
          $replaceAll: {
            input: {
              $replaceAll: {
                input: {
                  $replaceAll: {
                    input: DOC_CLEAN_STR,
                    find: ".",
                    replacement: "",
                  },
                },
                find: "-",
                replacement: "",
              },
            },
            find: " ",
            replacement: "",
          },
        },
        find: "/",
        replacement: "",
      },
    };

    // === Armar condiciones ===
    const or = [];
    const qDigits = onlyDigits(qRaw);
    const isNumeric = /^\d+$/.test(qRaw);

    if (hasByDocumento) {
      // DNI directo: igualdad por dígitos normalizados + exacto tal cual
      or.push({ $expr: { $eq: ["$_docDigits", byDocumentoDigits] } });
      or.push({ documento: byDocumentoRaw });
    } else if (hasByIdCliente) {
      or.push({ idCliente: byIdCliente });
    } else if (qRaw) {
      // Global
      or.push({ nombre: { $regex: qRaw, $options: "i" } });
      or.push({ domicilio: { $regex: qRaw, $options: "i" } });
      or.push({ documento: { $regex: qRaw, $options: "i" } });

      if (isNumeric) {
        or.push({ idCliente: Number(qRaw) });
        or.push({ idCobrador: Number(qRaw) });
        if (qDigits.length >= 6) {
          or.push({ $expr: { $eq: ["$_docDigits", qDigits] } });
        }
      }
    }

    const matchStage = or.length > 0 ? { $or: or } : {};

    // ===== Pipelines =====
    const preStages = [];
    if (needsDocDigits)
      preStages.push({ $addFields: { _docDigits: DOC_DIGITS_EXPR } });
    if (Object.keys(matchStage).length) preStages.push({ $match: matchStage });

    // Conteo (por grupo)
    const totalAgg = await Cliente.aggregate([
      ...preStages,
      { $group: { _id: "$idCliente" } },
      { $count: "n" },
    ]);
    const total = totalAgg?.[0]?.n || 0;

    // Items (1 por grupo)
    const itemsAgg = Cliente.aggregate([
      ...preStages,
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
              {
                $and: [
                  { $toBool: { $ifNull: ["$usarCuotaIdeal", false] } },
                  { $isNumber: "$cuotaIdeal" },
                ],
              },
              "$cuotaIdeal",
              { $ifNull: ["$cuota", 0] },
            ],
          },
          _isActive: {
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
          integrantesCount: { $sum: { $cond: ["$_isActive", 1, 0] } },
          cremacionesCount: {
            $sum: {
              $cond: [
                { $and: ["$_isActive", { $toBool: "$cremacion" }] },
                1,
                0,
              ],
            },
          },
          edadMax: {
            $max: {
              $cond: [
                "$_isActive",
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
          documento: "$firstDoc.documento",
          docTipo: "$firstDoc.docTipo",
          sexo: "$firstDoc.sexo",
          idCobrador: "$firstDoc.idCobrador",
          cuota: "$firstDoc.cuota",
          cuotaIdeal: "$firstDoc.cuotaIdeal",
          usarCuotaIdeal: "$firstDoc.usarCuotaIdeal",
          cuotaVigente: "$firstDoc._cuotaVigente",
          parcela: "$firstDoc.parcela",
          cremacion: "$firstDoc.cremacion",
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
      {
        $sort:
          sortBy === "createdAt"
            ? { createdAtSafe: sortDir, _id: sortDir }
            : { [sortBy]: sortDir, _id: sortDir },
      },
      { $skip: (page - 1) * limit },
      { $limit: limit },
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

/* ===================================== CREATE ===================================== */

export async function createCliente(req, res, next) {
  const session = await Cliente.startSession();
  session.startTransaction();
  try {
    const payload = normalizePayload(req.body);

    const integrantesRaw = Array.isArray(req.body.integrantes)
      ? req.body.integrantes
      : [];
    const integrantes = integrantesRaw.map(normalizePayload);
    delete payload.integrantes;

    if (!payload.idCliente && payload.idCliente !== 0) {
      const last = await Cliente.findOne({}, { idCliente: 1, _id: 0 })
        .sort({ idCliente: -1 })
        .lean();
      payload.idCliente = (last?.idCliente ?? 0) + 1;
    }

    const edadTitular = payload.fechaNac
      ? ageFromDate(payload.fechaNac)
      : payload.edad;
    if (typeof edadTitular === "number") payload.edad = edadTitular;

    // Rol titular default coherente
    const titularRol = ALLOWED_ROL.has(payload.rol) ? payload.rol : "TITULAR";
    payload.rol = titularRol;
    payload.integrante = titularRol === "TITULAR" ? 0 : payload.integrante ?? 1;

    const [titular] = await Cliente.create([payload], { session });

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
      "cremacion",
      "rol",
      "integrante",
      "nombreTitular",
      "usarCuotaIdeal",
    ];

    const nombreTit = (payload.nombre || "").trim();
    let nextIdx = await getNextIntegranteIndex(payload.idCliente);

    const familiaresDocs = integrantes
      .map((fam) => {
        const edad = fam.fechaNac ? ageFromDate(fam.fechaNac) : fam.edad;
        const base = { ...fam, edad };
        const famRol = ALLOWED_ROL.has(base.rol) ? base.rol : "INTEGRANTE";
        let famIdx = toNumOrUndef(base.integrante);

        const rolFinal = famRol === "TITULAR" ? "INTEGRANTE" : famRol;

        if (!Number.isFinite(famIdx) || famIdx === 0) {
          famIdx = nextIdx++;
        }

        return {
          ...pick(base, FIELDS),
          idCliente: payload.idCliente,
          rol: rolFinal,
          integrante: famIdx,
          nombreTitular: (base.nombreTitular || "").trim() || nombreTit,
        };
      })
      .filter((d) => (d.nombre || "").toString().trim() !== "");

    if (familiaresDocs.length) {
      await Cliente.insertMany(familiaresDocs, { session, ordered: true });
    }

    await session.commitTransaction();
    session.endSession();

    // Post: propagar nombreTitular + re-precio + ajustar cuota
    await propagateTitularName(payload.idCliente, nombreTit);
    await recomputeGroupPricing(payload.idCliente, { debug: false });
    await setAllActiveCuotaToIdeal(payload.idCliente);

    const titularFresh = await Cliente.findById(titular._id).lean();
    const cuotaVigente = titularFresh?.usarCuotaIdeal
      ? titularFresh?.cuotaIdeal ?? 0
      : titularFresh?.cuota ?? 0;

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

export async function updateCliente(req, res, next) {
  try {
    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ message: "ID inválido" });

    const payloadRaw = { ...req.body };
    delete payloadRaw._id;
    const payload = normalizePayload(payloadRaw);

    if (payload.hasOwnProperty("idCliente")) {
      const n = Number(payload.idCliente);
      if (!Number.isFinite(n))
        return res.status(400).json({ message: "idCliente debe ser numérico" });
      payload.idCliente = n;
    }

    if (payload.fechaNac && !payload.edad) {
      const edad = ageFromDate(payload.fechaNac);
      if (typeof edad === "number") payload.edad = edad;
    }

    const current = await Cliente.findById(id).lean();
    if (!current)
      return res.status(404).json({ message: "Cliente no encontrado" });

    const gid = Number(payload.idCliente ?? current.idCliente);
    const wasTitular = current.rol === "TITULAR";

    if (payload.rol && !ALLOWED_ROL.has(payload.rol)) delete payload.rol;

    // ===== Detectar cambios =====
    const touchedKeys = new Set(Object.keys(payload));
    const manualCuotaChange = touchedKeys.has("cuota");
    const newCuota = manualCuotaChange ? Number(payload.cuota) : undefined;

    const priceAffectingChange = [
      "edad",
      "fechaNac",
      "cremacion",
      "rol",
      "activo",
      "baja",
      "parcela",
    ].some((k) => touchedKeys.has(k));

    // Update base
    const updated = await Cliente.findByIdAndUpdate(
      id,
      { $set: payload },
      { new: true, runValidators: true }
    ).lean();

    // ===== Orquestación de grupo =====
    let mustPromote = false;
    const nowInactive =
      updated?.activo === false ||
      (updated?.baja && !Number.isNaN(new Date(updated.baja).getTime()));

    if (wasTitular && nowInactive) mustPromote = true;

    if (payload.rol === "TITULAR" && !wasTitular) {
      await Cliente.updateMany(
        { idCliente: gid, rol: "TITULAR", _id: { $ne: updated._id } },
        { $set: { rol: "INTEGRANTE" } }
      );
      await Cliente.updateOne(
        { _id: updated._id },
        { $set: { integrante: 0 } }
      );
      await resequenceIntegrantes(gid);
      await propagateTitularName(gid, (updated?.nombre || "").trim());
    }

    if (mustPromote) {
      await promoteOldestAsTitular(gid, updated._id);
    } else if (!(payload.rol === "TITULAR" && !wasTitular)) {
      const titularDoc = await Cliente.findOne({
        idCliente: gid,
        rol: "TITULAR",
      }).lean();
      if (titularDoc) {
        await propagateTitularName(gid, (titularDoc?.nombre || "").trim());
      }
    }

    // ===== Repricing (solo ideal) =====
    if (
      priceAffectingChange ||
      mustPromote ||
      touchedKeys.has("usarCuotaIdeal")
    ) {
      await recomputeGroupPricing(gid, { debug: false });
    }

    // ===== Política pedida (caso "ON"): si usarCuotaIdeal === true => alinear histórica a ideal =====
    const flagPresent = touchedKeys.has("usarCuotaIdeal");
    const flagIsTrue = payload.usarCuotaIdeal === true;

    // parámetro opcional (se mantiene como estaba)
    const propagateGroup =
      String(req.query.propagate || "").toLowerCase() === "1" ||
      payload.propagate === true;

    if (flagPresent && flagIsTrue) {
      if (propagateGroup) {
        await setAllActiveCuotaToIdeal(gid);
      } else {
        await setMemberHistoricalToIdeal(updated._id);
      }
    } else {
      // === NUEVO: caso "OFF" + cambio de cuota => propagar histórica manual a todo el grupo activo ===
      const wasUsingIdeal = !!current.usarCuotaIdeal; // ← NUEVO
      const turnedOffIdeal =
        flagPresent && wasUsingIdeal && payload.usarCuotaIdeal === false; // ← NUEVO

      if (turnedOffIdeal && manualCuotaChange && Number.isFinite(newCuota)) {
        // ← NUEVO
        await setGroupHistoricalCuota(gid, newCuota, { onlyActive: true }); // ← NUEVO
      } else if (manualCuotaChange && Number.isFinite(newCuota)) {
        // (se deja como opcional el comportamiento anterior)
        // await setGroupHistoricalCuota(gid, newCuota, { onlyActive: true });
      }
    }

    const fresh = await Cliente.findById(id).lean();
    const cuotaVigente = fresh?.usarCuotaIdeal
      ? fresh?.cuotaIdeal ?? 0
      : fresh?.cuota ?? 0;

    return res.json({ data: { ...fresh, cuotaVigente } });
  } catch (err) {
    next(err);
  }
}

/* ===================================== DELETE (Soft) ===================================== */

export async function deleteCliente(req, res, next) {
  try {
    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ message: "ID inválido" });

    // 1) Soft delete: baja + activo=false
    const doc = await Cliente.findById(id).lean();
    if (!doc) return res.status(404).json({ message: "Cliente no encontrado" });

    const gid = Number(doc.idCliente);

    await Cliente.updateOne(
      { _id: id },
      { $set: { baja: new Date(), activo: false } }
    );

    // 2) Si era TITULAR → promover al de mayor edad activo
    if (doc.rol === "TITULAR" && Number.isFinite(gid)) {
      await promoteOldestAsTitular(gid, id);
    } else if (Number.isFinite(gid)) {
      // Si no era titular, igual resecuenciamos por prolijidad
      await resequenceIntegrantes(gid);
      // Propagamos nombre de titular actual por si cambió antes
      const titularDoc = await Cliente.findOne({
        idCliente: gid,
        rol: "TITULAR",
      }).lean();
      if (titularDoc) {
        await propagateTitularName(gid, (titularDoc?.nombre || "").trim());
      }
    }

    // 3) Re-precio grupo y alinear cuota de activos a ideal (post-sepelio)
    if (Number.isFinite(gid)) {
      await recomputeGroupPricing(gid, { debug: false });
      await setAllActiveCuotaToIdeal(gid);
    }

    return res.json({ ok: true, _id: id, idCliente: gid });
  } catch (err) {
    next(err);
  }
}

/* ===================================== STATS (placeholder) ===================================== */

// src/controllers/admin.stats.controller.js

/**
 * GET /admin/clientes/stats?period=YYYY-MM&dateFrom=YYYY-MM-DD&dateTo=YYYY-MM-DD&idCobrador=...
 *
 * Resumen integral de KPIs mezclando modelos:
 * - Clientes activos por período (grupos y miembros)
 * - Debe del período (sumatoria cuotas efectivas por grupo)
 * - Cobertura del período (pagado vs debido) y desgloses por cobrador
 * - Aging de deuda por grupo
 * - Mix de métodos/canales
 * - Tickets (avg/median)
 * - Top positivos/negativos (gap pagado - debido)
 * - Snapshot ledger últimos 30 días (caja/ingresos)
 */
export async function getClientesStats(req, res, next) {
  try {
    const {
      period, // "YYYY-MM" → obligatorio para cobertura
      dateFrom, // opcional (para stats de pagos)
      dateTo, // opcional (para stats de pagos)
      idCobrador, // opcional (filtrar por cobrador)
      method, // opcional (filtrar por método)
      channel, // opcional
      currency = "ARS",
    } = req.query;

    // === Helpers de fechas/periodos ===
    function parseISODate(s, def) {
      if (!s) return def;
      const d = new Date(s);
      return Number.isNaN(d.getTime()) ? def : d;
    }

    function getPeriodBounds(yyyyMm) {
      if (!/^\d{4}-\d{2}$/.test(String(yyyyMm || ""))) return null;
      const [y, m] = yyyyMm.split("-").map((n) => parseInt(n, 10));
      const start = new Date(Date.UTC(y, m - 1, 1, 0, 0, 0));
      const end = new Date(Date.UTC(y, m, 1, 0, 0, 0)); // excluyente
      return { start, end };
    }

    const now = new Date();
    const bounds = getPeriodBounds(period || "");
    if (!bounds) {
      return res
        .status(400)
        .json({ ok: false, message: "Falta o es inválido ?period=YYYY-MM" });
    }
    const { start: periodStartUTC, end: periodEndUTC } = bounds;

    // Ventana general para "pagos recientes" y ledger snapshot por default (últimos 30 días)
    const defaultFrom = new Date(now.getTime() - 30 * 86400000);
    const df = parseISODate(dateFrom, defaultFrom);
    const dt = parseISODate(dateTo, now);

    // Filtros comunes
    const paymentMatch = {
      currency,
      status: { $in: ["posted", "settled"] },
      ...(idCobrador ? { "collector.idCobrador": Number(idCobrador) } : {}),
      ...(method ? { method } : {}),
      ...(channel ? { channel } : {}),
      createdAt: { $gte: df, $lt: dt },
    };

    // ===== 1) BASE: “debido” del período por GRUPO =====
    // Clientes activos en el período (no dados de baja antes del inicio).
    // cuotaEfectiva = usarCuotaIdeal ? cuotaIdeal : cuota
    // DebidoGrupoPeriodo = SUM(cuotaEfectiva de todos los miembros activos del grupo)
    const debidoPorGrupo = await Cliente.aggregate([
      {
        $match: {
          activo: true,
          $or: [{ baja: null }, { baja: { $gte: periodStartUTC } }],
        },
      },
      {
        $addFields: {
          cuotaEfectiva: {
            $cond: [
              { $eq: ["$usarCuotaIdeal", true] },
              { $ifNull: ["$cuotaIdeal", 0] },
              { $ifNull: ["$cuota", 0] },
            ],
          },
        },
      },
      {
        $group: {
          _id: "$idCliente", // grupo
          idCliente: { $first: "$idCliente" },
          nombreTitular: { $first: "$nombreTitular" },
          miembros: { $sum: 1 },
          debido: { $sum: "$cuotaEfectiva" },
          idCobrador: { $first: "$idCobrador" },
        },
      },
    ]);

    // Índices rápidos
    const debidoMap = new Map();
    let totalDebido = 0;
    for (const row of debidoPorGrupo) {
      debidoMap.set(row.idCliente, row);
      totalDebido += row.debido || 0;
    }

    // ===== 2) COBERTURA DEL PERÍODO (pagado aplicado a allocations.period === period) =====
    // Sumamos amountApplied por grupo para allocations del período
    const pagadoPeriodo = await Payment.aggregate([
      {
        $match: {
          currency,
          status: { $in: ["posted", "settled"] },
          "allocations.period": period,
          ...(idCobrador ? { "collector.idCobrador": Number(idCobrador) } : {}),
          ...(method ? { method } : {}),
          ...(channel ? { channel } : {}),
        },
      },
      { $unwind: "$allocations" },
      { $match: { "allocations.period": period } },
      {
        $group: {
          _id: "$cliente.idCliente",
          idCliente: { $first: "$cliente.idCliente" },
          pagado: { $sum: "$allocations.amountApplied" },
          // para mix por cobrador
          idCobrador: { $first: "$collector.idCobrador" },
        },
      },
    ]);

    const pagoMap = new Map();
    let totalPagadoPeriodo = 0;
    for (const row of pagadoPeriodo) {
      pagoMap.set(row.idCliente, row.pagado || 0);
      totalPagadoPeriodo += row.pagado || 0;
    }

    // ===== 3) Construimos COVERAGE por grupo + gaps (top +/-) =====
    const coverage = [];
    const positive = [];
    const negative = [];

    for (const row of debidoPorGrupo) {
      const due = row.debido || 0;
      const paid = pagoMap.get(row.idCliente) || 0;
      const gap = Number((paid - due).toFixed(2));
      const statusAfter =
        paid >= due
          ? "paid"
          : paid > 0
          ? "partial"
          : due > 0
          ? "unpaid"
          : "zero";

      const item = {
        idCliente: row.idCliente,
        nombreTitular: row.nombreTitular || null,
        idCobrador: row.idCobrador ?? null,
        miembros: row.miembros,
        due,
        paid,
        gap,
        status: statusAfter,
      };
      coverage.push(item);
      if (gap >= 0) positive.push(item);
      else negative.push(item);
    }

    // Ordenar top lists
    const topPositive = [...positive]
      .sort((a, b) => b.gap - a.gap)
      .slice(0, 15);
    const topNegative = [...negative]
      .sort((a, b) => a.gap - b.gap)
      .slice(0, 15);

    // ===== 4) BY COBRADOR: agregados (cobertura, due, paid, mix, tickets) =====
    // Reutilizamos coverage para KPIs rápidos y completamos con mix desde payments
    const byCobradorBase = new Map(); // idCobrador -> { due, paid, grupos, paidCount, partialCount, unpaidCount }
    for (const g of coverage) {
      if (g.idCobrador == null) continue;
      const acc = byCobradorBase.get(g.idCobrador) || {
        idCobrador: g.idCobrador,
        due: 0,
        paid: 0,
        grupos: 0,
        paidCount: 0,
        partialCount: 0,
        unpaidCount: 0,
      };
      acc.due += g.due;
      acc.paid += g.paid;
      acc.grupos += 1;
      if (g.status === "paid") acc.paidCount += 1;
      else if (g.status === "partial") acc.partialCount += 1;
      else if (g.status === "unpaid") acc.unpaidCount += 1;
      byCobradorBase.set(g.idCobrador, acc);
    }

    // Mix de método/canal y tickets por cobrador (sobre ventana df..dt)
    const pagosVentana = await Payment.aggregate([
      { $match: paymentMatch },
      {
        $group: {
          _id: {
            idCobrador: "$collector.idCobrador",
            method: "$method",
            channel: "$channel",
          },
          count: { $sum: 1 },
          amount: { $sum: "$amount" },
        },
      },
    ]);

    // Distribuciones por cobrador
    const cobradorMix = new Map(); // idCobrador -> { methods: {..}, channels: {..}, tickets: {avg,median,count,sum} }
    // Necesitamos también tickets por cobrador para promedio/mediana
    const tickets = await Payment.aggregate([
      { $match: paymentMatch },
      {
        $group: {
          _id: "$collector.idCobrador",
          count: { $sum: 1 },
          sum: { $sum: "$amount" },
          amounts: { $push: "$amount" },
        },
      },
    ]);

    for (const t of tickets) {
      const arr = (t.amounts || []).sort((a, b) => a - b);
      const mid = Math.floor(arr.length / 2);
      const median =
        arr.length === 0
          ? 0
          : arr.length % 2
          ? arr[mid]
          : (arr[mid - 1] + arr[mid]) / 2;
      cobradorMix.set(t._id, {
        methods: {},
        channels: {},
        tickets: {
          count: t.count || 0,
          sum: Number((t.sum || 0).toFixed(2)),
          avg: t.count ? Number((t.sum / t.count).toFixed(2)) : 0,
          median: Number((median || 0).toFixed(2)),
        },
      });
    }

    for (const p of pagosVentana) {
      const idCob = p._id?.idCobrador ?? null;
      if (idCob == null) continue;
      const row = cobradorMix.get(idCob) || {
        methods: {},
        channels: {},
        tickets: { count: 0, sum: 0, avg: 0, median: 0 },
      };
      row.methods[p._id.method || "otro"] =
        (row.methods[p._id.method || "otro"] || 0) + p.amount;
      row.channels[p._id.channel || "otro"] =
        (row.channels[p._id.channel || "otro"] || 0) + p.amount;
      cobradorMix.set(idCob, row);
    }

    // Completar “byCobrador”
    const byCobrador = [];
    for (const [idCob, base] of byCobradorBase.entries()) {
      const mix = cobradorMix.get(idCob) || {
        methods: {},
        channels: {},
        tickets: { count: 0, sum: 0, avg: 0, median: 0 },
      };
      const coverageRate =
        base.due > 0 ? Number((base.paid / base.due).toFixed(4)) : 0;
      byCobrador.push({
        idCobrador: idCob,
        due: Number(base.due.toFixed(2)),
        paid: Number(base.paid.toFixed(2)),
        grupos: base.grupos,
        coverageRate,
        distribution: {
          methods: mix.methods,
          channels: mix.channels,
        },
        tickets: mix.tickets,
        counts: {
          paid: base.paidCount,
          partial: base.partialCount,
          unpaid: base.unpaidCount,
        },
      });
    }
    // Ordenar por cobertura y luego por paid
    byCobrador.sort(
      (a, b) => b.coverageRate - a.coverageRate || b.paid - a.paid
    );

    // ===== 5) AGING de deuda por grupo (gap negativo convertido en “deuda” del período) =====
    // Nota: Aging real multi-período requeriría allocations históricos; acá hacemos un aging “rápido”
    // sobre el gap del período. Si querés aging multi-mes, armamos V2 con proyección de períodos abiertos.
    function bucketizeGap(gapValue) {
      // Solo medimos deuda del período actual: si gap < 0 → deuda actual (0-30)
      // (V2: extender con saldos de períodos anteriores)
      if (gapValue >= 0) return null;
      return "0-30";
    }

    const agingBuckets = { "0-30": 0, "31-60": 0, "61-90": 0, "90+": 0 }; // placeholders V2
    let gruposConDeuda = 0;
    for (const g of coverage) {
      const b = bucketizeGap(g.gap);
      if (b) {
        agingBuckets[b] += Math.abs(g.gap);
        gruposConDeuda += 1;
      }
    }

    // ===== 6) MIX general (método/canal) y tickets globales en ventana df..dt =====
    const mixGeneral = await Payment.aggregate([
      { $match: paymentMatch },
      {
        $group: {
          _id: { method: "$method", channel: "$channel" },
          count: { $sum: 1 },
          amount: { $sum: "$amount" },
        },
      },
    ]);

    let ticketsGlobal = { count: 0, sum: 0, avg: 0, median: 0 };
    {
      const r = await Payment.aggregate([
        { $match: paymentMatch },
        {
          $group: {
            _id: null,
            count: { $sum: 1 },
            sum: { $sum: "$amount" },
            amounts: { $push: "$amount" },
          },
        },
      ]);
      if (r.length) {
        const a = r[0].amounts.sort((x, y) => x - y);
        const m = Math.floor(a.length / 2);
        const median =
          a.length === 0 ? 0 : a.length % 2 ? a[m] : (a[m - 1] + a[m]) / 2;
        ticketsGlobal = {
          count: r[0].count,
          sum: Number((r[0].sum || 0).toFixed(2)),
          avg: r[0].count ? Number((r[0].sum / r[0].count).toFixed(2)) : 0,
          median: Number((median || 0).toFixed(2)),
        };
      }
    }

    // ===== 7) LEDGER snapshot últimos 30 días (o df..dt) =====
    const ledgerSnapshot = await LedgerEntry.aggregate([
      {
        $match: {
          currency,
          postedAt: { $gte: df, $lt: dt },
        },
      },
      {
        $group: {
          _id: "$accountCode",
          amount: { $sum: "$amount" },
          debit: {
            $sum: { $cond: [{ $eq: ["$side", "debit"] }, "$amount", 0] },
          },
          credit: {
            $sum: { $cond: [{ $eq: ["$side", "credit"] }, "$amount", 0] },
          },
        },
      },
      { $sort: { _id: 1 } },
    ]);

    // ===== 8) USERS (opcional: mapa idCobrador -> nombre) =====
    const cobradoresUsers = await User.find({ idCobrador: { $ne: null } })
      .select({ name: 1, idCobrador: 1 })
      .lean();

    const cobradorNameMap = new Map();
    for (const u of cobradoresUsers) {
      if (u.idCobrador != null)
        cobradorNameMap.set(Number(u.idCobrador), u.name);
    }

    // Enriquecer byCobrador con nombres
    for (const row of byCobrador) {
      row.cobradorNombre = cobradorNameMap.get(Number(row.idCobrador)) || null;
    }

    // ===== 9) SUMMARY general =====
    const totalGrupos = debidoPorGrupo.length;
    const totalMiembros = await Cliente.countDocuments({
      activo: true,
      $or: [{ baja: null }, { baja: { $gte: periodStartUTC } }],
    });

    const fullyPaid = coverage.filter((c) => c.status === "paid").length;
    const partially = coverage.filter((c) => c.status === "partial").length;
    const unpaid = coverage.filter((c) => c.status === "unpaid").length;

    const coverageRateGlobal =
      totalDebido > 0
        ? Number((totalPagadoPeriodo / totalDebido).toFixed(4))
        : 0;

    // Mix general formateado
    const mix = { methods: {}, channels: {} };
    for (const row of mixGeneral) {
      const m = row._id.method || "otro";
      const ch = row._id.channel || "otro";
      mix.methods[m] = (mix.methods[m] || 0) + row.amount;
      mix.channels[ch] = (mix.channels[ch] || 0) + row.amount;
    }

    return res.json({
      ok: true,
      data: {
        period,
        window: { from: df.toISOString(), to: dt.toISOString() },
        summary: {
          totalGrupos,
          totalMiembros,
          totalDebido: Number(totalDebido.toFixed(2)),
          totalPagadoPeriodo: Number(totalPagadoPeriodo.toFixed(2)),
          coverageRate: coverageRateGlobal, // pagado/debido
          grupos: { paid: fullyPaid, partial: partially, unpaid },
          ticketsGlobal,
          mix,
        },

        // Cobertura por grupo (para tablas o drilldowns)
        coverage, // [{ idCliente, due, paid, gap, status, ... }]

        // Ranking por cobrador (mezcla due/pagado/mix/tickets)
        byCobrador,

        // Aging rápido del período (V2: extender a multi-período)
        aging: {
          buckets: agingBuckets,
          gruposConDeuda,
        },

        // Top 15 mejores y peores gaps
        topPositive,
        topNegative,

        // Ledger último tramo (caja/ingresos, etc.)
        ledgerSnapshot,
      },
      meta: {
        generatedAt: new Date().toISOString(),
        currency,
        notes: [
          "El debido del período por grupo = suma de cuotas efectivas (usarCuotaIdeal? cuotaIdeal : cuota) de los miembros activos.",
          "La cobertura del período usa Payment.allocations filtradas por allocations.period === period.",
          "El aging que ves es del período actual (0-30). Para aging multi-mes armamos V2 con saldos acumulados.",
        ],
      },
    });
  } catch (err) {
    next(err);
  }
}
