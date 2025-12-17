 // src/controllers/clientes.controller.js  (ESM)

import mongoose from "mongoose";
import Cliente from "../models/client.model.js";
import { recomputeGroupPricing } from "../services/pricing.services.js";
import Payment from "../models/payment.model.js";
import LedgerEntry from "../models/ledger-entry.model.js";
import User from "../models/user.model.js";
import { yyyymmAR, comparePeriod } from "./payments.shared.js";
import { getClientPeriodState } from "../services/debt.service.js";

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

  // Activo robusto: baja es fecha válida ⇒ inactivo; activo === false ⇒ inactivo
  const isActiveRobust = (m) => {
    if (isValidDate(m?.baja)) return false;
    return m?.activo !== false;
  };

  const PERIOD_RE = /^\d{4}-(0[1-9]|1[0-2])$/;
  const normalizePeriod = (s) => {
    const str = String(s || "").trim();
    return PERIOD_RE.test(str) ? str : null;
  };
  const localComparePeriod = (a, b) => {
    const A = normalizePeriod(a);
    const B = normalizePeriod(b);
    if (!A || !B) return 0;
    return A === B ? 0 : A < B ? -1 : 1;
  };

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
      // pricing (agregamos cuotaVigente)
      cuota: o.cuota,
      cuotaIdeal: o.cuotaIdeal,
      usarCuotaIdeal: o.usarCuotaIdeal,
      cuotaVigente: o.cuotaVigente,
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

  try {
    const id = String(req.params.id || "").trim();
    if (!isObjectId(id))
      return res.status(400).json({ ok: false, message: "ID inválido" });

    const docRaw = await Cliente.findById(id).lean();
    if (!docRaw)
      return res
        .status(404)
        .json({ ok: false, message: "Cliente no encontrado" });

    const redact = !!req.redactSensitive;

    // ==== cuotaVigente igual que en /collector/clientes/:id ====
    const cuotaVigente = docRaw.usarCuotaIdeal
      ? Number(docRaw.cuotaIdeal || 0)
      : Number(docRaw.cuota || 0);

    const baseDoc = { ...docRaw, cuotaVigente };

    // data principal (con o sin redacción)
    const data = redactIfNeeded(baseDoc, redact);

    // payload base: RESPUESTA CON EL MISMO SHAPE QUE EL COBRADOR
    const payload = {
      ok: true,
      data,
    };

    // ---------- expand flags ----------
    const expandRaw = String(req.query.expand || "").toLowerCase();
    const expandTokens = expandRaw
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean);

    const shouldExpandFamily =
      expandTokens.includes("family") || expandTokens.includes("all");

    const shouldExpandDebt =
      expandTokens.includes("debt") || expandTokens.includes("all");

    // ================= FAMILY (grupo) =================
    if (shouldExpandFamily) {
      const n = Number(docRaw.idCliente);
      if (Number.isFinite(n)) {
        const list = await Cliente.find({ idCliente: n })
          .select(
            "_id idCliente nombre documento edad fechaNac activo cuota " +
              "docTipo cuotaIdeal cremacion parcela rol integrante nombreTitular " +
              "baja usarCuotaIdeal ingreso vigencia createdAt updatedAt sexo " +
              "ciudad provincia cp telefono idCobrador"
          )
          .sort({ rol: 1, integrante: 1, nombre: 1, _id: 1 })
          .lean();

        // mismo criterio que collector: excluir el propio miembro
        const familyRaw = list.filter(
          (m) => String(m._id) !== String(docRaw._id)
        );

        // añadimos cuotaVigente a cada miembro (igual que en getCollectorClientById)
        const familyWithCuota = familyRaw.map((m) => ({
          ...m,
          cuotaVigente: m.usarCuotaIdeal
            ? Number(m.cuotaIdeal || 0)
            : Number(m.cuota || 0),
        }));

        const ALLOWED_ROL = new Set(["TITULAR", "INTEGRANTE"]);

        const todos = [baseDoc, ...familyWithCuota];
        const activosPoliza = todos
          .filter(isActiveRobust)
          .filter((m) => ALLOWED_ROL.has(m?.rol));

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
                  const d =
                    m.fechaNac instanceof Date
                      ? m.fechaNac
                      : new Date(m.fechaNac);
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

        // family con el mismo shape (pero con soporte de redacción)
        payload.family = redactFamilyIfNeeded(familyWithCuota, redact);

        // info de grupo extra para admin
        payload.__groupInfo = {
          integrantesCount: activosPoliza.length,
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
    } else {
      // por compatibilidad, siempre devolvemos family aunque sea []
      if (!("family" in payload)) {
        payload.family = [];
      }
    }

    // ================= DEBT (vista admin, similar a /collector/clientes/:id/deuda) =================
    if (shouldExpandDebt) {
      try {
        const { from, to, includeFuture } = req.query || {};

        const base = await getClientPeriodState(baseDoc, {
          from,
          to,
          includeFuture: Number(includeFuture),
        });

        let periods = Array.isArray(base?.periods) ? [...base.periods] : [];
        const nowPeriod = yyyymmAR(new Date());

        // ordenamos por período
        periods.sort((a, b) => localComparePeriod(a.period, b.period));

        const totalDueUpToNow = periods
          .filter(
            (p) =>
              p?.period &&
              localComparePeriod(p.period, nowPeriod) <= 0 &&
              Number(p.balance ?? 0) > 0
          )
          .reduce((acc, p) => acc + Math.max(0, Number(p.balance || 0)), 0);

        const monthsDue = periods.filter(
          (p) =>
            p?.period &&
            localComparePeriod(p.period, nowPeriod) <= 0 &&
            Number(p.balance ?? 0) > 0
        ).length;

        let lastDuePeriod = null;
        for (const p of periods) {
          const per = p.period;
          const bal = Number(p.balance || 0);
          if (!per || bal <= 0) continue;
          if (localComparePeriod(per, nowPeriod) <= 0) {
            if (!lastDuePeriod || localComparePeriod(per, lastDuePeriod) > 0) {
              lastDuePeriod = per;
            }
          }
        }

        const hasDebt = totalDueUpToNow > 0;

        payload.__debt = {
          periods,
          summary: {
            ...(base?.grandTotals || {}),
            monthsDue,
            totalBalanceDue: totalDueUpToNow,
          },
          from: base?.from || from || null,
          to: base?.to || to || null,
          nowPeriod,
          totalDueUpToNow,
          lastDuePeriod,
          monthsDue,
          hasDebt,
          balance: totalDueUpToNow,
          status: hasDebt ? "debe" : "al_dia",
        };
      } catch (e) {
        console.warn("getClienteById: error calculando deuda", e);
        // No rompemos el endpoint si falla deuda
      }
    }

    return res.json(payload);
  } catch (err) {
    next(err);
  }
}

// GET /clientes/:id/deuda  (vista ADMIN)
export async function getClientDebtAdmin(req, res, next) {
  try {
    // ───────────────────── Permisos básicos ─────────────────────
    const viewerRole = String(req.user?.role || "").trim();
    if (!["admin", "superAdmin"].includes(viewerRole)) {
      return res.status(403).json({
        ok: false,
        message: "Solo admin / superAdmin pueden ver la deuda detallada.",
      });
    }

    const id = String(req.params.id || "").trim();
    if (!isObjectId(id)) {
      return res.status(400).json({ ok: false, message: "ID inválido" });
    }

    // Traemos un solo miembro (cliente)
    const member = await Cliente.findById(id)
      .select(
        "_id idCliente nombre nombreTitular idCobrador usarCuotaIdeal cuota cuotaIdeal"
      )
      .lean();

    if (!member) {
      return res
        .status(404)
        .json({ ok: false, message: "Cliente no encontrado" });
    }

    // ───────────────────── Params de rango ─────────────────────
    const { from, to, includeFuture } = req.query || {};

    const base = await getClientPeriodState(member, {
      from,
      to,
      includeFuture: Number(includeFuture),
    });

    let periods = Array.isArray(base?.periods) ? [...base.periods] : [];
    const nowPeriod = yyyymmAR(new Date());

    const cuotaVigente =
      Number(member.usarCuotaIdeal ? member.cuotaIdeal : member.cuota) || 0;

    // Suma ya imputada al período actual
    const paidNowAgg = await Payment.aggregate([
      { $match: { "cliente.memberId": new Types.ObjectId(member._id) } },
      { $unwind: "$allocations" },
      { $match: { "allocations.period": nowPeriod } },
      {
        $group: {
          _id: null,
          sum: {
            $sum: {
              $ifNull: ["$allocations.amountApplied", "$allocations.amount"],
            },
          },
        },
      },
    ]).allowDiskUse(true);

    const alreadyAppliedNow = Number(paidNowAgg?.[0]?.sum || 0);

    // Ajustar / inyectar período actual
    const idx = periods.findIndex((p) => p?.period === nowPeriod);
    const computedBalanceNow = Math.max(0, cuotaVigente - alreadyAppliedNow);

    if (idx === -1) {
      periods.push({
        period: nowPeriod,
        charge: cuotaVigente,
        paid: alreadyAppliedNow,
        balance: computedBalanceNow,
        status: computedBalanceNow > 0 ? "due" : "paid",
      });
    } else {
      const cur = periods[idx] || {};
      const charge = Number(cur.charge ?? cuotaVigente);
      const paid = Math.max(Number(cur.paid || 0), alreadyAppliedNow);
      const balance = Math.max(0, charge - paid);
      periods[idx] = {
        ...cur,
        period: nowPeriod,
        charge,
        paid,
        balance,
        status: balance > 0 ? "due" : "paid",
      };
    }

    const totalDueUpToNow = periods
      .filter((p) => comparePeriod(p.period, nowPeriod) <= 0)
      .reduce((acc, p) => acc + Math.max(0, Number(p.balance || 0)), 0);

    return res.json({
      ok: true,
      clientId: String(member._id),
      currency: "ARS",
      from: base?.from || from || null,
      to: base?.to || to || null,
      grandTotals: base?.grandTotals || null,
      periods: periods.sort((a, b) =>
        a.period < b.period ? -1 : a.period > b.period ? 1 : 0
      ),
      summary: {
        nowPeriod,
        cuotaVigente,
        alreadyAppliedNow,
        balanceNow: computedBalanceNow,
        totalDueUpToNow,
      },
    });
  } catch (err) {
    next(err);
  }
}
// GET /clientes/collector-summary  (vista ADMIN o Cobrador)
export async function getCollectorSummaryAdmin(req, res, next) {
  try {
    const viewerRole = String(req.user?.role || "").trim();
    const myCollectorId = Number(req.user?.idCobrador);

    // idCobrador a consultar: para admin puede venir por query; para cobrador, se fuerza el suyo
    const targetIdRaw = req.query.idCobrador;
    const targetId = targetIdRaw != null ? Number(targetIdRaw) : myCollectorId;

    if (!Number.isFinite(targetId)) {
      return res.status(400).json({
        ok: false,
        message: "Falta idCobrador válido.",
      });
    }

    // Si es cobrador, sólo puede ver su propio resumen
    if (
      viewerRole === "cobrador" &&
      Number.isFinite(myCollectorId) &&
      myCollectorId !== targetId
    ) {
      return res.status(403).json({
        ok: false,
        message: "No podés ver el resumen de otro cobrador.",
      });
    }

    // ───────────────────── Fecha / período actual ─────────────────────
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth(); // 0–11
    const period = yyyymmAR(now); // "YYYY-MM"

    const monthStart = new Date(year, month, 1, 0, 0, 0, 0);
    const monthEnd = new Date(year, month + 1, 0, 23, 59, 59, 999);

    const daysInPeriod = new Date(year, month + 1, 0).getDate();
    const daysElapsed = now.getDate();
    const daysRemaining = Math.max(daysInPeriod - daysElapsed, 0);

    // Días hábiles (lun–sáb) del mes
    const countWorkingDays = () => {
      let total = 0;
      let elapsed = 0;

      for (let d = 1; d <= daysInPeriod; d++) {
        const dt = new Date(year, month, d);
        const day = dt.getDay(); // 0 = dom, 1 = lun, ..., 6 = sáb
        const isWorking = day >= 1 && day <= 6; // lun–sáb

        if (!isWorking) continue;
        total++;
        if (d <= daysElapsed) elapsed++;
      }
      const remaining = Math.max(total - elapsed, 0);
      return { total, elapsed, remaining };
    };

    const {
      total: workingDaysTotal,
      elapsed: workingDaysElapsed,
      remaining: workingDaysRemaining,
    } = countWorkingDays();

    const diffInDays = (from, to) => {
      const a = new Date(from);
      const b = new Date(to);
      if (Number.isNaN(a.getTime()) || Number.isNaN(b.getTime())) return 0;
      const ms = b.getTime() - a.getTime();
      return Math.max(0, Math.floor(ms / (1000 * 60 * 60 * 24)));
    };

    /* ───────────── Config de comisión (User del cobrador) ───────────── */

    let baseCommissionRate = 0; // decimal (0.05 = 5%)
    let graceDays = 7;
    let penaltyPerDay = 0; // caída de la tasa por día extra

    try {
      // Buscamos el User que corresponde al cobrador target
      const userDoc = await User.findOne({ idCobrador: targetId })
        .select(
          "porcentajeCobrador commissionGraceDays commissionPenaltyPerDay"
        )
        .lean();

      const rawPercent = userDoc?.porcentajeCobrador;
      if (typeof rawPercent === "number" && rawPercent > 0) {
        baseCommissionRate = rawPercent <= 1 ? rawPercent : rawPercent / 100;
      }

      if (
        userDoc &&
        userDoc.commissionGraceDays != null &&
        Number.isFinite(Number(userDoc.commissionGraceDays))
      ) {
        graceDays = Number(userDoc.commissionGraceDays);
      }

      const rawPenalty = userDoc?.commissionPenaltyPerDay;
      if (typeof rawPenalty === "number" && rawPenalty > 0) {
        penaltyPerDay = rawPenalty <= 1 ? rawPenalty : rawPenalty / 100;
      }
    } catch {
      // si falla, dejamos los defaults suaves
      baseCommissionRate = 0;
      graceDays = 7;
      penaltyPerDay = 0;
    }

    /* ───────────── Clientes asignados y cuota del mes ───────────── */

    const clientsAgg = await Cliente.aggregate([
      { $match: { idCobrador: targetId } },

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

    /* ───────────── Pagos del período + comisión pago a pago ───────────── */

    const paymentsAgg = await Payment.aggregate([
      {
        $match: {
          "collector.idCobrador": targetId,
          status: { $in: ["posted", "settled"] },
          $expr: {
            $and: [
              {
                $gte: [{ $ifNull: ["$postedAt", "$createdAt"] }, monthStart],
              },
              {
                $lte: [{ $ifNull: ["$postedAt", "$createdAt"] }, monthEnd],
              },
            ],
          },
        },
      },
      { $unwind: "$allocations" },
      {
        $match: {
          "allocations.period": period, // sólo lo imputado a este período
        },
      },
      {
        $project: {
          _id: 1,
          postedAt: { $ifNull: ["$postedAt", "$createdAt"] },
          amountApplied: "$allocations.amountApplied",
          "cliente.idCliente": 1,
        },
      },
    ]).allowDiskUse(true);

    const clientsSet = new Set();
    let totalCollectedThisPeriod = 0;
    let totalCommissionIdeal = 0;
    let totalCommissionDiscounted = 0;

    for (const p of paymentsAgg) {
      const clientId = p.cliente?.idCliente;
      if (clientId != null) clientsSet.add(clientId);

      const applied = Number(p.amountApplied) || 0;
      totalCollectedThisPeriod += applied;

      const idealRate = baseCommissionRate;
      const idealCommission = applied * idealRate;
      totalCommissionIdeal += idealCommission;

      let effectiveRate = idealRate;

      if (idealRate > 0 && penaltyPerDay > 0 && p.postedAt) {
        const daysHeld = diffInDays(p.postedAt, now);
        if (daysHeld > graceDays) {
          const extraDays = daysHeld - graceDays;
          const reduction = penaltyPerDay * extraDays;
          effectiveRate = Math.max(0, idealRate - reduction);
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

    /* ───────────── Saldo en mano (Ledger) ───────────── */

    const cashAccounts = ["CAJA_COBRADOR", "A_RENDIR_COBRADOR"];

    const balanceAgg = await LedgerEntry.aggregate([
      {
        $match: {
          "dimensions.idCobrador": targetId,
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

    /* ───────────── Comisiones globales ───────────── */

    const expectedCommission = totalChargeNow * baseCommissionRate;
    const currentCommission = totalCommissionDiscounted;

    /* ───────────── Respuesta ───────────── */

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
    const label = `${monthNamesEs[month] || "Mes"} ${year}`.replace(
      /^\w/,
      (c) => c.toUpperCase()
    );

    return res.json({
      ok: true,
      data: {
        idCobrador: targetId,
        assignedClients,
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
        balance: {
          collectorBalance,
        },
        commissions: {
          config: {
            basePercent: baseCommissionRate,
            graceDays,
            penaltyPerDay,
          },
          amounts: {
            expectedCommission,
            totalCommission: currentCommission,
            totalCommissionNoPenalty: totalCommissionIdeal,
          },
        },
      },
    });
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

    // ---- filtros directos ----
    const byIdClienteRaw = req.query.byIdCliente;
    const byIdCliente =
      byIdClienteRaw !== undefined && byIdClienteRaw !== ""
        ? Number(byIdClienteRaw)
        : undefined;
    const hasByIdCliente = Number.isFinite(byIdCliente);

    const byDocumentoRaw = (req.query.byDocumento ?? "").toString().trim();
    const hasByDocumento = byDocumentoRaw.length > 0;

    // sort
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
      "cuotaVigente", // lo mapeamos a cuota
      "updatedAt",
    ]);

    let sortBy = SORTABLE.has(sortByParam) ? sortByParam : "createdAt";
    if (sortBy === "cuotaVigente") {
      // en modo simple, usamos cuota como aproximación
      sortBy = "cuota";
    }

    const sortSpec = { [sortBy]: sortDir, _id: sortDir };

    // ==========================
    // Filtro base (simple)
    // ==========================
    const and = [];

    // Por diseño, mostramos un solo miembro por grupo → TITULAR
    and.push({ rol: "TITULAR" });

    if (hasByDocumento) {
      // simple: buscar documento por regex insensible
      const esc = byDocumentoRaw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      and.push({ documento: { $regex: esc, $options: "i" } });
    } else if (hasByIdCliente) {
      and.push({ idCliente: byIdCliente });
    } else if (qRaw) {
      const isNumeric = /^\d+$/.test(qRaw);
      const esc = qRaw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

      const or = [
        { nombre: { $regex: esc, $options: "i" } },
        { domicilio: { $regex: esc, $options: "i" } },
        { documento: { $regex: esc, $options: "i" } },
      ];

      if (isNumeric) {
        or.push({ idCliente: Number(qRaw) });
        or.push({ idCobrador: Number(qRaw) });
      }

      and.push({ $or: or });
    }

    const filter = and.length ? { $and: and } : {};

    // ==========================
    // find + sort + skip + limit
    // ==========================

    const [docs, total] = await Promise.all([
      Cliente.find(filter)
        .sort(sortSpec)
        .skip((page - 1) * limit)
        .limit(limit)
        .lean(),
      // este count es barato, NO usa sort ni aggregate gordo
      Cliente.countDocuments(filter),
    ]);

    // Si NO te interesa __debt en el listado, podés devolver docs directo
    const nowPeriod = yyyymmAR(new Date());

    const items = await Promise.all(
      docs.map(async (it) => {
        try {
          // cuotaVigente rápido
          const cuotaVigente =
            it.usarCuotaIdeal && typeof it.cuotaIdeal === "number"
              ? it.cuotaIdeal
              : it.cuota;

          // Si querés evitar E/S extra, comentá todo el bloque __debt
          const member = await Cliente.findById(it._id).lean();
          if (!member) {
            return { ...it, cuotaVigente };
          }

          const debtState = await getClientPeriodState(member, {
            to: nowPeriod,
            includeFuture: 0,
          });

          const periods = Array.isArray(debtState?.periods)
            ? debtState.periods
            : [];

          const totalDueUpToNow = periods
            .filter((p) => comparePeriod(p.period, nowPeriod) <= 0)
            .reduce((acc, p) => acc + Math.max(0, Number(p.balance || 0)), 0);

          let lastDuePeriod = null;
          for (const p of periods) {
            const per = p.period;
            const bal = Number(p.balance || 0);
            if (bal <= 0) continue;
            if (comparePeriod(per, nowPeriod) <= 0) {
              if (!lastDuePeriod || comparePeriod(per, lastDuePeriod) > 0) {
                lastDuePeriod = per;
              }
            }
          }

          const hasDebt = totalDueUpToNow > 0;

          return {
            ...it,
            cuotaVigente,
            __debt: {
              nowPeriod,
              totalDueUpToNow,
              lastDuePeriod,
              hasDebt,
              balance: totalDueUpToNow,
              status: hasDebt ? "debe" : "al_dia",
            },
          };
        } catch (e) {
          console.warn("listClientes(simple): error calculando deuda", {
            id: it._id,
            err: e,
          });
          return it;
        }
      })
    );

    return res.json({
      items,
      total, // si querés “no calcular total”, podés mandar null y listo
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

    // Guardamos y sacamos integrantes del payload del titular
    const incomingIntegrantes = Array.isArray(payload.integrantes)
      ? payload.integrantes
      : null;
    if (incomingIntegrantes) {
      delete payload.integrantes;
    }

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

    const integrantesPayloadPresent = Array.isArray(incomingIntegrantes);

    // ===== Update base (titular) =====
    const updated = await Cliente.findByIdAndUpdate(
      id,
      { $set: payload },
      { new: true, runValidators: true }
    ).lean();

    if (!updated) {
      return res
        .status(404)
        .json({ message: "Cliente no encontrado (post-update)" });
    }

    // ===== Orquestación de grupo (titular / nombre / promoción) =====
    let mustPromote = false;
    const nowInactive =
      updated?.activo === false ||
      (updated?.baja && !Number.isNaN(new Date(updated.baja).getTime()));

    if (wasTitular && nowInactive) mustPromote = true;

    if (payload.rol === "TITULAR" && !wasTitular) {
      // Si este pasa a ser titular, los otros titulares del grupo pasan a INTEGRANTE
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

    // ===== Upsert de integrantes (familiares del grupo) =====
    let integrantesTouched = false;

    if (Array.isArray(incomingIntegrantes)) {
      integrantesTouched = true;

      // Todos los miembros actuales del grupo, excepto el titular que acabamos de editar
      const existingMembers = await Cliente.find({
        idCliente: gid,
        _id: { $ne: updated._id },
      }).lean();

      const existingById = new Map(
        existingMembers.map((m) => [String(m._id), m])
      );
      const incomingIds = new Set();

      const today = new Date().toISOString().slice(0, 10);

      // Helpers pequeños de normalización
      const normNombre = (s) => (s || "").toString().trim().toUpperCase();
      const normDoc = (s) => (s || "").toString().trim();
      const toBool = (v) =>
        typeof v === "boolean"
          ? v
          : String(v).toLowerCase() === "true" || Number(v) === 1;
      const toNumOr = (v, fb) =>
        v === "" || v == null || Number.isNaN(Number(v)) ? fb : Number(v);

      // Upsert de cada integrante entrante
      for (const raw of incomingIntegrantes) {
        if (!raw) continue;

        const _idStr = raw._id ? String(raw._id) : null;
        if (_idStr) incomingIds.add(_idStr);

        const base = {
          nombre: normNombre(raw.nombre),
          documento: normDoc(raw.documento),
          docTipo: raw.docTipo || "DNI",
          fechaNac: raw.fechaNac || null,
          edad: toNumOr(raw.edad, undefined),
          sexo: raw.sexo || "X",
          cuil: (raw.cuil || "").toString().trim(),
          telefono: (raw.telefono || "").toString().trim(),
          domicilio: (raw.domicilio || "").toString().trim(),
          ciudad: (raw.ciudad || "").toString().trim(),
          provincia: (raw.provincia || "").toString().trim(),
          cp: (raw.cp || "").toString().trim(),
          observaciones: (raw.observaciones || "").toString().trim(),
          cremacion: toBool(raw.cremacion),
          parcela: toBool(raw.parcela),
          activo: raw.activo === false ? false : true,
          idCliente: gid,
          rol: "INTEGRANTE",
        };

        if (_idStr && existingById.has(_idStr)) {
          // Update integrante existente
          await Cliente.findByIdAndUpdate(
            _idStr,
            { $set: base },
            { runValidators: true }
          );
        } else {
          // Crear nuevo integrante
          await Cliente.create({
            ...base,
            integrante: 0, // luego resequenceIntegrantes lo corrige
          });
        }
      }

      // Dar de baja integrantes que ya existían pero no vienen más en el payload
      const toDeactivate = existingMembers
        .filter((m) => !incomingIds.has(String(m._id)))
        .map((m) => m._id);

      if (toDeactivate.length > 0) {
        await Cliente.updateMany(
          { _id: { $in: toDeactivate } },
          {
            $set: {
              activo: false,
              baja: today,
            },
          }
        );
      }

      // Reordenar índices de integrante y asegurar nombre de titular replicado
      await resequenceIntegrantes(gid);
      const titularDoc = await Cliente.findOne({
        idCliente: gid,
        rol: "TITULAR",
      }).lean();
      if (titularDoc) {
        await propagateTitularName(gid, (titularDoc?.nombre || "").trim());
      }
    }

    // ===== Repricing (solo ideal) =====
    const needsRepricing =
      priceAffectingChange ||
      mustPromote ||
      touchedKeys.has("usarCuotaIdeal") ||
      integrantesTouched;

    if (needsRepricing) {
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
      // === caso "OFF" + cambio de cuota => propagar histórica manual a todo el grupo activo ===
      const wasUsingIdeal = !!current.usarCuotaIdeal;
      const turnedOffIdeal =
        flagPresent && wasUsingIdeal && payload.usarCuotaIdeal === false;

      if (turnedOffIdeal && manualCuotaChange && Number.isFinite(newCuota)) {
        await setGroupHistoricalCuota(gid, newCuota, { onlyActive: true });
      } else if (manualCuotaChange && Number.isFinite(newCuota)) {
        // comportamiento opcional anterior (lo dejo comentado)
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
    const { start: periodStartUTC, end: periodEndUTC } = bounds; // (no usado pero lo dejamos por si extiendes aging V2)

    // Ventana general para "pagos recientes" y ledger snapshot por default (últimos 30 días)
    const defaultFrom = new Date(now.getTime() - 30 * 86400000);
    const df = parseISODate(dateFrom, defaultFrom);
    const dt = parseISODate(dateTo, now);

    // Filtros comunes para pagos
    const paymentMatch = {
      currency,
      status: { $in: ["posted", "settled"] },
      ...(idCobrador ? { "collector.idCobrador": Number(idCobrador) } : {}),
      ...(method ? { method } : {}),
      ...(channel ? { channel } : {}),
      createdAt: { $gte: df, $lt: dt },
    };

    // ===== 1) BASE: “debido” del período por GRUPO =====
    // Regla:
    // - Ignorar miembros con baja=true
    // - Pero NO perder el grupo si hay al menos UN miembro activo.
    // - cuotaEfectiva = usarCuotaIdeal ? cuotaIdeal : cuota, sólo para miembros activos.
    const debidoPorGrupo = await Cliente.aggregate([
      {
        $addFields: {
          isActivoPeriodo: {
            $and: [
              { $eq: ["$activo", true] },
              {
                $or: [
                  { $eq: [{ $type: "$baja" }, "missing"] },
                  { $eq: ["$baja", null] },
                  { $eq: ["$baja", false] },
                ],
              },
            ],
          },
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
          _id: "$idCliente",
          idCliente: { $first: "$idCliente" },
          nombreTitular: { $first: "$nombreTitular" },
          miembrosActivos: {
            $sum: {
              $cond: ["$isActivoPeriodo", 1, 0],
            },
          },
          debido: {
            $sum: {
              $cond: ["$isActivoPeriodo", "$cuotaEfectiva", 0],
            },
          },
          idCobrador: { $first: "$idCobrador" },
        },
      },
      {
        // si TODOS están de baja/inactivos, miembrosActivos = 0 → se filtra el grupo
        $match: {
          idCliente: { $ne: null },
          miembrosActivos: { $gt: 0 },
        },
      },
      {
        $project: {
          _id: 0,
          idCliente: 1,
          nombreTitular: 1,
          miembros: "$miembrosActivos",
          debido: 1,
          idCobrador: 1,
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

    const topPositive = [...positive]
      .sort((a, b) => b.gap - a.gap)
      .slice(0, 15);
    const topNegative = [...negative]
      .sort((a, b) => a.gap - b.gap)
      .slice(0, 15);

    // ===== 4) BY COBRADOR =====
    const byCobradorBase = new Map();
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

    const cobradorMix = new Map();
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
    byCobrador.sort(
      (a, b) => b.coverageRate - a.coverageRate || b.paid - a.paid
    );

    // ===== 5) AGING rápido =====
    function bucketizeGap(gapValue) {
      if (gapValue >= 0) return null;
      return "0-30";
    }

    const agingBuckets = { "0-30": 0, "31-60": 0, "61-90": 0, "90+": 0 };
    let gruposConDeuda = 0;
    for (const g of coverage) {
      const b = bucketizeGap(g.gap);
      if (b) {
        agingBuckets[b] += Math.abs(g.gap);
        gruposConDeuda += 1;
      }
    }

    // ===== 6) MIX general y tickets globales =====
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

    // ===== 8) USERS → nombres de cobradores =====
    const cobradoresUsers = await User.find({ idCobrador: { $ne: null } })
      .select({ name: 1, idCobrador: 1 })
      .lean();

    const cobradorNameMap = new Map();
    for (const u of cobradoresUsers) {
      if (u.idCobrador != null)
        cobradorNameMap.set(Number(u.idCobrador), u.name);
    }

    for (const row of byCobrador) {
      row.cobradorNombre = cobradorNameMap.get(Number(row.idCobrador)) || null;
    }

    // ===== 9) SUMMARY general =====
    const totalGrupos = debidoPorGrupo.length;

    const totalMiembros = await Cliente.countDocuments({
      activo: true,
      $or: [
        { baja: { $exists: false } },
        { baja: null },
        { baja: false },
      ],
    });

    const fullyPaid = coverage.filter((c) => c.status === "paid").length;
    const partially = coverage.filter((c) => c.status === "partial").length;
    const unpaid = coverage.filter((c) => c.status === "unpaid").length;

    const coverageRateGlobal =
      totalDebido > 0
        ? Number((totalPagadoPeriodo / totalDebido).toFixed(4))
        : 0;

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
          coverageRate: coverageRateGlobal,
          grupos: { paid: fullyPaid, partial: partially, unpaid },
          ticketsGlobal,
          mix,
        },
        coverage,
        byCobrador,
        aging: {
          buckets: agingBuckets,
          gruposConDeuda,
        },
        topPositive,
        topNegative,
        ledgerSnapshot,
      },
      meta: {
        generatedAt: new Date().toISOString(),
        currency,
        notes: [
          "El debido del período por grupo usa solo integrantes activos (activo=true y baja!=true).",
          "Si todos los integrantes de un grupo están dados de baja, el grupo no aparece en coverage.",
          "La cobertura del período usa Payment.allocations filtradas por allocations.period === period.",
          "El aging que ves es del período actual (0-30). Para aging multi-mes armamos V2 con saldos acumulados.",
        ],
      },
    });
  } catch (err) {
    next(err);
  }
}
