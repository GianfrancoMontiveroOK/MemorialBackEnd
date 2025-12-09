// src/controllers/collector.shared.js
import mongoose from "mongoose";

/* ============ Helpers genéricos ============ */
export const isObjectId = (v) =>
  mongoose.Types.ObjectId.isValid(String(v || ""));
export const toInt = (v, def = 0) => {
  const n = Number.parseInt(v, 10);
  return Number.isFinite(n) ? n : def;
};
export const toDir = (v) => (String(v || "").toLowerCase() === "asc" ? 1 : -1);
export const onlyDigits = (s = "") => String(s).replace(/\D+/g, "");
export const safeNumber = (v, def = 0) =>
  Number.isFinite(Number(v)) ? Number(v) : def;

/* ============ Contabilidad (ajustá a tu mapa/ERP) ============ */
/**
 * Plan de cuentas lógico de Memorial (no es el plan contable completo,
 * solo las cuentas que el sistema necesita conocer para asientos automáticos).
 *
 * OJO: los nombres (values) son los códigos que van a LedgerEntry.accountCode.
 */
export const ACCOUNTS = Object.freeze({
  // 💵 Cajas de cobradores de campo
  CAJA_COBRADOR: "CAJA_COBRADOR",

  // 💵 Cajas de oficina
  CAJA_ADMIN: "CAJA_ADMIN",
  CAJA_SUPERADMIN: "CAJA_SUPERADMIN",

  // 📈 Resultados
  INGRESOS_CUOTAS: "INGRESOS_CUOTAS",

  // 🏦 Medios de cobro / bancos
  BANCO_NACION: "BANCO_NACION",
  TARJETA_NARANJA: "TARJETA_NARANJA",
});

/**
 * Mapa rol → cuenta de caja principal.
 *
 * Esto evita tener que hardcodear strings en cada controlador.
 * Si mañana cambiás la lógica (p. ej. superAdmin usa otra caja),
 * tocás solo acá.
 */
export const CASH_ROLE_ACCOUNT = Object.freeze({
  cobrador: ACCOUNTS.CAJA_COBRADOR,
  admin: ACCOUNTS.CAJA_ADMIN,
  superAdmin: ACCOUNTS.CAJA_SUPERADMIN,
});

/**
 * Devuelve la cuenta de caja que corresponde al rol de usuario.
 * Fallback: CAJA_COBRADOR si el rol no está mapeado.
 */
export function getCashAccountForRole(role) {
  const key = String(role || "").trim();
  return CASH_ROLE_ACCOUNT[key] || ACCOUNTS.CAJA_COBRADOR;
}

/* ============ Proyección whitelisted para vista cobrador ============ */
export const projectCollector = {
  _id: 1,
  idCliente: 1,
  nombre: 1,
  nombreTitular: 1,
  domicilio: 1,
  ciudad: 1,
  provincia: 1,
  cp: 1,
  telefono: 1,
  cuota: 1,
  cuotaIdeal: 1,
  usarCuotaIdeal: 1,
  cuotaVigente: 1,
  sexo: 1,
  idCobrador: 1,
  activo: 1,
  ingreso: 1,
  vigencia: 1,
  baja: 1,
  createdAt: 1,
  updatedAt: 1,
  rol: 1,
  integrante: 1,
  integrantesCount: 1,
  cremacionesCount: 1,
  edadMax: 1,
  createdAtSafe: 1,
  updatedAtMax: 1,
};

/* ============ Períodos (YYYY-MM, TZ Mendoza) ============ */
const fmtAR = new Intl.DateTimeFormat("en-CA", {
  timeZone: "America/Argentina/Mendoza",
  year: "numeric",
  month: "2-digit",
});
export const yyyymmAR = (date = new Date()) => fmtAR.format(date); // "YYYY-MM"

export const PERIOD_RE = /^\d{4}-(0[1-9]|1[0-2])$/;
export const normalizePeriod = (s) => {
  const str = String(s || "").trim();
  return PERIOD_RE.test(str) ? str : null;
};

export const comparePeriod = (a, b) => {
  const A = normalizePeriod(a);
  const B = normalizePeriod(b);
  if (!A || !B) return 0; // si alguno es inválido, no ordenamos
  return A === B ? 0 : A < B ? -1 : 1;
};

/* ============ Utilidades allocations ============ */
export const sumAllocations = (allocs = []) =>
  allocs.reduce((acc, x) => acc + (Number(x?.amount) || 0), 0);

/**
 * Devuelve la lista de períodos vencidos (con balance > 0)
 * hasta nowPeriod (inclusive), ordenados ascendente (viejo → nuevo).
 *
 * SUPOSICIÓN: debtState tiene forma:
 * { periods: [{ period: "YYYY-MM", balance: Number }, ...] }
 */
export function getDuePeriodsUntilNow(debtState, nowPeriod) {
  const np = normalizePeriod(nowPeriod);
  if (!np) return [];

  const periods = Array.isArray(debtState?.periods) ? debtState.periods : [];

  return periods
    .filter((p) => {
      const per = normalizePeriod(p?.period);
      const bal = safeNumber(p?.balance, 0);
      return per && bal > 0 && comparePeriod(per, np) <= 0;
    })
    .map((p) => p.period)
    .sort((a, b) => (a < b ? -1 : 1)); // asc (viejo → nuevo)
}

/**
 * Cantidad de meses en atraso (períodos vencidos con balance > 0)
 * hasta nowPeriod inclusive.
 *
 * Esto NO aplica ninguna regla de negocio (3 meses, 4 meses, etc.),
 * solo devuelve el número de períodos adeudados. La regla se maneja
 * en los controladores de pago.
 */
export function countArrearsMonths(debtState, nowPeriod) {
  return getDuePeriodsUntilNow(debtState, nowPeriod).length;
}

/* ============ FIFO hasta nowPeriod (sin futuros) ============ */
export function fifoAllocateUntilNow(debtState, nowPeriod, amount) {
  const np = normalizePeriod(nowPeriod);
  let remaining = safeNumber(amount, 0);
  const out = [];
  if (!np || remaining <= 0) return { allocations: out, leftover: remaining };

  // Filtramos períodos válidos, con balance > 0 y <= nowPeriod
  const duePeriods = (
    Array.isArray(debtState?.periods) ? debtState.periods : []
  )
    .filter((p) => {
      const per = normalizePeriod(p?.period);
      const bal = safeNumber(p?.balance, 0);
      return per && bal > 0 && comparePeriod(per, np) <= 0;
    })
    .sort((a, b) => (a.period < b.period ? -1 : 1)); // asc (viejo → nuevo)

  for (const p of duePeriods) {
    if (remaining <= 0) break;
    const bal = safeNumber(p.balance, 0);
    const take = Math.min(remaining, bal);
    if (take > 0) {
      out.push({ period: p.period, amount: +take.toFixed(2) }); // redondeo suave
      remaining = +(remaining - take).toFixed(2);
    }
  }
  return { allocations: out, leftover: remaining };
}

/* ============ Serializador de Payment (+Receipt opcional) ============ */
export function serializePayment(p, receipt = null) {
  const base = {
    _id: p._id,
    kind: p.kind,
    status: p.status,
    postedAt: p.postedAt,
    settledAt: p.settledAt,
    amount: p.amount,
    currency: p.currency,
    method: p.method,
    channel: p.channel,
    notes: p.notes || "",
    idempotencyKey: p.idempotencyKey,
    externalRef: p.externalRef || null,
    reversalOf: p.reversalOf || null,
    cashSessionId: p.cashSessionId || null,
    createdAt: p.createdAt,
    updatedAt: p.updatedAt,
    cliente: p.cliente,
    collector: p.collector,
    // 👇 importante: en tu modelo usamos allocations.amount (no amountApplied)
    allocations: p.allocations || [],
    meta: p.meta || {},
  };
  return receipt
    ? {
        ...base,
        receipt: {
          _id: receipt._id,
          number: receipt.number,
          qrData: receipt.qrData,
          pdfUrl: receipt.pdfUrl,
          voided: receipt.voided,
        },
      }
    : base;
}
