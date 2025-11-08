// /src/services/debt.service.js
import Payment from "../models/payment.model.js";
import { toYYYYMM, rangePeriods, nextPeriod } from "./periods.util.js";

/* ================== Parámetros de facturación (MVP) ================== */
const GO_LIVE_PERIOD = "2025-10"; // primer período facturable del sistema
const DUE_DAY = 10; // antes del día 10, el mes corriente está "open" (no due)

/* ================== Helpers ================== */
function getQuotaFor(clienteDoc) {
  const q = clienteDoc?.usarCuotaIdeal
    ? clienteDoc?.cuotaIdeal
    : clienteDoc?.cuota;
  const val = Number(q || 0);
  return Number.isFinite(val) && val > 0 ? Math.round(val) : 0;
}
const maxPeriod = (a, b) => (!a ? b : !b ? a : a > b ? a : b);
const minPeriod = (a, b) => (!a ? b : !b ? a : a < b ? a : b);

function clampByRange(periods, { from, to }) {
  return periods.filter((p) => {
    if (from && p.period < from) return false;
    if (to && p.period > to) return false;
    return true;
  });
}

/**
 * getClientPeriodState(clienteDoc, opts)
 * opts:
 *  - from: "YYYY-MM" (no menor a billableFrom)
 *  - to: "YYYY-MM"   (default: período actual)
 *  - includeFuture: number (default: 1)
 *
 * return:
 *  {
 *    periods: [{ period, charge, paid, balance, status }],
 *    summary: { monthsDue, totalBalanceDue, hasCredit, creditAmount, isUpToDate },
 *    meta: { generatedAt, quotaBase, quotaValueUsed, start, end, goLive, dueDay }
 *  }
 */
export async function getClientPeriodState(clienteDoc, opts = {}) {
  const now = new Date();
  const todayP = toYYYYMM(now);
  const quota = getQuotaFor(clienteDoc);

  const includeFuture = Number.isFinite(opts.includeFuture)
    ? Math.max(0, opts.includeFuture)
    : 1;

  // Alta del cliente -> usar vigencia si existe; si no, createdAt; fallback today
  const onboardingP = clienteDoc?.vigencia
    ? toYYYYMM(new Date(clienteDoc.vigencia))
    : clienteDoc?.createdAt
    ? toYYYYMM(new Date(clienteDoc.createdAt))
    : todayP;

  // 1) Primer período facturable
  const billableFrom = maxPeriod(onboardingP, GO_LIVE_PERIOD);

  // 2) Ventana solicitada (clamp)
  const requestedFrom = opts.from || billableFrom;
  const baseFrom = maxPeriod(requestedFrom, billableFrom);
  const baseTo = opts.to ? minPeriod(opts.to, todayP) : todayP;

  // 3) Períodos base [baseFrom..baseTo]
  const basePeriods = baseFrom <= baseTo ? rangePeriods(baseFrom, baseTo) : [];

  // 4) Futuros
  const futurePeriods = [];
  let p = nextPeriod(baseTo);
  for (let i = 0; i < includeFuture; i++) {
    futurePeriods.push(p);
    p = nextPeriod(p);
  }
  const allPeriods = [...basePeriods, ...futurePeriods];

  // 5) Pagos por período del miembro (solo posted/settled)
  const payments = await Payment.find(
    {
      "cliente.memberId": clienteDoc._id,
      "allocations.0": { $exists: true },
      status: { $in: ["posted", "settled"] },
    },
    { allocations: 1 }
  ).lean();

  const paidByPeriod = new Map(); // period -> sum(amountApplied)
  for (const pay of payments) {
    for (const a of pay.allocations || []) {
      if (!a?.period || typeof a.amountApplied !== "number") continue;
      paidByPeriod.set(
        a.period,
        (paidByPeriod.get(a.period) || 0) + a.amountApplied
      );
    }
  }

  // 6) Clasificación período a período  ->  { period, charge, paid, balance, status }
  const periods = allPeriods.map((period) => {
    const isFuture = period > baseTo;

    const charge = quota; // monto de cuota por período
    const paid = paidByPeriod.get(period) || 0; // aplicado a ese período
    let status = "due";
    let balance = charge - paid; // puede ser < 0 (crédito)

    if (isFuture) {
      // Futuros no son deuda. Si hubo pagos a cuenta, marcamos credit
      if (paid > 0) {
        status = "credit";
        // balance ya refleja crédito si es negativo; dejamos así
        return { period, charge, paid, balance, status };
      }
      // futuro sin pagos: neutral
      return { period, charge, paid: 0, balance: 0, status: "future" };
    }

    // No futuro:
    if (charge === 0 && paid === 0) {
      status = "paid";
      balance = 0;
    } else if (paid <= 0 && charge > 0) {
      status = "due";
      balance = charge;
    } else if (paid > 0 && paid < charge) {
      status = "partial";
      balance = charge - paid;
    } else if (paid >= charge) {
      status = "paid";
      balance = 0;
    }

    return { period, charge, paid, balance, status };
  });

  // 7) Regla de vencimiento: mes corriente antes del DUE_DAY => "open" (no due)
  const day = now.getDate();
  if (day < DUE_DAY) {
    const idx = periods.findIndex((x) => x.period === todayP);
    if (idx >= 0) {
      const row = periods[idx];
      if (row.status === "due" || row.status === "partial") {
        periods[idx] = { ...row, status: "open" };
      }
    }
  }

  // 8) Summary (deuda real: due | partial; excluir open/future/credit/paid)
  const debtRows = periods.filter(
    (r) => r.status === "due" || r.status === "partial"
  );
  const monthsDue = debtRows.length;
  const totalBalanceDue = debtRows.reduce(
    (acc, r) => acc + Math.max(0, r.balance || 0),
    0
  );

  // Crédito global (solo períodos facturables)
  const facturableRows = periods.filter((r) => r.period <= baseTo);
  const sumDue = facturableRows.reduce((acc, r) => acc + (r.charge || 0), 0);
  const sumPaid = facturableRows.reduce((acc, r) => acc + (r.paid || 0), 0);
  const creditAmount = Math.max(0, sumPaid - sumDue);
  const hasCredit = creditAmount > 0;

  const isUpToDate = monthsDue === 0;

  return {
    periods,
    summary: {
      monthsDue,
      totalBalanceDue,
      hasCredit,
      creditAmount,
      isUpToDate,
    },
    meta: {
      generatedAt: new Date().toISOString(),
      quotaBase: clienteDoc?.usarCuotaIdeal ? "cuotaIdeal" : "cuota",
      quotaValueUsed: quota,
      start: baseFrom,
      end: baseTo,
      goLive: GO_LIVE_PERIOD,
      dueDay: DUE_DAY,
    },
  };
}
