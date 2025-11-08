// /src/services/allocation.service.js
import { getClientPeriodState } from "./debt.service.js";
import { toYYYYMM, nextPeriod } from "./periods.util.js";

function ensurePositive(n) {
  const x = Number(n);
  if (!Number.isFinite(x) || x <= 0) throw new Error("Importe inválido");
  return x;
}

export async function buildAllocationsAuto(
  clienteDoc,
  total,
  { includeFuture = 1 } = {}
) {
  const payAmount = ensurePositive(total);
  const todayP = toYYYYMM(new Date());

  // Estado de períodos: hasta hoy + futuros
  const { periods, meta } = await getClientPeriodState(clienteDoc, {
    includeFuture: Math.max(1, includeFuture),
  });

  let remaining = payAmount;
  const allocations = [];

  // 1) Cubrir due/partial en orden cronológico
  for (const p of periods) {
    if (remaining <= 0) break;
    if (p.status !== "due" && p.status !== "partial") continue;

    const apply = Math.min(remaining, p.balance);
    if (apply <= 0) continue;

    remaining -= apply;
    const statusAfter = p.balance - apply === 0 ? "paid" : "partial";
    allocations.push({
      period: p.period,
      amountApplied: apply,
      statusAfter,
      memberId: clienteDoc._id,
    });
  }

  // 2) Si sobra, aplicar a período(s) futuros comenzando por el inmediato
  if (remaining > 0) {
    // Tomamos el primer futuro: nextPeriod(hoy) (o último futuro de periods si ya viene calculado)
    const lastPeriod = periods[periods.length - 1]?.period || todayP;
    const firstFuture = nextPeriod(toYYYYMM(new Date())); // futuro inmediato desde hoy
    const targetFuture =
      lastPeriod > firstFuture ? nextPeriod(lastPeriod) : firstFuture;

    allocations.push({
      period: targetFuture,
      amountApplied: remaining,
      statusAfter: "partial", // quedará “paid” si luego el due del futuro = cuota y alcanza
      memberId: clienteDoc._id,
    });
  }

  const periodsApplied = allocations.map((a) => a.period);
  return {
    allocations,
    periodsApplied,
    quotaBase: meta?.quotaBase,
    quotaValueUsed: meta?.quotaValueUsed,
  };
}

/**
 * breakdown: [{ period:"YYYY-MM", amount:Number }]
 * Regla: suma <= total; amounts > 0; períodos únicos
 */
export function applyManualBreakdown(clienteDoc, total, breakdown = []) {
  const payAmount = ensurePositive(total);
  if (!Array.isArray(breakdown) || breakdown.length === 0) {
    throw new Error("Breakdown manual vacío");
  }

  const seen = new Set();
  let sum = 0;
  const allocations = [];

  for (const row of breakdown) {
    const period = String(row?.period || "").trim();
    const amount = ensurePositive(row?.amount);

    if (!/^\d{4}-\d{2}$/.test(period))
      throw new Error(`Periodo inválido: ${period}`);
    if (seen.has(period))
      throw new Error(`Periodo duplicado en breakdown: ${period}`);
    seen.add(period);

    sum += amount;
    allocations.push({
      period,
      amountApplied: amount,
      statusAfter: "partial", // se recalcula visualmente con debt.service, pero persistimos partial por seguridad conservadora
      memberId: clienteDoc._id,
    });
  }

  if (sum > payAmount) throw new Error("La suma del breakdown supera el total");

  const periodsApplied = allocations.map((a) => a.period);
  return { allocations, periodsApplied };
}
