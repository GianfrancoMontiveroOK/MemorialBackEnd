import { getGlobalPriceRules } from "./priceRules.provider.js";

/* Redondeo a $500 con corte $250 */
export function round500(x) {
  const n = Number(x) || 0;
  const base = Math.floor(n / 500) * 500;
  return n - base >= 250 ? base + 500 : base;
}

export function membersFactor(n, groupCfg) {
  const m = Math.max(1, Number(n) || 1);
  const { neutralAt = 4, step = 0.25, minMap = {} } = groupCfg || {};
  if (minMap[m] != null) return Number(minMap[m]);
  if (m <= neutralAt) {
    if (m === 1) return 0.5;
    if (m === 2) return 0.75;
    return 1.0; // 3 y 4 => 1.0
  }
  return 1 + step * (m - neutralAt);
}

export function ageCoef(edadMax, tiers) {
  const e = Number(edadMax) || 0;
  for (const t of tiers || [])
    if (e >= (t?.min ?? 0)) return Number(t?.coef || 1);
  return 1.0;
}
  
/** Calcula cuota ideal con reglas dinámicas (lee modelo o usa defaults/env). */
export async function computeCuotaIdealAsync({
  base,
  integrantes,
  edadMax,
  cremCount,
}) {
  const rules = await getGlobalPriceRules();
  const B = Number(base ?? rules.base);
  const g = membersFactor(integrantes, rules.group);
  const a = ageCoef(edadMax, rules.age);
  const crem =
    B * Number(rules.cremationCoef) * Math.max(0, Number(cremCount) || 0);
  return round500(B * g * a + crem);
}

/** Variante síncrona recibiendo `rules` ya cargadas (para co ntroladores). */
export function computeCuotaIdealWith(
  { base, integrantes, edadMax, cremCount },
  rules
) {
  const B = Number(base ?? rules.base);
  const g = membersFactor(integrantes, rules.group);
  const a = ageCoef(edadMax, rules.age);
  const crem =
    B * Number(rules.cremationCoef) * Math.max(0, Number(cremCount) || 0);
  return round500(B * g * a + crem);
}
