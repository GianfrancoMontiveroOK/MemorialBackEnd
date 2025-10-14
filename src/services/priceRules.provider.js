// src/services/priceRules.provider.js  (ESM)

let _cache = null;
let _cacheAt = 0;
const TTL_MS = 60_000;

/** Reglas por defecto (coinciden con tu lógica vigente) */
export const DEFAULT_PRICE_RULES = {
  base: 16000,
  cremationCoef: 0.125,
  group: {
    neutralAt: 4, // n desde el cual el factor es 1.0 y sube por step
    step: 0.25, // incremento por integrante adicional > neutralAt
    minMap: { 1: 0.5, 2: 0.75, 3: 1.0 }, // overrides para n pequeños
  },
  age: [
    { min: 66, coef: 1.375 },
    { min: 61, coef: 1.25 },
    { min: 51, coef: 1.125 },
    // implícito: menor a 51 => 1.0
  ],
};

function isFresh() {
  return Date.now() - _cacheAt < TTL_MS;
}

/* --------------------------------- utils ---------------------------------- */

const toNum = (v, def) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
};

const asPlainObject = (v, def = {}) =>
  v && typeof v === "object" && !Array.isArray(v) ? v : def;

function normalizePriceRules(raw) {
  const base = toNum(raw?.base, DEFAULT_PRICE_RULES.base);
  const cremationCoef = toNum(
    raw?.cremationCoef,
    DEFAULT_PRICE_RULES.cremationCoef
  );

  const gIn = asPlainObject(raw?.group);
  const group = {
    neutralAt: Math.max(
      1,
      toNum(gIn.neutralAt, DEFAULT_PRICE_RULES.group.neutralAt)
    ),
    step: toNum(gIn.step, DEFAULT_PRICE_RULES.group.step),
    minMap: asPlainObject(gIn.minMap, DEFAULT_PRICE_RULES.group.minMap),
  };

  // Normalizar claves de minMap a números enteros >=1 y valores numéricos
  const mm = {};
  for (const [k, v] of Object.entries(group.minMap)) {
    const nk = Math.max(1, Math.floor(Number(k)));
    const nv = toNum(v, undefined);
    if (Number.isFinite(nk) && Number.isFinite(nv)) {
      mm[nk] = nv;
    }
  }
  // Asegurar los tres básicos por defecto
  for (const [k, v] of Object.entries(DEFAULT_PRICE_RULES.group.minMap)) {
    if (mm[k] == null) mm[k] = v;
  }
  group.minMap = mm;

  // Normalizar tramos de edad
  let age = Array.isArray(raw?.age) ? raw.age : DEFAULT_PRICE_RULES.age;
  age = age
    .map((t) => ({
      min: Math.floor(toNum(t?.min, NaN)),
      coef: toNum(t?.coef, NaN),
    }))
    .filter((t) => Number.isFinite(t.min) && Number.isFinite(t.coef))
    .sort((a, b) => b.min - a.min); // orden descendente por min

  if (age.length === 0) age = DEFAULT_PRICE_RULES.age;

  return { base, cremationCoef, group, age };
}

function validatePriceRules(rules) {
  const errors = [];

  if (!Number.isFinite(rules.base) || rules.base <= 0) {
    errors.push("base debe ser un número > 0");
  }
  if (!Number.isFinite(rules.cremationCoef) || rules.cremationCoef < 0) {
    errors.push("cremationCoef debe ser un número ≥ 0");
  }
  if (!Number.isFinite(rules.group?.neutralAt) || rules.group.neutralAt < 1) {
    errors.push("group.neutralAt debe ser ≥ 1");
  }
  if (!Number.isFinite(rules.group?.step) || rules.group.step < 0) {
    errors.push("group.step debe ser ≥ 0");
  }
  if (!rules.group?.minMap || typeof rules.group.minMap !== "object") {
    errors.push("group.minMap inválido");
  }
  if (!Array.isArray(rules.age) || rules.age.length === 0) {
    errors.push("age debe ser una lista no vacía");
  } else {
    for (const t of rules.age) {
      if (!Number.isFinite(t.min) || !Number.isFinite(t.coef)) {
        errors.push("age contiene items inválidos (min/coef)");
        break;
      }
    }
  }

  return { ok: errors.length === 0, errors };
}

function stamp(obj) {
  return { ...obj, _appliedAt: new Date().toISOString() };
}

/* -------------------------------- providers -------------------------------- */

/**
 * Lee las reglas globales desde cache/env/DB y devuelve una versión normalizada.
 * Si `force=true`, ignora el cache.
 */
export async function getGlobalPriceRules({ force = false } = {}) {
  if (!force && _cache && isFresh()) return _cache;

  // Partimos del DEFAULT y aplicamos overrides de ENV si existen
  const envBase = Number(process.env.PRICING_BASE || NaN);
  const envCrem = Number(process.env.PRICING_CREMATION_COEF || NaN);
  const rules = JSON.parse(JSON.stringify(DEFAULT_PRICE_RULES));
  if (!Number.isNaN(envBase)) rules.base = envBase;
  if (!Number.isNaN(envCrem)) rules.cremationCoef = envCrem;

  try {
    // Carga perezosa del modelo para evitar ciclos
    const mod = await import("../models/settings.model.js");
    const GlobalSettings = mod.default;

    const doc = await GlobalSettings.findOne({ singleton: "GLOBAL" })
      .select({ priceRules: 1 })
      .lean();

    if (doc?.priceRules) {
      // merge + normalize
      const merged = normalizePriceRules({
        ...rules,
        ...asPlainObject(doc.priceRules),
      });
      const { ok } = validatePriceRules(merged);
      _cache = ok ? stamp(merged) : stamp(normalizePriceRules(rules));
    } else {
      _cache = stamp(normalizePriceRules(rules));
    }
  } catch {
    // si falla el modelo, seguimos con defaults/env
    _cache = stamp(normalizePriceRules(rules));
  }

  _cacheAt = Date.now();
  return _cache;
}

/**
 * Persiste reglas nuevas en la colección `settings`, doc {singleton:"GLOBAL"}.
 * Invalida el cache y devuelve las reglas efectivas (normalizadas).
 *
 * Acepta payload como:
 *  - { priceRules: {...} }
 *  - {...}  (directo)
 */
export async function updateGlobalPriceRules(payload = {}) {
  const incoming = asPlainObject(payload?.priceRules ?? payload);
  const normalized = normalizePriceRules(incoming);
  const { ok, errors } = validatePriceRules(normalized);
  if (!ok) {
    const err = new Error("Price rules inválidas");
    err.details = errors;
    err.status = 400;
    throw err;
  }

  const mod = await import("../models/settings.model.js");
  const GlobalSettings = mod.default;

  const update = {
    $set: {
      singleton: "GLOBAL",
      priceRules: normalized,
      updatedAt: new Date(),
    },
    $setOnInsert: { createdAt: new Date() },
  };

  await GlobalSettings.updateOne({ singleton: "GLOBAL" }, update, {
    upsert: true,
  });

  // invalidar cache y devolver reglas frescas
  _cache = null;
  _cacheAt = 0;
  return await getGlobalPriceRules({ force: true });
}

/** Limpia el caché manualmente (útil en tests o tareas batch). */
export function clearPriceRulesCache() {
  _cache = null;
  _cacheAt = 0;
}

/** Exporte por defecto opcional */
export default {
  DEFAULT_PRICE_RULES,
  getGlobalPriceRules,
  updateGlobalPriceRules,
  clearPriceRulesCache,
};
