// src/db.js  (ESM)

import mongoose from "mongoose";
import scheduleDailyPricingRecompute from "./job/pricing.cron.js";

// ⚙️ Servicio centralizado (nuevo refactor)
import {
  recomputeGroupPricing, // núcleo por grupo
  recomputeAllGroups, // orquestador (todos)
  recomputeGroupsByIds, // orquestador (ids específicos)
  fixAgesAndMaybeReprice, // normaliza fechaNac→edad y re-precia
} from "./services/pricing.services.js";

import { getGlobalPriceRules } from "./services/priceRules.provider.js";
import Cliente from "./models/client.model.js";

/** Recalcula cuotaIdeal para TODOS los grupos (con barra de progreso) */
async function repriceAllGroups({
  concurrency = Number(process.env.PRICING_CONCURRENCY || 8),
  base, // override opcional
} = {}) {
  // Cargamos reglas dinámicas una sola vez
  const rules = await getGlobalPriceRules();
  const BASE = Number.isFinite(base)
    ? Number(base)
    : Number(process.env.PRICING_BASE || rules.base || 16000);

  console.log(
    `[REPRICE-ALL] Recalculando cuotaIdeal de TODOS los grupos (base=${BASE})…`
  );

  const ids = await Cliente.distinct("idCliente", { idCliente: { $ne: null } });
  const uniq = [...new Set(ids.map((x) => String(x).trim()).filter(Boolean))];
  console.log(`[REPRICE-ALL] Grupos detectados: ${uniq.length}`);

  let cursor = 0;
  let procesados = 0;
  let errores = 0;
  let modifiedTotal = 0;
  let matchedTotal = 0;
  const t0 = Date.now();

  const isTTY = process.stdout && process.stdout.isTTY;
  const barWidth = 30;
  const logEvery = 200;
  const fmt = (n) => n.toLocaleString("es-AR");
  const render = (final = false) => {
    const total = uniq.length;
    const pct = total ? procesados / total : 1;
    const pctText = (pct * 100).toFixed(1) + "%";
    const elapsed = (Date.now() - t0) / 1000;
    const rate = procesados > 0 ? procesados / elapsed : 0;
    const remain = Math.max(0, total - procesados);
    const etaSec = rate > 0 ? remain / rate : 0;
    const eta =
      etaSec > 3600
        ? `${Math.floor(etaSec / 3600)}h ${Math.floor((etaSec % 3600) / 60)}m`
        : etaSec > 60
        ? `${Math.floor(etaSec / 60)}m ${Math.floor(etaSec % 60)}s`
        : `${Math.floor(etaSec)}s`;

    if (isTTY) {
      const filled = Math.round(barWidth * pct);
      const bar =
        "█".repeat(filled) + "░".repeat(Math.max(0, barWidth - filled));
      const line = ` ${bar} ${pctText} | ${fmt(procesados)}/${fmt(
        total
      )} · mod=${fmt(modifiedTotal)} · err=${fmt(errores)} · ETA ${eta} `;
      process.stdout.clearLine(0);
      process.stdout.cursorTo(0);
      process.stdout.write(line);
      if (final) process.stdout.write("\n");
    } else if (final || procesados % logEvery === 0) {
      console.log(
        `[REPRICE-ALL] Progreso: ${procesados}/${
          uniq.length
        } (${pctText}) · modified=${fmt(modifiedTotal)} · errores=${fmt(
          errores
        )} · ETA ${eta}`
      );
    }
  };

  let intervalId = null;
  if (isTTY) intervalId = setInterval(() => render(false), 300);

  async function worker() {
    const total = uniq.length;
    while (true) {
      const idx = cursor++;
      if (idx >= total) break;
      const id = uniq[idx];
      try {
        // El servicio usa reglas dinámicas; pasamos BASE como override opcional
        const r = await recomputeGroupPricing(id, { base: BASE, debug: false });
        matchedTotal += r?.matched ?? 0;
        modifiedTotal += r?.modified ?? 0;
      } catch (e) {
        errores++;
        console.error(`[REPRICE-ALL] ERROR grupo ${id}:`, e?.message || e);
      } finally {
        procesados++;
        if (!isTTY && procesados % logEvery === 0) render(false);
      }
    }
  }

  await Promise.all(
    Array.from({ length: Math.max(1, concurrency) }, () => worker())
  );

  if (intervalId) clearInterval(intervalId);
  render(true);

  const secs = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(
    `[REPRICE-ALL] Terminado en ${secs}s · Procesados=${fmt(
      procesados
    )}, Matched=${fmt(matchedTotal)}, Modified=${fmt(
      modifiedTotal
    )}, Errores=${fmt(errores)}`
  );
}

/** Corre mantenimientos según flags .env */
async function runPricingMaintenance() {
  const MODE = (process.env.PRICING_MODE || "").toLowerCase(); // "prueba" fuerza todo
  const CONC = Number(process.env.PRICING_CONCURRENCY || 8);

  // Siempre intentamos leer reglas dinámicas primero
  const rules = await getGlobalPriceRules();
  const BASE = Number(process.env.PRICING_BASE || rules.base || 16000);

  // 1) Recalcular EDADES y re-preciar grupos afectados (servicio unificado)
  if (process.env.AGE_FIX_AND_REPRICE === "1" || MODE === "prueba") {
    await fixAgesAndMaybeReprice({
      forceAll: MODE === "prueba" || process.env.AGE_FORCE_REPRICE_ALL === "1",
      base: BASE,
      concurrency: CONC,
      logEvery: 200,
      debug: false,
    });
    console.log("[AGE-FIX] OK");
  }

  // 2) Recalcular PRECIO IDEAL para TODOS los grupos
  if (process.env.REPRICE_ALL === "1" || MODE === "prueba") {
    // Puedes usar el orquestador del servicio directamente:
    // await recomputeAllGroups({ base: BASE, concurrency: CONC, debug: false });
    // …o si preferís mantener la barra personalizada:
    await repriceAllGroups({ concurrency: CONC, base: BASE });
  }

  // 3) Fix puntual: solo donde cuotaIdeal=0/null
  if (process.env.PRICING_FIX_ZERO === "1" && MODE !== "prueba") {
    const { default: fixZeroPricing } = await import("./job/pricing.fix.js"); // wrapper delgado
    await fixZeroPricing({ concurrency: CONC, base: BASE });
  }
}

export const connectDB = async () => {
  try {
    await mongoose.connect(process.env.MONGODB_URI);
    console.log("✅ MongoDB conectado");

    // —— Mantenimiento según .env (incluye modo 'prueba')
    await runPricingMaintenance();

    // —— Cron (habilitable por .env)
    if (process.env.ENABLE_PRICING_CRON !== "0") {
      if (!global.__pricingCronStarted) {
        scheduleDailyPricingRecompute();
        global.__pricingCronStarted = true; // evita doble arranque en ESM
        console.log("⏰ Cron de pricing programado");
      }
    } else {
      console.log("⏸️ Cron de pricing deshabilitado por .env");
    }
  } catch (error) {
    console.error(error);
    process.exit(1);
  }
};
