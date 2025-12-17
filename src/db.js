// src/db.js  (ESM)
import mongoose from "mongoose";

// Servicios de pricing
import {
  recomputeAllGroups,        // orquestador (todos)
} from "./controllers/admin.reprice.controller.js";

import { getGlobalPriceRules } from "./services/priceRules.provider.js";

// Jobs unificados (cron + fixes)
import {
  fixAgesAndReprice,         // normaliza fechaNac→edad y re-precia
  fixZeroPricing,            // re-precia sólo grupos con cuotaIdeal 0/null
  // scheduleDailyPricingRecompute  // ⬅ lo dejamos para app.js para evitar doble cron
} from "./job/pricing.jobs.js";

/** Corre mantenimientos según flags .env al iniciar la DB */
async function runPricingMaintenance() {
  const MODE = (process.env.PRICING_MODE || "").toLowerCase(); // "prueba" → fuerza tareas
  const CONC = Number(process.env.PRICING_CONCURRENCY || 8);

  // Aseguramos que las reglas están accesibles (y cacheadas)
  await getGlobalPriceRules().catch(() => null);

  // 1) Recalcular EDADES y re-preciar grupos afectados (o todos si 'prueba'/'AGE_FORCE_REPRICE_ALL')
  if (process.env.AGE_FIX_AND_REPRICE === "1" || MODE === "prueba") {
    await fixAgesAndReprice({
      concurrency: CONC,
      logEvery: 200,
    });
    console.log("✅ [AGE-FIX] OK");
  }

  // 2) Recalcular PRECIO IDEAL para TODOS los grupos (si lo pedís por .env o 'prueba')
  if (process.env.REPRICE_ALL === "1" || MODE === "prueba") {
    await recomputeAllGroups({
      concurrency: CONC,
      debug: false,
    });
    console.log("✅ [REPRICE-ALL] OK");
  }

  // 3) Fix puntual: sólo donde cuotaIdeal = 0/null (evitamos si ya está en modo 'prueba')
  if (process.env.PRICING_FIX_ZERO === "1" && MODE !== "prueba") {
    await fixZeroPricing({
      concurrency: CONC,
      logEvery: 200,
    });
    console.log("✅ [PRICING-FIX-ZERO] OK");
  }
}

export const connectDB = async () => {
  try {
    await mongoose.connect(process.env.MONGODB_URI);
    console.log("✅ MongoDB conectado");

    // —— Mantenimiento de pricing según .env (incluye modo 'prueba')
    await runPricingMaintenance();

    // —— Cron de pricing
    // Lo manejás en app.js para evitar doble scheduling.
    // Si preferís moverlo acá, descomentá lo siguiente y eliminá el de app.js:
    //
    // if (process.env.ENABLE_PRICING_CRON === "1" && !global.__pricingCronStarted) {
    //   scheduleDailyPricingRecompute();
    //   global.__pricingCronStarted = true; // evita doble arranque en ESM
    //   console.log("⏰ Cron de pricing programado");
    // } else {
    //   console.log("⏸️ Cron de pricing deshabilitado por .env o ya iniciado");
    // }

  } catch (error) {
    console.error(error);
    process.exit(1);
  }
};
