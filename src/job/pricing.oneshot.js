import mongoose from "mongoose";
import Cliente from "../models/client.model.js";
import recomputeGroupPricing from "../services/pricing.services.provider..js";

const CONCURRENCY = Number(process.env.PRICING_CONCURRENCY || 8);

async function main() {
  const uri = process.env.MONGODB_URI;
  if (!uri) throw new Error("MONGODB_URI no definido");
  await mongoose.connect(uri);
  console.log("✅ MongoDB conectado (oneshot)");

  const groups = await Cliente.distinct("idCliente", { idCliente: { $ne: null } });
  const ids = [...new Set(groups.map((g) => String(g).trim()))];

  console.log(`[ONESHOT] grupos a recalcular: ${ids.length}`);

  let idx = 0, ok = 0, fail = 0;
  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= ids.length) break;
      const id = ids[i];
      try {
        const r = await recomputeGroupPricing(id);
        if (r?.ok) ok++; else fail++; 
      } catch (e) {
        fail++;
        console.error("[ONESHOT] error en grupo", id, e?.message || e);
      }
      if ((i + 1) % 500 === 0 || i + 1 === ids.length) {
        console.log(`[ONESHOT] avance ${i + 1}/${ids.length} (ok=${ok}, fail=${fail})`);
      }
    }
  } 

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  console.log(`[ONESHOT] FIN (ok=${ok}, fail=${fail})`);
  await mongoose.disconnect();
}

main().catch((e) => {
  console.error("[ONESHOT] fallo", e);
  process.exit(1);
});

// 👇 ELIMINADO: NO EXPORTAR NADA EN UN SCRIPT
// export default schedulePricingRecomputeAll;
