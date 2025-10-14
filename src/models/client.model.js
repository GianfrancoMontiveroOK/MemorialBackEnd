import mongoose from "mongoose";
import recomputeGroupPricing from "../services/pricing.services.js"; // ⬅ usa tu servicio ESM

/* ===== Helpers ===== */
const toBool = (v) => {
  if (typeof v === "boolean") return v;
  if (v === 1 || v === "1" || String(v).toLowerCase() === "true") return true;
  if (v === 0 || v === "0" || String(v).toLowerCase() === "false") return false;
  return Boolean(v);
};
const toNumOrNull = (v) => {
  if (v === "" || v == null) return null;
  const n = Number(String(v).replace(/[^0-9.-]/g, ""));
  return Number.isFinite(n) ? n : null;
};
const toDateOrNull = (v) => {
  if (!v) return null;
  if (
    typeof v === "string" &&
    v.trim().replace(/-/g, "").replace(/\s/g, "") === ""
  )
    return null;
  const s = String(v).trim();
  if (/^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(s)) {
    const [m, d, y] = s.split("/").map((x) => parseInt(x, 10));
    const yyyy = y < 100 ? 1900 + y : y;
    const dt = new Date(yyyy, m - 1, d);
    return Number.isNaN(dt.getTime()) ? null : dt;
  }
  const dt = new Date(s);
  return Number.isNaN(dt.getTime()) ? null : dt;
};
const cleanString = (v) => (v == null ? v : String(v).trim());

/* ===== Esquema ===== */
const ClienteSchema = new mongoose.Schema(
  {
    _id: { type: mongoose.Schema.Types.ObjectId, auto: true },

    // Identificador de agrupación (grupo familiar)
    idCliente: {
      type: Number,
      required: true,
      index: true,
      set: (v) => {
        const n = toNumOrNull(v);
        return n == null ? undefined : n;
      },
    },

    nombre: {
      type: String,
      required: true,
      trim: true,
      uppercase: true,
      set: cleanString,
    },
    domicilio: { type: String, trim: true, set: cleanString },
    ciudad: { type: String, trim: true, set: cleanString },
    provincia: { type: String, trim: true, set: cleanString },
    cp: {
      type: String,
      trim: true,
      set: (v) => (v === 0 ? "" : cleanString(v)),
    },

    telefono: {
      type: String,
      trim: true,
      set: (v) => (v === 0 ? "" : cleanString(v)),
    },
    documento: { type: String, trim: true, set: cleanString },
    docTipo: {
      type: String,
      enum: ["DNI", "CUIT", "PASAPORTE", "OTRO"],
      default: "DNI",
    },

    fechaNac: { type: Date, set: toDateOrNull },
    edad: {
      type: Number,
      min: 0,
      max: 120,
      set: (v) => toNumOrNull(v) ?? undefined,
    },

    idCobrador: {
      type: Number,
      index: true,
      set: (v) => toNumOrNull(v) ?? undefined,
    },

    rol: {
      type: String,
      enum: ["TITULAR", "INTEGRANTE", "OTRO"],
      default: "TITULAR",
    },
    integrante: { type: Boolean, default: false, set: toBool },
    nombreTitular: { type: String, trim: true, set: cleanString },

    // 💾 Histórico que cobrás
    cuota: { type: Number, default: 0, set: (v) => toNumOrNull(v) ?? 0 },

    // 🧮 Dinámica (se recalcula por reglas y se persiste)
    cuotaIdeal: { type: Number, default: 0, set: (v) => toNumOrNull(v) ?? 0 },

    // ===== NUEVO: flags de producto =====
    cremacion: { type: Boolean, default: false, set: toBool }, // ⬅ reemplaza al “plan … cremación”
    parcela: { type: Boolean, default: false, set: toBool }, // ya existía, confirmamos boolean

    observaciones: { type: String, trim: true, set: cleanString },

    cuotaAnterior: { type: Number, set: (v) => toNumOrNull(v) ?? undefined },
    cuotaNueva: { type: Number, set: (v) => toNumOrNull(v) ?? undefined },

    emergencia: { type: Boolean, default: false, set: toBool },

    tipoFactura: {
      type: String,
      enum: ["A", "B", "C", "none"],
      default: "none",
    },
    factura: { type: Boolean, default: false, set: toBool },

    tarjeta: { type: Boolean, default: false, set: toBool },

    sexo: { type: String, enum: ["M", "F", "X"], default: "X" },
    cuil: { type: String, trim: true, set: cleanString },

    fechaAumento: { type: Date, set: toDateOrNull },
    vigencia: { type: Date, set: toDateOrNull },
    baja: { type: Date, set: toDateOrNull },
    ingreso: { type: Date, set: toDateOrNull },

    activo: { type: Boolean, default: true, set: toBool },
  },
  { timestamps: true, versionKey: false }
);

/* ===== Hooks de grupo: recalcular cuotaIdeal del grupo tras cambios ===== */
async function _getIdClienteFromOp(docOrQuery) {
  try {
    if (docOrQuery?.idCliente != null) return Number(docOrQuery.idCliente);
    const filter = docOrQuery?.getFilter?.() || docOrQuery?._conditions || {};
    if (filter?.idCliente != null) return Number(filter.idCliente);
    const found = await docOrQuery.model
      .findOne(filter)
      .select("idCliente")
      .lean();
    return Number(found?.idCliente);
  } catch {
    return undefined;
  }
}

ClienteSchema.post("save", async function (doc, next) {
  try {
    const idCliente = Number(doc?.idCliente);
    if (Number.isFinite(idCliente)) await recomputeGroupPricing(idCliente);
    next();
  } catch (e) {
    next(e);
  }
});

ClienteSchema.post("findOneAndUpdate", async function (_doc, next) {
  try {
    const idCliente = await _getIdClienteFromOp(this);
    if (Number.isFinite(idCliente)) await recomputeGroupPricing(idCliente);
    next();
  } catch (e) {
    next(e);
  }
});

ClienteSchema.post("findOneAndDelete", async function (_doc, next) {
  try {
    const idCliente = await _getIdClienteFromOp(this);
    if (Number.isFinite(idCliente)) await recomputeGroupPricing(idCliente);
    next();
  } catch (e) {
    next(e);
  }
});

ClienteSchema.post(
  "deleteOne",
  { document: false, query: true },
  async function (_res, next) {
    try {
      const idCliente = await _getIdClienteFromOp(this);
      if (Number.isFinite(idCliente)) await recomputeGroupPricing(idCliente);
      next();
    } catch (e) {
      next(e);
    }
  }
);

export default mongoose.model("clientes", ClienteSchema);
