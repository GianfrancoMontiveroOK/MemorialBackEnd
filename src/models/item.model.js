// src/models/item.model.js
import mongoose from "mongoose";

const { Schema } = mongoose;

/**
 * Ítems de stock de Memorial (por ahora, cajones).
 *
 * Registra:
 * - precioCompra: cuánto le cuesta a la cochería
 * - margenPct: margen que se quiere aplicar (%)
 * - precioLista: se calcula automáticamente a partir de compra + margen
 * - adicionalPct / adicionalValor: extras cuando corresponde
 * - stockActual: cantidad disponible en depósito
 */
const itemSchema = new Schema(
  {
    codigo: {
      type: String,
      trim: true,
      unique: true,
      sparse: true,
    },
    nombre: {
      type: String,
      trim: true,
      required: true,
    },
    descripcion: {
      type: String,
      trim: true,
    },

    // Por si después agregás otros tipos de ítems
    tipo: {
      type: String,
      enum: ["cajon", "otro"],
      default: "cajon",
    },

    // STOCK
    stockActual: {
      type: Number,
      default: 0,
      min: 0,
    },

    // PRECIOS
    precioCompra: {
      type: Number,
      default: 0,
      min: 0,
    },
    margenPct: {
      type: Number,
      default: 0,
    },
    // Se recalcula automáticamente cuando cambia precioCompra o margenPct
    precioLista: {
      type: Number,
      default: 0,
      min: 0,
    },

    // ADICIONALES
    adicionalPct: {
      type: Number,
      default: 0,
    },
    adicionalValor: {
      type: Number,
      default: 0,
      min: 0,
    },

    activo: {
      type: Boolean,
      default: true,
    },
    deletedAt: {
      type: Date,
    },
  },
  {
    timestamps: true,
  }
);

// 🔹 Cálculo automático de precio de lista
itemSchema.pre("save", function (next) {
  if (this.isModified("precioCompra") || this.isModified("margenPct")) {
    const base = Number(this.precioCompra) || 0;
    const pct = Number(this.margenPct) || 0;
    const precioLista = base * (1 + pct / 100);

    // Podés redondear a múltiplos si querés (ej a 10 / 100 / 500)
    this.precioLista = Math.round(precioLista);
  }
  next();
});

const Item = mongoose.model("Item", itemSchema);

export default Item;
