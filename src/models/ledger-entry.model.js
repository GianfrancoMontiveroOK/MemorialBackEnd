// src/models/ledgerEntry.model.js
import mongoose from "mongoose";

/**
 * Doble partida mínima:
 *  - Cada Payment 'posted' genera al menos 2 líneas:
 *    * DEBIT  CAJA_COBRADOR (o A_RENDIR_COBRADOR)
 *    * CREDIT INGRESOS_CUOTAS (o CUENTAS_A_COBRAR si manejás devengado)
 *  - Reversals generan líneas invertidas.
 */

const SIDES = ["debit", "credit"];

const LedgerEntrySchema = new mongoose.Schema(
  {
    paymentId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "payments",
      required: true,
      index: true,
    },

    // 👇 NUEVO: quién ejecutó el asiento (retiro, cobro, etc.)
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "userMemorial",
      required: true,
      index: true,
    },

    side: { type: String, enum: SIDES, required: true },
    accountCode: { type: String, required: true, index: true }, // ej: CAJA_COBRADOR, INGRESOS_CUOTAS
    amount: { type: Number, required: true, min: 0 },
    currency: { type: String, default: "ARS" },
    postedAt: { type: Date, default: () => new Date(), index: true },

    // Dimensiones analíticas
    dimensions: {
      idCobrador: { type: Number, index: true },
      idCliente: { type: Number, index: true },
      plan: { type: String },
      canal: { type: String },
      // agrega lo que necesites para BI (zona, sucursal, etc.)
    },
  },
  { timestamps: true, versionKey: false }
);

// Índices útiles
LedgerEntrySchema.index({ accountCode: 1, postedAt: -1 });
LedgerEntrySchema.index({ userId: 1, postedAt: -1 });
LedgerEntrySchema.index({ userId: 1, currency: 1, postedAt: -1 });
LedgerEntrySchema.index({ "dimensions.idCobrador": 1, postedAt: -1 });

export default mongoose.model("ledgerentries", LedgerEntrySchema);
