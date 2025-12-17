// src/models/ledgerEntry.model.js
import mongoose from "mongoose";

const SIDES = ["debit", "credit"];

const LedgerEntrySchema = new mongoose.Schema(
  {
    paymentId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "payments",
      required: true,
      index: true,
    },

    // quién ejecutó/creó el asiento (actor del evento)
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "userMemorial",
      required: true,
      index: true,
    },

    // ✅ NUEVO (como vos querés): texto, no ObjectId
    fromUser: {
      type: String, // ← nombre (cliente o usuario)
      default: null,
      index: true,
    },
    toUser: {
      type: String, // ← nombre (cliente o usuario)
      default: null,
      index: true,
    },

    // ✅ (opcional pero MUY útil) de dónde → hacia dónde (cuentas lógicas)
    fromAccountCode: { type: String, default: null, index: true },
    toAccountCode: { type: String, default: null, index: true },

    // Tipo lógico del movimiento (ej: "payment", "commission_payout", "transfer", "ARQUEO_MANUAL")
    kind: { type: String, index: true },

    side: { type: String, enum: SIDES, required: true },
    accountCode: { type: String, required: true, index: true },
    amount: { type: Number, required: true, min: 0 },
    currency: { type: String, default: "ARS" },
    postedAt: { type: Date, default: () => new Date(), index: true },

    // Dimensiones analíticas / operativas
    // ✅ IMPORTANTE: declararlas para que NO se pierdan
    dimensions: {
      // analytics
      idCobrador: { type: Number, index: true, default: null },
      idCliente: { type: Number, index: true, default: null },
      plan: { type: String, default: null },
      canal: { type: String, default: null },
      note: { type: String, default: "" },
    },
  },
  { timestamps: true, versionKey: false }
);

export default mongoose.model("ledgerentries", LedgerEntrySchema);
