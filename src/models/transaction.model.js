// models/transaction.model.js
import mongoose from "mongoose";

export const TRANSACTION_TYPES = [
  "payment",
  "refund",
  "reversal",
  "adjustment",
];
// payment=cobro, refund=devolución, reversal=anulación (revierte/payment), adjustment=ajuste contable

export const TRANSACTION_STATUS = [
  "pending",
  "completed",
  "failed",
  "reversed",
];

export const PAYMENT_METHODS = [
  "cash",
  "transfer",
  "debit_auto",
  "mp",
  "pos",
  "other",
];

const TransactionSchema = new mongoose.Schema(
  {
    // Identidad y correlación
    clientGeneratedId: { type: String, index: true }, // para PWA offline/idempotencia
    idempotencyKey: { type: String, index: true, unique: true, sparse: true },

    // Núcleo
    type: { type: String, enum: TRANSACTION_TYPES, required: true },
    status: {
      type: String,
      enum: TRANSACTION_STATUS,
      default: "completed",
      index: true,
    },
    method: { type: String, enum: PAYMENT_METHODS, required: true },

    amount: { type: Number, required: true, min: 0 },
    currency: { type: String, default: "ARS" },

    // Fechas
    effectiveAt: { type: Date, default: Date.now, index: true }, // fecha del cobro
    createdBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
      index: true,
    }, // cobrador/admin
    branchId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Branch",
      index: true,
    }, // sucursal/punto de venta
    customerId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Customer",
      required: true,
      index: true,
    },

    // Relación operativa
    receiptNumber: { type: String, index: true }, // correlativo emitido
    talonarioId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Talonario",
      index: true,
    },
    boxId: { type: mongoose.Schema.Types.ObjectId, ref: "Box", index: true }, // caja del día asociada

    // Reversiones / vínculos
    parentTransactionId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Transaction",
      index: true,
    },
    // ej: refund/reversal de un payment

    // Sincronización (offline y pasarelas)
    sync: {
      offlineQueued: { type: Boolean, default: false },
      syncedAt: { type: Date },
      provider: { type: String }, // mp/banco/etc
      providerRef: { type: String }, // id de operación externo
      webhooksOk: { type: Boolean, default: false },
    },

    // Auditoría y notas
    notes: { type: String },
    meta: { type: mongoose.Schema.Types.Mixed },
  },
  { timestamps: true }
);

// Índices compuestos útiles
TransactionSchema.index({ branchId: 1, effectiveAt: -1 });
TransactionSchema.index({ customerId: 1, effectiveAt: -1 });
TransactionSchema.index({ type: 1, status: 1, effectiveAt: -1 });

// Idempotencia: ignora duplicados con misma key
TransactionSchema.statics.findByIdempotency = function (key) {
  if (!key) return Promise.resolve(null);
  return this.findOne({ idempotencyKey: key });
};

export default mongoose.model("Transaction", TransactionSchema);
