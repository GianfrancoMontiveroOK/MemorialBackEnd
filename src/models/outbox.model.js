import mongoose from "mongoose";

/**
 * Outbox: eventos para integrar con ERP (ARCA).
 * - Se escribe en la MISMA transacción que el Payment/Ledger.
 * - Un worker lee 'pending' y publica. Maneja retries.
 */

const STATUSES = ["pending", "sent", "failed"];

const OutboxSchema = new mongoose.Schema(
  {
    topic: { type: String, required: true, index: true }, // ej: payment.posted, payment.reversed, cashsession.approved
    payload: { type: Object, required: true }, // cuerpo inmutable para enviar
    status: { type: String, enum: STATUSES, default: "pending", index: true },
    attempts: { type: Number, default: 0 },
    lastError: { type: String },
  },
  { timestamps: true, versionKey: false }
);

OutboxSchema.index({ status: 1, createdAt: 1 });

export default mongoose.model("outbox", OutboxSchema);
