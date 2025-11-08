import mongoose from "mongoose";

/**
 * Caja/rendición del cobrador:
 *  - 'open' → registra pagos → 'closed' → 'submitted' → 'approved'/'rejected'
 *  - Totales cacheados para UI rápida (se recalculan server-side al cerrar).
 */

const STATUSES = ["open", "closed", "submitted", "approved", "rejected"];

const CashSessionSchema = new mongoose.Schema(
  {
    collector: {
      idCobrador: { type: Number, required: true, index: true },
      userId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "users",
        required: true,
        index: true,
      },
    },

    status: { type: String, enum: STATUSES, default: "open", index: true },
    openedAt: { type: Date, default: () => new Date(), index: true },
    closedAt: { type: Date },

    // Totales por método
    totals: {
      efectivo: { type: Number, default: 0 },
      transferencia: { type: Number, default: 0 },
      tarjeta: { type: Number, default: 0 },
      qr: { type: Number, default: 0 },
      otros: { type: Number, default: 0 },
    },
    expectedTotal: { type: Number, default: 0 },
    declaredTotal: { type: Number, default: 0 },
    diff: { type: Number, default: 0 },

    commissionBase: { type: Number, default: 0 },
    commissionCalc: { type: Number, default: 0 },

    notes: { type: String, trim: true },
    attachments: [{ kind: String, url: String }],
  },
  { timestamps: true, versionKey: false }
);

CashSessionSchema.index({ "collector.idCobrador": 1, status: 1, openedAt: -1 });

export default mongoose.model("cashsessions", CashSessionSchema);
