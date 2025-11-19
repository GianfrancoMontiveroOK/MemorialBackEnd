import mongoose from "mongoose";

const METHODS = [
  "efectivo",
  "transferencia",
  "tarjeta",
  "qr",
  "otro",
  "debito_automatico",
];
const CHANNELS = [
  "field",
  "backoffice",
  "portal",
  "api",
  "debito_automatico_naranja",
  "debito_automatico_bna",
];
const STATUSES = ["draft", "posted", "settled", "reversed"]; // 'deleted' NO existe (WORM)
const KINDS = ["payment", "adjustment", "reversal"]; // tipificar evento económico

// ===== ETAPA 1.2: allocations por período (MVP) =====
const ALLOC_STATUSES = ["paid", "partial"]; // estado del período después de aplicar
const PaymentAllocationSchema = new mongoose.Schema(
  {
    period: { type: String, required: true, trim: true }, // "YYYY-MM"
    amountApplied: { type: Number, required: true, min: 0 },
    statusAfter: { type: String, enum: ALLOC_STATUSES, required: true },
    memberId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "clientes",
      default: null,
    }, // MVP: titular, V2: por integrante
  },
  { _id: false }
);

const PaymentSchema = new mongoose.Schema(
  {
    kind: { type: String, enum: KINDS, default: "payment", index: true },

    // Identidad del cliente (cache útil para reportes)
    cliente: {
      memberId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "clientes",
        required: true,
        index: true,
      },
      idCliente: { type: Number, required: true, index: true }, // grupo
      nombre: { type: String, trim: true }, // opcional cache UI
    },

    // Identidad del cobrador/usuario operador
    collector: {
      idCobrador: { type: Number, required: true, index: true },
      userId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "users",
        required: true,
        index: true,
      },
    },

    // Montos
    currency: { type: String, default: "ARS" },
    amount: { type: Number, required: true, min: 0.01 },
    rounding: { type: Number, default: 0 }, // si aplicás redondeos

    // Clasificación del cobro
    intendedPeriod: { type: String }, // ej "2025-10" (opcional)
    method: { type: String, enum: METHODS, required: true, index: true },
    channel: { type: String, enum: CHANNELS, default: "field", index: true },

    // Ciclo de vida
    status: { type: String, enum: STATUSES, default: "draft", index: true },
    postedAt: { type: Date, index: true },
    settledAt: { type: Date }, // conciliado POS/banco

    // Reversa / ajuste
    reversalOf: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "payments",
      default: null,
      index: true,
    },

    // Idempotencia / referencias externas
    idempotencyKey: { type: String, required: true, unique: true },
    externalRef: { type: String, index: true }, // id POS/MP/ERP si aplica
    cashSessionId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "cashsessions",
      default: null,
      index: true,
    },

    // ===== ETAPA 1.2: nuevas estructuras =====
    allocations: {
      type: [PaymentAllocationSchema],
      default: [],
    }, // desglose por períodos aplicado en este pago

    meta: {
      periodsApplied: { type: [String], default: [] }, // para filtros rápidos e informes
    },

    // Metadata útil para campo
    notes: { type: String, trim: true },
    geo: { lat: Number, lng: Number },
    device: { type: String, trim: true },
    ip: { type: String, trim: true },

    // Auditoría
    createdBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "users",
      required: true,
    },
    updatedBy: { type: mongoose.Schema.Types.ObjectId, ref: "users" },
  },
  { timestamps: true, versionKey: false }
);

// Índices compuestos frecuentes
PaymentSchema.index({ "cliente.idCliente": 1, createdAt: -1 });
PaymentSchema.index({ "collector.idCobrador": 1, createdAt: -1 });
PaymentSchema.index({ status: 1, createdAt: -1 });
PaymentSchema.index({ method: 1, createdAt: -1 });

// ===== ETAPA 1.2: índices para consultas por período =====
PaymentSchema.index({ "allocations.period": 1, createdAt: -1 });
PaymentSchema.index({ "allocations.period": 1, "cliente.idCliente": 1 });

// Validaciones de negocio simples
PaymentSchema.path("amount").validate(function (v) {
  // permitir montos negativos sólo en 'adjustment' o 'reversal'
  if (v > 0) return true;
  return this.kind === "adjustment" || this.kind === "reversal";
}, "Monto inválido para tipo de transacción");

// Helper: marcar como posted
PaymentSchema.methods.markPosted = function () {
  if (this.status !== "draft") return;
  this.status = "posted";
  this.postedAt = new Date();
};

// Helper: marcar como settled
PaymentSchema.methods.markSettled = function () {
  if (this.status !== "posted") return;
  this.status = "settled";
  this.settledAt = new Date();
};

export default mongoose.model("payments", PaymentSchema);
