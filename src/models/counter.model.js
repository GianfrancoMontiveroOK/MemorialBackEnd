// src/models/counter.model.js
import mongoose from "mongoose";

/**
 * Counter simple y atómico por clave (e.g., "receipt:2025")
 * Sirve para numeradores con findOneAndUpdate + $inc (atomicidad a nivel doc).
 */
const CounterSchema = new mongoose.Schema(
  {
    _id: { type: String, required: true }, // clave del contador (p.ej., "receipt:2025")
    seq: { type: Number, required: true, default: 0 },
    meta: {
      period: { type: String }, // opcional (año/mes)
      note: { type: String },
    },
  },
  { timestamps: true }
);

CounterSchema.index({ _id: 1 }, { unique: true });

const Counter = mongoose.model("Counter", CounterSchema);
export default Counter;
