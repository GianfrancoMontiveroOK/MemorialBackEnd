import mongoose from "mongoose";

const AgeTierSchema = new mongoose.Schema(
  {
    min: { type: Number, required: true },
    coef: { type: Number, required: true },
  },
  { _id: false }
);

const PriceRulesSchema = new mongoose.Schema(
  {
    base: { type: Number, default: 16000 },
    cremationCoef: { type: Number, default: 0.125 },
    group: {
      neutralAt: { type: Number, default: 4 },
      step: { type: Number, default: 0.25 },
      minMap: {
        type: Map,
        of: Number,
        default: () => ({ 1: 0.5, 2: 0.75, 3: 1.0 }),
      },
    },
    age: {
      type: [AgeTierSchema],
      default: () => [
        { min: 66, coef: 1.375 },
        { min: 61, coef: 1.25 },
        { min: 51, coef: 1.125 },
      ],
    },
  },
  { _id: false }
);

const GlobalSettingsSchema = new mongoose.Schema(
  {
    singleton: { type: String, unique: true, default: "GLOBAL" },
    priceRules: { type: PriceRulesSchema, default: () => ({}) },
    updatedBy: { type: String },
  },
  { timestamps: true }
);

export default mongoose.model("GlobalSettings", GlobalSettingsSchema);
