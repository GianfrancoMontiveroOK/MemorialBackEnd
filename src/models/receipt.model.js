import mongoose from "mongoose";

const ReceiptSchema = new mongoose.Schema(
  {
    paymentId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Payment",
      index: true,
      required: true,
    },
    number: { type: String, index: true, sparse: true },
    pdfUrl: { type: String, default: null },

    qrData: { type: mongoose.Schema.Types.Mixed, default: null }, // ✅

    // opcionalmente signature también:
    signature: { type: mongoose.Schema.Types.Mixed, default: null },

    voided: { type: Boolean, default: false },
  },
  { timestamps: true }
);

export default mongoose.model("Receipt", ReceiptSchema);
