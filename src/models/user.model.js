// models/user.model.js
import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      required: true,
      unique: true,
      lowercase: true,
      trim: true,
      index: true,
    },
    password: {
      type: String,
      required: true,
    },
    name: {
      type: String,
      trim: true,
      required: true,
    },
    // En user.model.js, dentro del schema:
    idCobrador: { type: String, index: true }, // o ObjectId si referencia otra colección
    idVendedor: { type: String, index: true },

    // Roles: buyer (default), organizer, staff, admin
    role: {
      type: String,
      enum: ["admin", "user", "client", "superAdmin", "cobrador", "vendedor"], // <- agregá "client"
      default: "user",
    },

    emailVerified: { type: Boolean, default: false },

    // ⚠️ Verificación de email
    emailToken: { type: String, index: true },

    // Recuperación de contraseña
    resetToken: String,
    resetTokenExpires: Date,
  },
  {
    timestamps: true,
    toJSON: {
      transform(doc, ret) {
        delete ret.password;
        delete ret.resetToken;
        delete ret.resetTokenExpires;
        delete ret.emailToken;
        delete ret.emailTokenExpiresAt;
        delete ret.__v;
        return ret;
      },
    },
  }
);

export default mongoose.model("userMemorial", userSchema);
