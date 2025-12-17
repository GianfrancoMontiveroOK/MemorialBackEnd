import User from "../models/user.model.js";
import crypto from "crypto";
import bcrypt from "bcryptjs";
import { sendConfirmationEmail } from "../controllers/sendEmail.controller.js";

/* ================== Helpers ================== */
const normalizeEmail = (email) => String(email || "").trim().toLowerCase();

function toSafeUser(userDoc) {
  if (!userDoc) return null;
  return {
    id: userDoc._id,
    name: userDoc.name,
    email: userDoc.email,
    emailVerified: userDoc.emailVerified,
    role: userDoc.role,
    plan: userDoc.plan,
    ui: userDoc.ui, // 👈 clave para theme (ui.themeMode)
    idCobrador: userDoc.idCobrador,
    idVendedor: userDoc.idVendedor,
    organizerProfile: userDoc.organizerProfile,
    createdAt: userDoc.createdAt,
    updatedAt: userDoc.updatedAt,
  };
}

/* ================== Auth ================== */
export const register = async (req, res) => {
  const email = normalizeEmail(req.body?.email);
  const password = String(req.body?.password || "");
  const name = String(req.body?.name || "").trim();

  // opcional: permitir setear modo inicial desde el frontend
  const themeModeRaw = String(req.body?.themeMode || "").trim();
  const themeMode =
    themeModeRaw === "light" || themeModeRaw === "dark" ? themeModeRaw : "dark";

  try {
    if (!email || !password || !name) {
      return res.status(400).json({ message: "Faltan campos obligatorios." });
    }

    const userFound = await User.findOne({ email });
    if (userFound) {
      return res.status(400).json({ message: "El email ya está en uso." });
    }

    const passwordHash = await bcrypt.hash(password, 10);
    const emailToken = crypto.randomBytes(32).toString("hex");

    const newUser = new User({
      name,
      email,
      password: passwordHash,
      emailVerified: false,
      emailToken,
      // 👇 conectar theme con el user
      ui: { themeMode },
    });

    await newUser.save();

    await sendConfirmationEmail({
      name: newUser.name,
      email: newUser.email,
      token: newUser.emailToken,
    });

    return res.status(200).json({
      message: "Registro exitoso. Revisa tu correo para confirmar tu cuenta.",
    });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
};

export const login = async (req, res) => {
  const email = normalizeEmail(req.body?.email);
  const password = String(req.body?.password || "");

  try {
    if (!email || !password) {
      return res.status(400).json({ message: "Email y contraseña son obligatorios." });
    }

    // ✅ Traemos password para comparar (por si está select:false)
    const userFound = await User.findOne({ email }).select("+password");
    if (!userFound) return res.status(400).json({ message: "Usuario no encontrado." });

    if (!userFound.emailVerified) {
      return res.status(401).json({
        message: "Debes confirmar tu email antes de iniciar sesión.",
      });
    }

    const isMatch = await bcrypt.compare(password, userFound.password);
    if (!isMatch) {
      return res.status(400).json({ message: "Contraseña incorrecta." });
    }

    // ✅ Session con lo necesario (incluye theme)
    req.session.user = {
      id: String(userFound._id),
      name: userFound.name,
      email: userFound.email,
      role: userFound.role,
      plan: userFound.plan,
      ui: userFound.ui,
      idCobrador: userFound.idCobrador,
      idVendedor: userFound.idVendedor,
    };

    return res.status(200).json({ user: toSafeUser(userFound) });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
};

export const logout = (req, res) => {
  try {
    // ✅ si usás express-session
    if (req.session) {
      req.session.destroy(() => {
        // nombre típico cookie de sesión: connect.sid (puede variar)
        res.clearCookie("connect.sid");
        return res.sendStatus(200);
      });
      return;
    }
    return res.sendStatus(200);
  } catch (e) {
    return res.status(500).json({ message: "Error al cerrar sesión." });
  }
};

export const profile = (req, res) => {
  res.send("profile");
};

export const verifyToken = async (req, res) => {
  try {
    const sessionUser = req.session?.user;
    if (!sessionUser?.id) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    // ⚠️ NO traemos password ni tokens
    const userFound = await User.findById(sessionUser.id);
    if (!userFound) return res.status(401).json({ message: "Unauthorized" });

    if (!userFound.emailVerified) {
      return res.status(401).json({
        message: "Debes confirmar tu email antes de iniciar sesión.",
      });
    }

    // refrescamos sesión con datos actuales (incluye themeMode)
    req.session.user = {
      id: String(userFound._id),
      name: userFound.name,
      email: userFound.email,
      role: userFound.role,
      plan: userFound.plan,
      ui: userFound.ui,
      idCobrador: userFound.idCobrador,
      idVendedor: userFound.idVendedor,
    };

    return res.status(200).json({ user: toSafeUser(userFound) });
  } catch (error) {
    console.error("Error verifying token:", error.message);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};

export const confirmEmail = async (req, res) => {
  try {
    const token = String(req.query?.token || "").trim();
    if (!token) return res.status(400).json({ message: "Token faltante" });

    const user = await User.findOne({ emailToken: token });
    if (!user) return res.status(400).json({ message: "Token inválido" });

    if (user.emailVerified) {
      return res.status(200).json({ message: "Email ya confirmado previamente." });
    }

    user.emailVerified = true;
    user.emailToken = undefined;
    user.emailTokenExpiresAt = undefined; // si lo usás
    await user.save();

    return res.status(200).json({ message: "Email confirmado correctamente" });
  } catch (error) {
    console.error("confirmEmail:", error);
    return res.status(500).json({ message: "Error interno" });
  }
};

export const organizerOnboarding = async (req, res) => {
  try {
    const userId = req.user?.id || req.session?.user?.id;
    const {
      nombreComercial,
      telefono,
      emailFacturacion,
      cuit,
      ciudad,
    } = req.body;

    if (!userId) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    const user = await User.findById(userId);
    if (!user) return res.status(404).json({ message: "Usuario no encontrado" });

    user.role = "organizer";
    if (!user.plan || user.plan === "free") user.plan = "free";

    user.organizerProfile = {
      ...(user.organizerProfile || {}),
      organization: String(nombreComercial || "").trim(),
      phone: String(telefono || "").trim(),
      billingEmail: String(emailFacturacion || "").trim(),
      taxId: cuit ? String(cuit).trim() : undefined,
      city: String(ciudad || "").trim(),
    };

    await user.save();

    // mantener sesión sincronizada
    if (req.session?.user) {
      req.session.user.role = user.role;
      req.session.user.plan = user.plan;
      req.session.user.ui = user.ui;
    }

    return res.status(200).json({ user: toSafeUser(user) });
  } catch (e) {
    console.error("organizerOnboarding:", e);
    return res.status(500).json({ message: "Error al dar de alta el perfil" });
  }
};
export const setMyPreferences = async (req, res) => {
  try {
    const userId = req.user?.id || req.session?.user?.id;
    if (!userId) return res.status(401).json({ message: "Unauthorized" });

    const themeModeRaw = String(req.body?.themeMode || "").trim();
    const themeMode =
      themeModeRaw === "light" || themeModeRaw === "dark" ? themeModeRaw : null;

    if (!themeMode) {
      return res.status(400).json({ message: "themeMode inválido (light|dark)" });
    }

    const user = await User.findByIdAndUpdate(
      userId,
      { $set: { "ui.themeMode": themeMode } },
      { new: true }
    );

    if (!user) return res.status(404).json({ message: "Usuario no encontrado" });

    // ✅ mantener sesión sincronizada (clave para que el frontend lo lea al toque)
    if (req.session?.user) {
      req.session.user.ui = { ...(req.session.user.ui || {}), themeMode };
    }

    return res.status(200).json({ user: toSafeUser(user) });
  } catch (e) {
    console.error("setMyPreferences:", e);
    return res.status(500).json({ message: "Error interno" });
  }
};
