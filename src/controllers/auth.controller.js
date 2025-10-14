import User from "../models/user.model.js";
import { sendConfirmationEmail } from "../controllers/sendEmail.controller.js";
import crypto from "crypto";
import bcrypt from "bcryptjs";

export const register = async (req, res) => {
  const { email, password, name } = req.body;
  try {
    const userFound = await User.findOne({ email });
    if (userFound) return res.status(400).json(["the email is already use"]);

    const passwordHash = await bcrypt.hash(password, 10);
    const emailToken = crypto.randomBytes(32).toString("hex");

    const newUser = new User({
      name,
      email,
      password: passwordHash,
      emailVerified: false,
      emailToken: emailToken,
    });

    await newUser.save();

    await sendConfirmationEmail({
      name: newUser.name,
      email: newUser.email,
      token: newUser.emailToken,
    });

    // No iniciar sesión ni devolver datos sensibles
    return res.status(200).json({
      message: "Registro exitoso. Revisa tu correo para confirmar tu cuenta.",
    });
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

export const login = async (req, res) => {
  const { email, password } = req.body;
  try {
    const userFound = await User.findOne({ email });
    if (!userFound) return res.status(400).json({ message: "User not found" });

    // 🚩 Solo permitir login si el email está verificado
    if (!userFound.emailVerified) {
      return res
        .status(401)
        .json({ message: "Debes confirmar tu email antes de iniciar sesión." });
    }

    const isMatch = await bcrypt.compare(password, userFound.password);
    if (!isMatch)
      return res.status(400).json({ message: "Incorrect password" });

    req.session.user = {
      id: userFound._id,
      name: userFound.name,
      email: userFound.email,
    };

    return res.status(200).json({ user: userFound });
  } catch (error) {
    return res.status(500).json({ message: error.message });
  }
};

export const logout = (req, res) => {
  res.cookie("token", "", {
    expires: new Date(0),
  });
  return res.sendStatus(200);
};

export const profile = (req, res) => {
  res.send("profile");
};

export const verifyToken = async (req, res) => {
  try {
    const sessionUser = req.session?.user;
    console.log("Session:", sessionUser); // <-- LOG
    if (!sessionUser) {
      return res.status(401).json({ message: "Unauthorized" });
    }
    const userFound = await User.findById(sessionUser.id);
    if (!userFound) {
      return res.status(401).json({ message: "Unauthorized" });
    }
    if (!userFound.emailVerified) {
      return res
        .status(401)
        .json({ message: "Debes confirmar tu email antes de iniciar sesión." });
    }

    // 4️⃣ Preparar la respuesta con los detalles necesarios del usuario
    const userData = {
      id: userFound._id,
      name: userFound.name,
      email: userFound.email,
      role: userFound.role,
      plan: userFound.plan,
      createdAt: userFound.createdAt,
      updatedAt: userFound.updatedAt,
      // Agrega aquí otros campos si los necesitas
    };

    return res.status(200).json(userData);
  } catch (error) {
    console.error("Error verifying token:", error.message);
    return res.status(500).json({ message: "Internal Server Error" });
  }
};

export const confirmEmail = async (req, res) => {
  try {
    const { token } = req.query;
    console.log(token);
    if (!token) {
      return res.status(400).json({ message: "Token faltante" });
    }

    const user = await User.findOne({ emailToken: token });
    if (!user) {
      return res.status(400).json({ message: "Token inválido" });
    }

    if (user.emailVerified) {
      return res
        .status(200)
        .json({ message: "Email ya confirmado previamente." });
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
    const { nombreComercial, telefono, emailFacturacion, cuit, ciudad } =
      req.body;

    const user = await User.findById(userId);
    if (!user)
      return res.status(404).json({ message: "Usuario no encontrado" });

    user.role = "organizer";
    if (!user.plan || user.plan === "free") user.plan = "free";
    user.organizerProfile = {
      ...(user.organizerProfile || {}),
      organization: nombreComercial,
      phone: telefono,
      billingEmail: emailFacturacion,
      taxId: cuit || undefined,
      city: ciudad,
    };
    await user.save();

    if (req.session?.user) {
      req.session.user.role = user.role;
      req.session.user.plan = user.plan;
    }

    return res.status(200).json({
      id: user._id,
      name: user.name,
      email: user.email,
      role: user.role,
      plan: user.plan,
      organizerProfile: user.organizerProfile,
      createdAt: user.createdAt,
      updatedAt: user.updatedAt,
    });
  } catch (e) {
    console.error("organizerOnboarding:", e);
    return res.status(500).json({ message: "Error al dar de alta el perfil" });
  }
};
