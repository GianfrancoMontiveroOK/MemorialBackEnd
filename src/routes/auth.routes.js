import { Router } from "express";
import {
  login,
  register,
  logout,
  profile,
  verifyToken,
  confirmEmail,
  organizerOnboarding,
  setMyPreferences, // ✅ NUEVO
} from "../controllers/auth.controller.js";

import { authRequired } from "../middlewares/validateToken.js";
import { validateSchema } from "../middlewares/validator.middleware.js";
import {
  registerSchema,
  loginSchema,
  organizerOnboardingSchema,
} from "../schemas/auth.schema.js";

const router = Router();

router.post("/register", validateSchema(registerSchema), register);
router.post("/login", validateSchema(loginSchema), login);
router.post("/logout", logout);

router.get("/verify", verifyToken);
router.get("/profile", authRequired, profile);

router.get("/confirmar-email", confirmEmail);

// ✅ preferencias UI (themeMode, etc.)
router.patch("/me/preferences", authRequired, setMyPreferences);

// ✅ bien: auth + validateSchema(schema) + controlador
router.post(
  "/socio/alta",
  authRequired,
  validateSchema(organizerOnboardingSchema),
  organizerOnboarding
);

export default router;
