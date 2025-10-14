// schemas/auth.schema.js
import { z } from "zod";

export const registerSchema = z.object({
  name: z
    .string({ required_error: "El nombre es obligatorio" })
    .trim()
    .min(2, "El nombre es muy corto")
    .max(60, "El nombre es demasiado largo"),
  email: z
    .string({ required_error: "El email es obligatorio" })
    .trim()
    .email({ message: "Ingresa un mail válido" })
    .transform((v) => v.toLowerCase()),
  password: z
    .string({ required_error: "La contraseña es obligatoria" })
    .min(8, { message: "La contraseña debe tener al menos 8 caracteres" }),
  // Si querés permitir registrar organizers directamente:
  // role: z.enum(["buyer", "organizer", "staff", "admin"]).optional(),
  // plan: z.enum(["free", "pro"]).optional(),
});

export const loginSchema = z.object({
  email: z
    .string({ required_error: "El email es obligatorio" })
    .trim()
    .email({ message: "Email inválido" })
    .transform((v) => v.toLowerCase()),
  password: z
    .string({ required_error: "La contraseña es obligatoria" })
    .min(6, { message: "La contraseña debe tener al menos 6 caracteres" }),
});

// Útil para validar la query de /confirmar-email
export const confirmEmailQuerySchema = z.object({
  token: z.string().min(32, "Token inválido"),
});

export const organizerOnboardingSchema = z.object({
  nombreComercial: z.string().trim().min(2, "Ingresá el nombre comercial."),
  telefono: z.string().trim().min(6, "Ingresá un teléfono válido."),
  emailFacturacion: z
    .string()
    .trim()
    .email("Ingresá un email de facturación válido."),
  cuit: z.string().trim().optional().or(z.literal("")),
  ciudad: z.string().trim().min(2, "Ingresá la ciudad."),
  aceptaTyC: z
    .boolean()
    .refine((v) => v === true, "Debés aceptar los Términos y Condiciones."),
});
