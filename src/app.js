// src/app.js (o donde montes Express)
import express from "express";
import morgan from "morgan";
import cookieParser from "cookie-parser";
import cors from "cors";
import session from "express-session";
import MongoStore from "connect-mongo";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";

import authRoutes from "./routes/auth.routes.js";
import dashboardRoutes from "./routes/dashboard.routes.js";
import usersRoutes from "./routes/users.routes.js";
import clientsRoutes from "./routes/clients.routes.js";
import collectorRoutes from "./routes/collector.routes.js";
import settingsRoutes from "./routes/settings.routes.js";

dotenv.config();

const app = express();
const isProd = process.env.NODE_ENV === "production";

// 🔐 MUY IMPORTANTE detrás de proxy (Render/Heroku/NGINX)
app.set("trust proxy", 1);

// 🔗 Define el/los orígenes permitidos (sin '/')
const ALLOWED_ORIGINS = [
  "http://localhost:3000",
  "https://memorialclient.onrender.com",
  process.env.FRONT_ORIGIN, // opcional por .env
].filter(Boolean);

// CORS (con credenciales)
app.use(
  cors({
    origin(origin, cb) {
      // permite Postman/insomnia (sin Origin) y orígenes listados
      if (!origin || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
      return cb(new Error(`CORS bloqueado para ${origin}`));
    },
    credentials: true,
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: [
      "Content-Type",
      "Authorization",
      "X-Requested-With",
      "cache-control",
      "x-csrf-token",
    ],
  })
);

// Middlewares
app.use(morgan("dev"));
app.use(express.json());
app.use(cookieParser());

// Sesión (cookie cross-site en prod)
app.use(
  session({
    name: "sid", // opcional: nombre de cookie
    proxy: true, // necesario con trust proxy
    secret: process.env.SESSION_SECRET || "somesecret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: isProd, // obliga HTTPS en prod
      sameSite: isProd ? "none" : "lax", // cross-site requiere 'none'
      maxAge: 1000 * 60 * 60 * 24 * 30, // 30 días
      // domain: ".tudominio.com" // SOLO si querés compartir entre subdominios
      path: "/",
    },
    store: MongoStore.create({
      mongoUrl: process.env.MONGODB_URI,
      collectionName: "sessionMemorial",
      stringify: false,
      autoRemove: "native",
    }),
  })
);

// Rutas
app.use("/api", authRoutes);
app.use("/api", dashboardRoutes);
app.use("/api", usersRoutes);
app.use("/api/clientes", clientsRoutes);
app.use("/api/collector", collectorRoutes);
app.use("/api/settings", settingsRoutes);

// Static uploads
const UPLOADS_ROOT =
  process.env.UPLOADS_DIR || path.join(process.cwd(), "uploads");
fs.mkdirSync(path.join(UPLOADS_ROOT, "flyers"), { recursive: true });
app.use("/uploads", express.static(UPLOADS_ROOT));

// Error handler
app.use((err, req, res, next) => {
  console.error(err);
  res
    .status(500)
    .json({ error: "Internal server error", detail: err?.message });
});

export default app;
