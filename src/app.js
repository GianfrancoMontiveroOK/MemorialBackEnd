// src/app.js
import express from "express";
import morgan from "morgan";
import cookieParser from "cookie-parser";
import cors from "cors";
import session from "express-session";
import MongoStore from "connect-mongo";
import dotenv from "dotenv";
import fs from "fs";
import path from "path";

// Rutas
import authRoutes from "./routes/auth.routes.js";
import dashboardRoutes from "./routes/dashboard.routes.js";
import usersRoutes from "./routes/users.routes.js";
import clientsRoutes from "./routes/clients.routes.js";
import collectorRoutes from "./routes/collector.routes.js";
import settingsRoutes from "./routes/settings.routes.js";
import repriceRoutes from "./routes/admin.reprice.routes.js";
import adminTransactionsRoutes from "./routes/admin.transactions.routes.js";
import adminReceiptsRoutes from "./routes/admin-receipts.routes.js";
import receiptsRouter from "./routes/receipts.routes.js";
import adminLedgerRoutes from "./routes/admin.ledger.routes.js";
// (Opcional) cron de pricing (puede moverse al bootstrap/server.js)
import { scheduleDailyPricingRecompute } from "./job/pricing.jobs.js";
import adminOutboxRoutes from "./routes/admin.outbox.routes.js";
import adminArqueosRoutes from "./routes/admin.arqueos.routes.js";
import adminStatsRoutes from "./routes/admin.stats.routes.js";
import adminItemsRoutes from "./routes/admin.items.routes.js";
dotenv.config();

const app = express();
const isProd = process.env.NODE_ENV === "production";

// 🔐 MUY IMPORTANTE detrás de proxy (Render/Heroku/NGINX)
app.set("trust proxy", 1);

// 🔗 Orígenes permitidos
const ALLOWED_ORIGINS = [
  process.env.FRONT_ORIGIN, // opcional por .env
].filter(Boolean);

// CORS (con credenciales)
const corsOptions = {
  origin(origin, cb) {
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
};
app.use(cors(corsOptions));
app.options("*", cors(corsOptions));

// Middlewares
app.use(morgan("dev"));
app.use(express.json({ limit: "2mb" }));
app.use(cookieParser());

// Sesión (cookie cross-site en prod)
const mongoUri = process.env.MONGODB_URI;
if (!mongoUri) {
  console.warn(
    "⚠️  MONGODB_URI no está definido. La sesión no podrá persistir."
  );
}
app.use(
  session({
    name: process.env.SESSION_NAME || "sid",
    proxy: true,
    secret: process.env.SESSION_SECRET || "somesecret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: isProd,
      sameSite: isProd ? "none" : "lax",
      maxAge: 1000 * 60 * 60 * 24 * 30, // 30 días
      path: "/",
    },
    store: mongoUri
      ? MongoStore.create({
          mongoUrl: mongoUri,
          collectionName: "sessionMemorial",
          stringify: false,
          autoRemove: "native",
        })
      : undefined,
  })
);

// Healthcheck
app.get("/health", (_req, res) => res.json({ ok: true }));

/* =========================
   STATIC: RECEIPTS (PDFs)
   ========================= */
const RECEIPTS_DIR =
  process.env.RECEIPTS_DIR || path.join(process.cwd(), "storage", "receipts");

// Aseguro existencia de la carpeta
fs.mkdirSync(RECEIPTS_DIR, { recursive: true });

// Servir como /files/receipts/<archivo>.pdf
app.use(
  "/files/receipts",
  express.static(RECEIPTS_DIR, {
    maxAge: "7d",
    setHeaders: (res) => {
      res.setHeader("Access-Control-Expose-Headers", "Content-Disposition");
    },
  })
);

// Rutas API
app.use("/api", authRoutes);
app.use("/api", dashboardRoutes);
app.use("/api", usersRoutes);
app.use("/api/clientes", clientsRoutes);
app.use("/api/collector", collectorRoutes);
app.use("/api/settings", settingsRoutes);
app.use("/api/admin", repriceRoutes);
app.use("/api/adminTransactions", adminTransactionsRoutes);
app.use("/api", adminReceiptsRoutes);
app.use("/api", receiptsRouter);
app.use("/api", adminLedgerRoutes);
app.use("/api", adminOutboxRoutes);
app.use("/api", adminArqueosRoutes);
app.use("/api", adminStatsRoutes);
app.use("/admin/items", adminItemsRoutes);
// 404
app.use((req, res) => {
  if (req.path === "/favicon.ico") return res.status(204).end();
  res.status(404).json({ ok: false, message: "Not Found" });
});

// Error handler
app.use((err, req, res, _next) => {
  const isCorsErr = /CORS bloqueado/i.test(err?.message || "");
  const status = isCorsErr ? 403 : err?.status || 500;
  console.error(err);
  res.status(status).json({
    ok: false,
    error: isCorsErr ? "CORS error" : "Internal server error",
    detail: err?.message,
  });
});

// (Opcional) cron
if (process.env.ENABLE_PRICING_CRON === "1") {
  scheduleDailyPricingRecompute();
}

export default app;
