import express from "express";
import morgan from "morgan";
import cookieParser from "cookie-parser";
import cors from "cors";
import session from "express-session";
import MongoStore from "connect-mongo";
import dotenv from "dotenv";
import authRoutes from "./routes/auth.routes.js";
import dashboardRoutes from "./routes/dashboard.routes.js";
import fs from "fs";
import path from "path";
import usersRoutes from "./routes/users.routes.js";
import clientsRoutes from "./routes/clients.routes.js";
import collectorRoutes from "./routes/collector.routes.js";
import settingsRoutes from "./routes/settings.routes.js"; // 👈 NUEVO

dotenv.config();

const app = express();

// CORS…
app.use(
  cors({
    origin: ["http://localhost:3000", "https://memorialclient.onrender.com"],
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

// Sesión
app.use(
  session({
    proxy: true,
    secret: process.env.SESSION_SECRET || "somesecret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: process.env.NODE_ENV === "production",
      httpOnly: true,
      maxAge: 1000 * 60 * 60 * 24 * 30,
    },
    store: MongoStore.create({
      mongoUrl: process.env.MONGODB_URI,
      collectionName: "sessionMemorial",
    }),
  })
);

// Rutas
app.use("/api", authRoutes);
app.use("/api", dashboardRoutes);
app.use("/api", usersRoutes);
app.use("/api/clientes", clientsRoutes);
app.use("/api/collector", collectorRoutes);
app.use("/api/settings", settingsRoutes); // 👈 NUEVO

// Uploads
const UPLOADS_ROOT =
  process.env.UPLOADS_DIR || path.join(process.cwd(), "uploads");
fs.mkdirSync(path.join(UPLOADS_ROOT, "flyers"), { recursive: true });
app.use("/uploads", express.static(UPLOADS_ROOT));

// Error handler
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});

export default app;
