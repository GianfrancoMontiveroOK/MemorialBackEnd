// src/utils/fs.util.js
import fs from "fs";
import path from "path";

/** Asegura que un directorio exista (mkdir -p). */
export function ensureDirp(dirPath) {
  if (!dirPath) return;
  fs.mkdirSync(dirPath, { recursive: true });
}

/** Ruta segura uniendo segmentos normalizados. */
export function joinSafe(...parts) {
  return path.join(...parts.map((p) => String(p || "")));
}
