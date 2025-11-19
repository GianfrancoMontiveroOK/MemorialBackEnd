// src/middlewares/upload.js
import multer from "multer";

// Usamos memoria (no guardamos nada en disco)
const storage = multer.memoryStorage();

const upload = multer({
  storage,
  limits: {
    fileSize: 5 * 1024 * 1024, // máx 5 MB por archivo (ajustable)
  },
  fileFilter(req, file, cb) {
    // Podríamos filtrar por mimetype, pero los TXT bancarios a veces vienen
    // como text/plain u octet-stream, así que dejamos pasar todo.
    cb(null, true);
  },
});

export default upload;
