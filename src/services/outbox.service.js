// src/services/outbox.service.js
/**
 * Servicio de Outbox: encola eventos y actualiza su estado.
 *
 * API expuesta:
 *  - enqueue(topic, payload, { session?, dedupeKey?, dedupeTtlMs? })
 *  - markSent(id, extra?, { session? })
 *  - markFailed(id, errorMessage?, extra?, { session? })
 *  - requeue(id, { session? })
 *  - getById(id)
 *
 * Requisitos mínimos del modelo:
 *  - fields: topic (String), payload (Mixed), status (String), attempts (Number)
 *  - opcionales (si tu schema los tiene): dedupeKey, lastError, lastAttemptAt, processedAt, meta
 */

import mongoose from "mongoose";
import Outbox from "../models/outbox.model.js";

const DEFAULT_MAX_ATTEMPTS = Number(process.env.OUTBOX_MAX_ATTEMPTS || 10);

/** Normaliza un ObjectId válido o null */
function asObjectId(id) {
  try {
    return new mongoose.Types.ObjectId(String(id));
  } catch {
    return null;
  }
}

/**
 * Encola un evento de outbox.
 * - Si se pasa dedupeKey y dedupeTtlMs: evita duplicados recientes (status=pending o createdAt > now-ttl)
 * - Devuelve el documento (lean) creado o existente si se deduplicó.
 */
export async function enqueue(
  topic,
  payload,
  {
    session = undefined,
    dedupeKey = undefined,
    dedupeTtlMs = 0,
    meta = undefined,
  } = {}
) {
  if (!topic || typeof topic !== "string") {
    throw new Error("enqueue: topic requerido (string)");
  }

  // Estrategia de deduplicación opcional (best-effort)
  if (dedupeKey) {
    const and = [{ topic }, { dedupeKey }];
    if (dedupeTtlMs > 0) {
      const since = new Date(Date.now() - Number(dedupeTtlMs));
      and.push({ createdAt: { $gte: since } });
    }
    const existing = await Outbox.findOne({ $and: and })
      .sort({ createdAt: -1, _id: -1 })
      .session(session || null)
      .lean();
    if (existing) return existing;
  }

  const doc = await Outbox.create(
    [
      {
        topic,
        payload: payload ?? {},
        status: "pending",
        attempts: 0,
        // opcionales; si tu schema no los tiene, Mongoose (strict) los ignorará
        dedupeKey: dedupeKey || undefined,
        meta: meta || undefined,
      },
    ],
    { session: session || undefined }
  );

  return doc[0].toObject();
}

/**
 * Marca un evento como enviado/procesado con éxito.
 * Intenta setear campos opcionales si existen en el schema.
 */
export async function markSent(id, extra = {}, { session } = {}) {
  const _id = asObjectId(id);
  if (!_id) throw new Error("markSent: id inválido");

  const update = {
    $set: {
      status: "sent",
      processedAt: new Date(),
      ...(extra || {}),
    },
  };

  await Outbox.updateOne({ _id }, update, { session: session || undefined });
  return await Outbox.findById(_id).lean();
}

/**
 * Marca un evento como fallido (incrementa attempts).
 * Si supera OUTBOX_MAX_ATTEMPTS, lo deja en status="failed" definitivo.
 * Si no, lo deja en "pending" para reintento.
 */
export async function markFailed(
  id,
  errorMessage = "unknown_error",
  extra = {},
  { session } = {}
) {
  const _id = asObjectId(id);
  if (!_id) throw new Error("markFailed: id inválido");

  // Obtenemos attempts actuales
  const cur = await Outbox.findById(_id)
    .session(session || null)
    .lean();
  if (!cur) throw new Error("markFailed: evento no encontrado");

  const nextAttempts = Number(cur.attempts || 0) + 1;
  const reachedMax = nextAttempts >= DEFAULT_MAX_ATTEMPTS;

  const update = {
    $set: {
      status: reachedMax ? "failed" : "pending",
      lastError: String(errorMessage || "unknown_error"),
      lastAttemptAt: new Date(),
      ...(extra || {}),
    },
    $inc: { attempts: 1 },
  };

  await Outbox.updateOne({ _id }, update, { session: session || undefined });
  return await Outbox.findById(_id).lean();
}

/**
 * Reencola manualmente (acción "Reintentar" en el panel).
 * - Resetea status a "pending" si estaba "failed"
 * - (Opcional) reduce attempts en 1 para no bloquear por límite (configurable si querés).
 */
export async function requeue(id, { session } = {}) {
  const _id = asObjectId(id);
  if (!_id) throw new Error("requeue: id inválido");

  const cur = await Outbox.findById(_id)
    .session(session || null)
    .lean();
  if (!cur) throw new Error("requeue: evento no encontrado");

  const update = {
    $set: {
      status: "pending",
      lastError: undefined,
    },
  };

  await Outbox.updateOne({ _id }, update, { session: session || undefined });
  return await Outbox.findById(_id).lean();
}

/** Utilidad simple */
export async function getById(id) {
  const _id = asObjectId(id);
  if (!_id) return null;
  return await Outbox.findById(_id).lean();
}
