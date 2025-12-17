import mongoose from "mongoose";

const SESSIONS_COLLECTION =
  process.env.SESSION_COLLECTION || "sessionMemorial";

/**
 * Revoca sesiones del usuario borrando docs de MongoStore.
 * // SUPOSICIÓN: en tu session guardás el user en alguno de estos paths:
 *   - session.user._id
 *   - session.userId
 *   - session.passport.user (Passport)
 *   - session.user.email
 */
export async function revokeUserSessions({ userId, email }) {
  const col = mongoose.connection?.db?.collection(SESSIONS_COLLECTION);
  if (!col) throw new Error("Mongo no está listo para revocar sesiones.");

  const uid = userId ? String(userId) : null;
  const em = email ? String(email).trim().toLowerCase() : null;

  const or = [];
  if (uid) {
    or.push(
      { "session.user._id": uid },
      { "session.user.id": uid },
      { "session.userId": uid },
      { "session.passport.user": uid }
    );
  }
  if (em) {
    or.push(
      { "session.user.email": em },
      { "session.email": em }
    );
  }

  if (!or.length) return { deletedCount: 0 };

  const r = await col.deleteMany({ $or: or });
  return { deletedCount: r?.deletedCount || 0 };
}
