// src/utils/crypto.util.js
import crypto from "crypto";

/**
 * Firma HMAC para "numeración segura" de recibos.
 * Evita manipulaciones: se firma número + fields críticos.
 */
export function hmacSign(payloadString, secret) {
  return crypto.createHmac("sha256", String(secret || "")).update(payloadString).digest("hex");
}

/** Crea una cadena canónica estable para firmar (orden fijo de campos). */
export function canonicalizeReceiptSignature({
  receiptNumber,
  paymentId,
  clientId,
  amount,
  postedAt,
}) {
  return [
    `r=${receiptNumber || ""}`,
    `p=${paymentId || ""}`,
    `c=${clientId || ""}`,
    `a=${Number(amount || 0).toFixed(2)}`,
    `t=${postedAt ? new Date(postedAt).toISOString() : ""}`,
  ].join("|");
}
