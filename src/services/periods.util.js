// /src/services/periods.util.js
export function toYYYYMM(dateLike) {
  const d = dateLike ? new Date(dateLike) : new Date();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

export function parseYYYYMM(str) {
  const [y, m] = String(str || "")
    .split("-")
    .map(Number);
  if (!y || !m || m < 1 || m > 12) throw new Error(`Periodo inválido: ${str}`);
  return { y, m };
}

export function comparePeriods(a, b) {
  const { y: ya, m: ma } = parseYYYYMM(a);
  const { y: yb, m: mb } = parseYYYYMM(b);
  if (ya !== yb) return ya - yb;
  return ma - mb;
}

export function nextPeriod(p) {
  const { y, m } = parseYYYYMM(p);
  const mm = m === 12 ? 1 : m + 1;
  const yy = m === 12 ? y + 1 : y;
  return `${yy}-${String(mm).padStart(2, "0")}`;
}

export function prevPeriod(p) {
  const { y, m } = parseYYYYMM(p);
  const mm = m === 1 ? 12 : m - 1;
  const yy = m === 1 ? y - 1 : y;
  return `${yy}-${String(mm).padStart(2, "0")}`;
}

export function rangePeriods(from, to) {
  const out = [];
  let cur = from;
  while (comparePeriods(cur, to) <= 0) {
    out.push(cur);
    cur = nextPeriod(cur);
  }
  return out;
}
