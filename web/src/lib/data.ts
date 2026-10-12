import type { ActorKind, CalDraws, Family, FamilyDraws, LbTable, Meta, Role, ShotFilter, XgbModel, XptsEcdf } from "./types";
import { decodeModel } from "./xgb";

// Decoding is shared by the browser loaders and the Node parity tests.

function familyBlocks(buf: ArrayBuffer, nDraws: number, first: number): FamilyDraws {
  const block = (i: number) => new Float32Array(buf, 4 * nDraws * (first + i), nDraws);
  return { rim: block(0), j2: block(1), j3: block(2) };
}

// cal.bin: intercept for rim, j2, j3, then slope for rim, j2, j3.
export function decodeCal(meta: Meta, buf: ArrayBuffer): CalDraws {
  return { intercept: familyBlocks(buf, meta.n_draws, 0), slope: familyBlocks(buf, meta.n_draws, 3) };
}

// draws/<kind>/<idx>.bin: random-effect draws of one actor for rim, j2, j3.
export function decodeActorDraws(meta: Meta, buf: ArrayBuffer): FamilyDraws {
  return familyBlocks(buf, meta.n_draws, 0);
}

// JSON has no NaN; the export writes NA as null.
export function decodeLbTable(raw: LbTable): LbTable {
  const out: Record<string, unknown> = { ...raw };
  for (const [key, col] of Object.entries(raw)) {
    if (Array.isArray(col) && /^(shot_quality|total_|per100_)/.test(key)) {
      out[key] = (col as (number | null)[]).map((v) => v ?? NaN);
    }
  }
  return out as LbTable;
}

export const drawsPath = (meta: Meta, kind: ActorKind, idx: number) => `${meta.draws[kind]}/${idx}.bin`;
export const lbPath = (role: Role, shot: ShotFilter) => `lb/${role}__${shot}.json`;

const dataUrl = (path: string) => `${import.meta.env.BASE_URL}data/${path}`;

async function fetchBuffer(path: string): Promise<ArrayBuffer> {
  const res = await fetch(dataUrl(path));
  if (!res.ok) throw new Error(`Could not load ${path} (${res.status})`);
  return res.arrayBuffer();
}

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(dataUrl(path));
  if (!res.ok) throw new Error(`Could not load ${path} (${res.status})`);
  return res.json() as Promise<T>;
}

// Each loader runs once per key; later calls share the same promise. A failed
// load is dropped from the cache so the next attempt retries.
const cache = new Map<string, Promise<unknown>>();
function once<T>(key: string, load: () => Promise<T>): Promise<T> {
  let p = cache.get(key) as Promise<T> | undefined;
  if (!p) {
    p = load();
    p.catch(() => cache.delete(key));
    cache.set(key, p);
  }
  return p;
}

export const loadMeta = () => once("meta", () => fetchJson<Meta>("meta.json"));

export const loadCal = (): Promise<CalDraws> =>
  once("cal", async () => {
    const meta = await loadMeta();
    return decodeCal(meta, await fetchBuffer(meta.cal.file));
  });

export const loadEcdf = () => once("ecdf", () => fetchJson<XptsEcdf>("xpts_ecdf.json"));

export const loadModel = (family: Family): Promise<XgbModel> =>
  once(`model:${family}`, async () => {
    const meta = await loadMeta();
    const index = meta.models[family];
    return decodeModel(index, await fetchBuffer(index.file));
  });

export const loadActorDraws = (kind: ActorKind, idx: number): Promise<FamilyDraws> =>
  once(`draws:${kind}:${idx}`, async () => {
    const meta = await loadMeta();
    return decodeActorDraws(meta, await fetchBuffer(drawsPath(meta, kind, idx)));
  });

export const loadLbTable = (role: Role, shot: ShotFilter): Promise<LbTable> =>
  once(`lb:${role}:${shot}`, async () => decodeLbTable(await fetchJson<LbTable>(lbPath(role, shot))));
