// Leaderboard and scatterplot table logic, ported from the server function in
// 03_app/app.R. No DOM or fetch calls, so the parity tests can run it in Node.
import type { CiKey, LbRow, LbTable, Metric, Role, ShotFilter, SortKey } from "./types";

const ROLE_LABEL: Record<Role, string> = {
  offense: "Offensive Players", defense: "Defensive Players", team: "Team Defense",
};
const SHOT_LABEL: Record<ShotFilter, string> = {
  all: "All Shots", j3: "3-Pointers", j2: "Mid-Range", rim: "Rim Attempts",
};
const metricLabel = (metric: Metric) => (metric === "per100" ? "EPAA per 100" : "Total EPAA");

// output$lb_title in app.R
export const lbTitle = (role: Role, shot: ShotFilter, metric: Metric) =>
  `${ROLE_LABEL[role]}  •  ${SHOT_LABEL[shot]}  •  ${metricLabel(metric)}`;

// output$sc_title in app.R
export const scTitle = (role: Role, shot: ShotFilter, metric: Metric) =>
  `${ROLE_LABEL[role]}  •  ${SHOT_LABEL[shot]}  •  Shot Quality vs ${metricLabel(metric)}`;

export interface TableFilter {
  metric: Metric;
  minObs: number;
  team: string; // "all" or a team abbreviation
}

export interface LeaderboardOptions extends TableFilter {
  ci: CiKey;
  sort: SortKey;
}

type LoQ = "q10" | "q25" | "q40";
type HiQ = "q90" | "q75" | "q60";
const CI_COLUMNS: Record<CiKey, [LoQ, HiQ]> = {
  "80": ["q10", "q90"],
  "50": ["q25", "q75"],
  "20": ["q40", "q60"],
};

function selectRows(table: LbTable, filter: TableFilter, loQ: LoQ, hiQ: HiQ): LbRow[] {
  const isTeam = table.role === "team";
  const minObs = isTeam ? 0 : filter.minObs;
  const m = filter.metric;
  const mid = table[`${m}_mid`];
  const lo = table[`${m}_${loQ}`];
  const hi = table[`${m}_${hiQ}`];
  const rows: LbRow[] = [];
  for (let i = 0; i < table.idx.length; i++) {
    if (!(table.n_obs[i] >= minObs)) continue;
    if (!isTeam && filter.team !== "all" && table.team_abbr[i] !== filter.team) continue;
    rows.push({
      idx: table.idx[i],
      label: table.label[i],
      n_obs: table.n_obs[i],
      shot_quality: table.shot_quality[i],
      team_abbr: table.team_abbr[i],
      img_url: table.img_url[i],
      team_logo_url: table.team_logo_url[i],
      mid: mid[i],
      lo: lo[i],
      hi: hi[i],
    });
  }
  return rows;
}

// lb_data() in app.R: filter the cached table, pick the interval columns, sort.
// order() in R is stable and puts NA last; Array.prototype.sort is stable too.
export function lbData(table: LbTable, opts: LeaderboardOptions): LbRow[] {
  const [loQ, hiQ] = CI_COLUMNS[opts.ci] ?? CI_COLUMNS["80"];
  const rows = selectRows(table, opts, loQ, hiQ);
  const bySq = opts.sort === "sq_desc" || opts.sort === "sq_asc";
  const decreasing = bySq ? opts.sort === "sq_desc" : opts.sort !== "epaa_asc";
  const key = (r: LbRow) => (bySq ? r.shot_quality : r.mid);
  return rows.sort((a, b) => {
    const ka = key(a);
    const kb = key(b);
    const na = Number.isNaN(ka);
    const nb = Number.isNaN(kb);
    if (na || nb) return na === nb ? 0 : na ? 1 : -1;
    return decreasing ? kb - ka : ka - kb;
  });
}

// scatter_data() in app.R: talent is the posterior mean with its 80% interval.
export function scatterData(table: LbTable, filter: TableFilter): LbRow[] {
  return selectRows(table, filter, "q10", "q90");
}

// The team menu: sorted team abbreviations present in the table.
export function teamChoices(table: LbTable): string[] {
  if (table.role === "team") return [];
  return [...new Set(table.team_abbr.filter((t) => t.length > 0))].sort();
}

// ci_band_positive_pct() in app.R: share of the interval above zero.
export function ciBandPositivePct(lo: number, hi: number): number {
  const width = hi - lo;
  if (!Number.isFinite(lo) || !Number.isFinite(hi) || !(width > 0)) return NaN;
  const pos = Math.max(0, hi) - Math.max(0, lo);
  return 100 * Math.min(Math.max(pos / width, 0), 1);
}

// lb_x_range_from_df() in app.R, shared by the fixed axis and the scrolling body.
export function lbXRange(rows: LbRow[]): [number, number] {
  let min = Infinity;
  let max = -Infinity;
  for (const r of rows) {
    for (const v of [r.lo, r.hi]) {
      if (Number.isNaN(v)) continue;
      if (v < min) min = v;
      if (v > max) max = v;
    }
  }
  let pad = 0.08 * (max - min);
  if (!Number.isFinite(pad) || pad === 0) pad = 1;
  return [min - pad * 1.5, max + pad * 1.5];
}
