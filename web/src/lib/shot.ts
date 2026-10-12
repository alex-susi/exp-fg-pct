// Shot Explorer calculations, ported one to one from the server function in
// 03_app/app.R. No DOM or fetch calls, so the parity tests can run it in Node.
import { approxOrdered, invLogit, qlogis, quantiles } from "./stats";
import type { CalDraws, Family, XgbModel, XptsEcdf } from "./types";
import { predictProba } from "./xgb";

export interface ShotLocation {
  x: number;
  y: number;
  family: Family;
  type: 1 | 2 | 3;
  distance: number;
  angle: number;
}

// The Shot Quality Features card.
export interface ShotInputs {
  minGame: number;
  minStint: number;
  shotClock: number;
  defenderDistLevel: number;
  dribbleLevel: number;
  is2ndChance: boolean;
  isFastbreak: boolean;
  isFromTurnover: boolean;
  isDunk: boolean;
}

export const COURT = { xMin: -25, xMax: 25, yMin: -5.25, yMax: 41.75 };

// classify_shot() in app.R
export function classifyShot(x: number, y: number): { family: Family; type: 1 | 2 | 3; dist: number } {
  const dist = Math.sqrt(x * x + y * y);
  const isCorner = Math.abs(x) >= 22;
  const threePtDist = isCorner ? 22 : 23.75;
  if (dist <= 4) return { family: "rim", type: 1, dist };
  if (dist >= threePtDist) return { family: "j3", type: 3, dist };
  return { family: "j2", type: 2, dist };
}

// observeEvent(input$court_click) in app.R
export function locateShot(clickX: number, clickY: number): ShotLocation {
  const x = Math.max(COURT.xMin, Math.min(COURT.xMax, clickX));
  const y = Math.max(COURT.yMin, Math.min(COURT.yMax, clickY));
  const info = classifyShot(x, y);
  return {
    x, y,
    family: info.family,
    type: info.type,
    distance: info.dist,
    angle: (Math.abs(Math.atan2(x, y)) * 180) / Math.PI,
  };
}

export interface Baseline {
  xfg: number;
  xfgLogit: number;
  type: 1 | 2 | 3;
  family: Family;
}

// xfg_baseline() in app.R. The rim model is the only one with an is_dunk
// feature; predictProba() reads just the features its model names.
export function xfgBaseline(model: XgbModel, loc: ShotLocation, inputs: ShotInputs): Baseline {
  const xfg = predictProba(model, {
    shot_distance: loc.distance,
    shot_angle: loc.angle,
    player_min_game: inputs.minGame,
    player_min_stint: inputs.minStint,
    shot_clock: inputs.shotClock,
    defender_dist_level: inputs.defenderDistLevel,
    dribble_level: inputs.dribbleLevel,
    is_2ndchance: inputs.is2ndChance ? 1 : 0,
    is_fastbreak: inputs.isFastbreak ? 1 : 0,
    is_fromturnover: inputs.isFromTurnover ? 1 : 0,
    is_dunk: inputs.isDunk ? 1 : 0,
  });
  return { xfg, xfgLogit: qlogis(xfg), type: loc.type, family: loc.family };
}

// get_grade_color() in app.R: red (0) to yellow (50) to green (100).
export function gradeColor(pct: number): string {
  let r: number;
  let g: number;
  if (pct <= 50) {
    r = 255;
    g = Math.trunc(255 * (pct / 50));
  } else {
    r = Math.trunc(255 * (1 - (pct - 50) / 50));
    g = 255;
  }
  return `rgb(${r}, ${g}, 40)`;
}

// get_letter_grade() in app.R
export function letterGrade(pct: number): string {
  if (pct >= 90) return "A";
  if (pct >= 80) return "B";
  if (pct >= 70) return "C";
  if (pct >= 60) return "D";
  return "F";
}

export interface PosteriorResults {
  xfg_raw: number;
  xfg_pct: number;
  point_value: number;
  baseline_median: number;
  baseline_q05: number;
  baseline_q95: number;
  final_median: number;
  final_q05: number;
  final_q95: number;
  xpts_median: number;
  xpts_q05: number;
  xpts_q95: number;
  shot_grade: number;
  letter_grade: string;
  grade_color: string;
  shooter_median: number;
  shooter_q05: number;
  shooter_q95: number;
  defender_median: number;
  defender_q05: number;
  defender_q95: number;
  defteam_median: number;
  defteam_q05: number;
  defteam_q95: number;
  has_shooter: boolean;
  has_defender: boolean;
  has_defteam: boolean;
}

export interface PosteriorArgs {
  base: Baseline;
  cal: CalDraws;
  ecdf: XptsEcdf;
  // Random-effect draws for the family of the shot; null when no actor is selected.
  shooter: Float32Array | null;
  defender: Float32Array | null;
  defteam: Float32Array | null;
}

// posterior_results() in app.R
export function posteriorResults({ base, cal, ecdf, shooter, defender, defteam }: PosteriorArgs): PosteriorResults {
  const fam = base.family;
  const pointValue = base.type === 3 ? 3 : 2;
  const calInt = cal.intercept[fam];
  const calSlope = cal.slope[fam];
  const n = calInt.length;

  const pBase = new Float64Array(n);
  const pFull = new Float64Array(n);
  const shooterPp = new Float64Array(n);
  const defenderPp = new Float64Array(n);
  const defteamPp = new Float64Array(n);
  for (let i = 0; i < n; i++) {
    const etaBase = calInt[i] + calSlope[i] * base.xfgLogit;
    const s = shooter ? shooter[i] : 0;
    const d = defender ? defender[i] : 0;
    const t = defteam ? defteam[i] : 0;
    const p0 = invLogit(etaBase);
    pBase[i] = p0;
    pFull[i] = invLogit(etaBase + s + d + t);
    // Isolated impacts: percentage-point change from the baseline.
    shooterPp[i] = (invLogit(etaBase + s) - p0) * 100;
    defenderPp[i] = (invLogit(etaBase + d) - p0) * 100;
    defteamPp[i] = (invLogit(etaBase + t) - p0) * 100;
  }

  const probs = [0.05, 0.5, 0.95];
  const [baseQ05, baseMed, baseQ95] = quantiles(pBase, probs);
  const [fullQ05, fullMed, fullQ95] = quantiles(pFull, probs);
  const [shQ05, shMed, shQ95] = quantiles(shooterPp, probs);
  const [dfQ05, dfMed, dfQ95] = quantiles(defenderPp, probs);
  const [dtQ05, dtMed, dtQ95] = quantiles(defteamPp, probs);

  // Shot grade: percentile of expected points against the league distribution.
  const xptsMed = fullMed * pointValue;
  const shotPctile = Math.round(approxOrdered(ecdf.x, ecdf.p, xptsMed) * 100);

  return {
    xfg_raw: base.xfg,
    xfg_pct: base.xfg * 100,
    point_value: pointValue,
    baseline_median: baseMed * 100,
    baseline_q05: baseQ05 * 100,
    baseline_q95: baseQ95 * 100,
    final_median: fullMed * 100,
    final_q05: fullQ05 * 100,
    final_q95: fullQ95 * 100,
    xpts_median: xptsMed,
    xpts_q05: fullQ05 * pointValue,
    xpts_q95: fullQ95 * pointValue,
    shot_grade: shotPctile,
    letter_grade: letterGrade(shotPctile),
    grade_color: gradeColor(shotPctile),
    shooter_median: shMed,
    shooter_q05: shQ05,
    shooter_q95: shQ95,
    defender_median: dfMed,
    defender_q05: dfQ05,
    defender_q95: dfQ95,
    defteam_median: dtMed,
    defteam_q05: dtQ05,
    defteam_q95: dtQ95,
    has_shooter: shooter !== null,
    has_defender: defender !== null,
    has_defteam: defteam !== null,
  };
}
