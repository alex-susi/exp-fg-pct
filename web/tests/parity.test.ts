// Parity with the Shiny app. web/scripts/parity_reference.R runs each case
// through the real server function in 03_app/app.R; these tests recompute the
// same cases with the TypeScript port from the exported data files.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { decodeActorDraws, decodeCal, decodeLbTable, drawsPath, lbPath } from "../src/lib/data";
import { lbData, lbTitle, lbXRange, scatterData, scTitle } from "../src/lib/leaderboard";
import { lbPlotHeight } from "../src/lib/leaderboardCharts";
import { scatterRanges, scatterXTitle } from "../src/lib/scatterCharts";
import { locateShot, posteriorResults, xfgBaseline, type PosteriorResults, type ShotInputs } from "../src/lib/shot";
import type { ActorKind, CiKey, Family, LbTable, Meta, Metric, Role, ShotFilter, SortKey, XptsEcdf } from "../src/lib/types";
import { decodeModel } from "../src/lib/xgb";
import reference from "./parity_reference.json";

const dataDir = join(dirname(fileURLToPath(import.meta.url)), "..", "public", "data");

function readBuffer(path: string): ArrayBuffer {
  const b = readFileSync(join(dataDir, path));
  return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); // aligned ArrayBuffer
}
const readJson = <T>(path: string) => JSON.parse(readFileSync(join(dataDir, path), "utf8")) as T;

const meta = readJson<Meta>("meta.json");
const cal = decodeCal(meta, readBuffer(meta.cal.file));
const ecdf = readJson<XptsEcdf>("xpts_ecdf.json");
const model = (family: Family) => decodeModel(meta.models[family], readBuffer(meta.models[family].file));
const draws = (kind: ActorKind, idx: number, family: Family) =>
  decodeActorDraws(meta, readBuffer(drawsPath(meta, kind, idx)))[family];
const table = (role: Role, shot: ShotFilter) => decodeLbTable(readJson<LbTable>(lbPath(role, shot)));

const level = (options: Meta["defender_dist_levels"], label: string) => {
  const hit = options.find((o) => o.label === label);
  if (!hit) throw new Error(`Unknown level: ${label}`);
  return hit.level;
};

interface ShotCase {
  name: string;
  click: { x: number; y: number };
  inputs: {
    shooter: string; defender: string; shot_clock: number; dribble_range: string; defender_dist: string;
    min_game: number; min_stint: number;
    is_fastbreak: boolean; is_fromturnover: boolean; is_2ndchance: boolean; is_dunk: boolean;
  };
  expected: Record<string, number | string | boolean | null>;
}

function runShot(c: ShotCase): { res: PosteriorResults; family: Family; distance: number; angle: number; defteam: number | null } {
  const loc = locateShot(c.click.x, c.click.y);
  const inputs: ShotInputs = {
    minGame: c.inputs.min_game,
    minStint: c.inputs.min_stint,
    shotClock: c.inputs.shot_clock,
    defenderDistLevel: level(meta.defender_dist_levels, c.inputs.defender_dist),
    dribbleLevel: level(meta.dribble_levels, c.inputs.dribble_range),
    is2ndChance: c.inputs.is_2ndchance,
    isFastbreak: c.inputs.is_fastbreak,
    isFromTurnover: c.inputs.is_fromturnover,
    isDunk: c.inputs.is_dunk,
  };
  const base = xfgBaseline(model(loc.family), loc, inputs);
  const shooterIdx = c.inputs.shooter ? Number(c.inputs.shooter) : null;
  const defenderIdx = c.inputs.defender ? Number(c.inputs.defender) : null;
  // The defensive team follows the defender, as in observeEvent(input$defender).
  const defteam = defenderIdx ? meta.defenders.team[defenderIdx - 1] : null;
  const res = posteriorResults({
    base, cal, ecdf,
    shooter: shooterIdx ? draws("player", shooterIdx, loc.family) : null,
    defender: defenderIdx ? draws("defender", defenderIdx, loc.family) : null,
    defteam: defteam ? draws("team", defteam, loc.family) : null,
  });
  return { res, family: loc.family, distance: loc.distance, angle: loc.angle, defteam };
}

const shotCases = reference.shot_cases as unknown as ShotCase[];
const byName = (name: string) => {
  const hit = shotCases.find((c) => c.name === name);
  if (!hit) throw new Error(`No reference case named ${name}`);
  return hit.expected as Record<string, number>;
};

describe("parity reference", () => {
  it("was generated from the current export", () => {
    expect(reference.export_generated).toBe(meta.generated);
  });

  // A reference in which a control was silently ignored would pass for the wrong reason.
  it("shows the user controls changing the numbers", () => {
    expect(Math.abs(byName("rim, dunk").xfg_pct - byName("rim, no actors").xfg_pct)).toBeGreaterThan(1);
    expect(Math.abs(byName("mid-range, shooter only").final_median - byName("mid-range, no actors").final_median)).toBeGreaterThan(0.5);
    expect(Math.abs(byName("mid-range, defender only").final_median - byName("mid-range, no actors").final_median)).toBeGreaterThan(0.5);
    expect(Math.abs(byName("three, above the break").xfg_pct - byName("three, off turnover only").xfg_pct)).toBeGreaterThan(0.1);
    expect(byName("mid-range, shooter only").xfg_pct).toBe(byName("mid-range, no actors").xfg_pct);
  });
});

describe("Shot Explorer", () => {
  const numeric = [
    "xfg_raw", "xfg_pct", "point_value",
    "baseline_median", "baseline_q05", "baseline_q95",
    "final_median", "final_q05", "final_q95",
    "xpts_median", "xpts_q05", "xpts_q95",
    "shooter_median", "shooter_q05", "shooter_q95",
    "defender_median", "defender_q05", "defender_q95",
    "defteam_median", "defteam_q05", "defteam_q95",
  ] as const;

  for (const c of shotCases) {
    it(c.name, () => {
      const { res, family, distance, angle, defteam } = runShot(c);
      const e = c.expected;
      expect(family).toBe(e.family);
      expect(res.point_value).toBe(e.point_value);
      expect(Math.abs(distance - (e.distance as number))).toBeLessThan(1e-9);
      expect(Math.abs(angle - (e.angle as number))).toBeLessThan(1e-9);
      expect(defteam ?? null).toBe(e.defteam_idx);
      for (const key of numeric) {
        expect(Math.abs(res[key] - (e[key] as number)), key).toBeLessThan(1e-4);
      }
      expect(res.shot_grade).toBe(e.shot_grade);
      expect(res.letter_grade).toBe(e.letter_grade);
      expect(res.grade_color).toBe(e.grade_color);
      expect(res.has_shooter).toBe(e.has_shooter);
      expect(res.has_defender).toBe(e.has_defender);
      expect(res.has_defteam).toBe(e.has_defteam);
    });
  }
});

const close = (a: number[], b: number[], tol = 1e-9) => {
  expect(a.length).toBe(b.length);
  for (let i = 0; i < a.length; i++) expect(Math.abs(a[i] - b[i])).toBeLessThan(tol);
};

describe("EPAA Leaderboard", () => {
  for (const c of reference.lb_cases) {
    const i = c.inputs;
    const role = i.lb_role as Role;
    const shot = i.lb_shot as ShotFilter;
    const metric = i.lb_xaxis as Metric;
    it(`${role} / ${shot} / ${metric} / ${i.lb_ci}% / ${i.lb_sort} / min ${i.lb_minobs} / ${i.lb_team}`, () => {
      const rows = lbData(table(role, shot), {
        metric, ci: i.lb_ci as CiKey, sort: i.lb_sort as SortKey,
        minObs: Number(i.lb_minobs), team: i.lb_team,
      });
      const e = c.expected as {
        n: number; idx: number[]; mid: number[]; lo: number[]; hi: number[];
        title: string; x_range?: number[]; plot_height?: number;
      };
      expect(rows.length).toBe(e.n);
      expect(rows.map((r) => r.idx)).toEqual(e.idx);
      close(rows.map((r) => r.mid), e.mid);
      close(rows.map((r) => r.lo), e.lo);
      close(rows.map((r) => r.hi), e.hi);
      expect(lbTitle(role, shot, metric)).toBe(e.title);
      if (e.n > 0) {
        close(lbXRange(rows), e.x_range!);
        expect(lbPlotHeight(rows.length)).toBe(e.plot_height);
      }
    });
  }
});

describe("EPAA Scatterplot", () => {
  for (const c of reference.sc_cases) {
    const i = c.inputs;
    const role = i.sc_role as Role;
    const shot = i.sc_shot as ShotFilter;
    const metric = i.sc_xaxis as Metric;
    it(`${role} / ${shot} / ${metric} / min ${i.sc_minobs} / ${i.sc_team}`, () => {
      const rows = scatterData(table(role, shot), { metric, minObs: Number(i.sc_minobs), team: i.sc_team });
      const e = c.expected as {
        n: number; idx: number[]; talent: number[]; talent_lo: number[]; talent_hi: number[];
        shot_quality: number[]; x_range: number[]; y_range: number[]; x_title: string; title: string;
      };
      expect(rows.length).toBe(e.n);
      expect(rows.map((r) => r.idx)).toEqual(e.idx);
      close(rows.map((r) => r.mid), e.talent);
      close(rows.map((r) => r.lo), e.talent_lo);
      close(rows.map((r) => r.hi), e.talent_hi);
      close(rows.map((r) => r.shot_quality), e.shot_quality);
      const { xRange, yRange } = scatterRanges(rows);
      close(xRange, e.x_range);
      close(yRange, e.y_range);
      expect(scatterXTitle(role, metric)).toBe(e.x_title);
      expect(scTitle(role, shot, metric)).toBe(e.title);
    });
  }
});
