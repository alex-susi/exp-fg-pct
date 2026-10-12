// Shapes of every file written by 04_web_export.R, plus the decoded forms.

export type Family = "rim" | "j2" | "j3";
export const FAMILIES: Family[] = ["rim", "j2", "j3"]; // shot_type 1, 2, 3 in app.R

export type Role = "offense" | "defense" | "team";
export type ShotFilter = "all" | "j3" | "j2" | "rim";
export type Metric = "total" | "per100";
export type CiKey = "80" | "50" | "20";
export type SortKey = "epaa_desc" | "epaa_asc" | "sq_desc" | "sq_asc";
export type ActorKind = "player" | "defender" | "team";

export interface ModelIndex {
  file: string;
  n_trees: number;
  n_nodes: number;
  base_margin: number;
  feature_names: string[];
}

export interface LevelOption {
  label: string;
  level: number;
}

export interface Meta {
  generated: string;
  n_draws: number;
  families: Family[];
  cal: { file: string; blocks: string[] };
  draws: Record<ActorKind, string>;
  models: Record<Family, ModelIndex>;
  defender_dist_levels: LevelOption[];
  dribble_levels: LevelOption[];
  // Index i holds the actor with app.R index i + 1.
  players: { name: string[]; img: string[] };
  defenders: { name: string[]; img: string[]; team: (number | null)[] };
  teams: { abbr: string[]; logo: string[] };
  lb: { roles: Role[]; shots: ShotFilter[]; rows: Record<string, number> };
}

// Flattened gradient-boosted trees. `value` is the split threshold on internal
// nodes and the leaf weight on leaves (left === -1).
export interface XgbModel {
  nTrees: number;
  treeOffset: Int32Array;
  left: Int32Array;
  right: Int32Array;
  value: Float32Array;
  feature: Uint8Array;
  defaultLeft: Uint8Array;
  baseMargin: number;
  featureNames: string[];
}

// Posterior draws per shot family, in draw order.
export type FamilyDraws = Record<Family, Float32Array>;
export interface CalDraws {
  intercept: FamilyDraws;
  slope: FamilyDraws;
}

export interface XptsEcdf {
  x: number[];
  p: number[];
}

type LbQuantile = "mid" | "q10" | "q90" | "q25" | "q75" | "q40" | "q60";
export type LbNumericColumn = "shot_quality" | `${Metric}_${LbQuantile}`;

// One leaderboard / scatterplot table (get_lb_cache(role, shot) in app.R), by column.
export type LbTable = {
  role: Role;
  shot: ShotFilter;
  obs_label: string;
  idx: number[];
  label: string[];
  n_obs: number[];
  team_abbr: string[];
  img_url: string[];
  team_logo_url: string[];
} & Record<LbNumericColumn, number[]>;

export interface LbRow {
  idx: number;
  label: string;
  n_obs: number;
  shot_quality: number;
  team_abbr: string;
  img_url: string;
  team_logo_url: string;
  mid: number;
  lo: number;
  hi: number;
}

// An <img> drawn over a Plotly chart at a data position.
export interface OverlayImage {
  key: string;
  src: string;
  x?: number; // data x
  xPx?: number; // or pixels from the left edge of the plot area (negative = left margin)
  y: number;
  w: number;
  h: number;
  lo?: number; // interval drawn while this image is hovered
  hi?: number;
}
