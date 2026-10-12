// Plotly is loaded on first use, in its own chunk, so the About and Shot
// Explorer tabs never download it. The cartesian bundle covers scatter traces.

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type PlotlyLib = any;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Layout = Record<string, any>;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Trace = Record<string, any>;

let plotlyPromise: Promise<PlotlyLib> | null = null;

export function loadPlotly(): Promise<PlotlyLib> {
  plotlyPromise ??= import("plotly.js-cartesian-dist-min").then((m) => m.default ?? m);
  return plotlyPromise;
}

// Colors shared by the figure builders (the Shiny theme in app.R).
export const C = {
  card: "#0f0f1a",
  grid: "#1a1a2a",
  text: "#cfd2dc",
  axis: "#a9adba",
  muted: "#8f95a3",
  rule: "#7c8290",
  good: "#22c55e",
  bad: "#ef4444",
  bg: "#0a0a14",
};

export const FONT = { family: "Inter, system-ui, sans-serif", color: C.text };

// plotly::add_segments(): one line trace with gaps between the segments.
export function segments(x0: number[], x1: number[], y: number[], line: Trace, extra: Trace = {}): Trace {
  const xs: (number | null)[] = [];
  const ys: (number | null)[] = [];
  for (let i = 0; i < y.length; i++) {
    xs.push(x0[i], x1[i], null);
    ys.push(y[i], y[i], null);
  }
  return { type: "scatter", mode: "lines", x: xs, y: ys, line, hoverinfo: "skip", showlegend: false, ...extra };
}
