// Figure builder for the EPAA Scatterplot tab: output$scatter_plot in app.R.
// Headshots and logos are <img> overlays; a transparent marker trace owns hover.
import { mediaUrl } from "./format";
import { C, FONT, type Layout, type Trace } from "./plotly";
import { weightedMean } from "./stats";
import type { LbRow, Metric, OverlayImage, Role } from "./types";

function paddedRange(values: number[], padShare: number, fallback: number): [number, number] {
  let min = Infinity;
  let max = -Infinity;
  for (const v of values) {
    if (Number.isNaN(v)) continue;
    if (v < min) min = v;
    if (v > max) max = v;
  }
  let pad = padShare * (max - min);
  if (!Number.isFinite(pad) || pad === 0) pad = fallback;
  return [min - pad, max + pad];
}

// The axis ranges and the league-average line computed in output$scatter_plot.
export function scatterRanges(rows: LbRow[]): { xRange: [number, number]; yRange: [number, number]; yAvg: number } {
  const xRange = paddedRange([...rows.flatMap((r) => [r.lo, r.hi, r.mid]), 0], 0.1, 1);
  const yAvg = weightedMean(rows.map((r) => r.shot_quality), rows.map((r) => Math.max(r.n_obs, 1)));
  const yRange = paddedRange([...rows.map((r) => r.shot_quality), yAvg], 0.1, 0.05);
  return { xRange, yRange, yAvg };
}

export function scatterXTitle(role: Role, metric: Metric): string {
  const metricLabel = metric === "total" ? "Total EPAA" : "EPAA per 100 Shots";
  return role === "offense" ? `Shooting Talent (${metricLabel})` : `Defensive Talent (${metricLabel} Prevented)`;
}

// Image footprint: headshots grow with shot volume, team logos stay fixed.
function imageSizes(rows: LbRow[], role: Role): { w: number; h: number; hit: number }[] {
  if (role === "team") return rows.map(() => ({ w: 32, h: 32, hit: 34 }));
  let min = Infinity;
  let max = -Infinity;
  for (const r of rows) {
    if (r.n_obs < min) min = r.n_obs;
    if (r.n_obs > max) max = r.n_obs;
  }
  const spread = max - min;
  return rows.map((r) => {
    const s = Number.isFinite(spread) && spread > 0 ? Math.sqrt(Math.max(r.n_obs - min, 0) / spread) : 0.55;
    const w = 30 + 26 * s;
    return { w, h: 22 + 20 * s, hit: Math.max(w, 34) };
  });
}

const QUADRANTS: Record<"offense" | "defense", string[]> = {
  offense: [
    "Easier Shots,<br>Worse Shot-Making", "Easier Shots,<br>Better Shot-Making",
    "Tougher Shots,<br>Worse Shot-Making", "Tougher Shots,<br>Better Shot-Making",
  ],
  defense: [
    "Easier Shots Contested,<br>Worse Defense", "Easier Shots Contested,<br>Better Defense",
    "Tougher Shots Contested,<br>Worse Defense", "Tougher Shots Contested,<br>Better Defense",
  ],
};

export function scatterFigure(rows: LbRow[], role: Role, metric: Metric): {
  data: Trace[];
  layout: (width: number) => Layout;
  images: (width: number) => OverlayImage[];
} {
  const { xRange, yRange, yAvg } = scatterRanges(rows);
  const sizes = imageSizes(rows, role);
  const rule = { color: C.rule, dash: "dash", width: 1 };

  const data: Trace[] = [
    { type: "scatter", mode: "lines", x: [0, 0], y: yRange, line: rule, hoverinfo: "skip", showlegend: false },
    { type: "scatter", mode: "lines", x: xRange, y: [yAvg, yAvg], line: rule, hoverinfo: "skip", showlegend: false },
    {
      type: "scatter", mode: "markers",
      x: rows.map((r) => r.mid), y: rows.map((r) => r.shot_quality),
      marker: {
        size: sizes.map((s) => s.hit), color: rows.map((r) => (r.mid >= 0 ? C.good : C.bad)),
        opacity: 0.01, line: { width: 0 },
      },
      customdata: rows.map((r) => String(r.idx)),
      hoverinfo: "none", showlegend: false,
    },
  ];

  const xSpan = xRange[1] - xRange[0];
  const ySpan = yRange[1] - yRange[0];
  const qx = [xRange[0] + 0.11 * xSpan, xRange[1] - 0.11 * xSpan];
  const qy = [yRange[1] - 0.11 * ySpan, yRange[0] + 0.11 * ySpan];
  const quad = QUADRANTS[role === "offense" ? "offense" : "defense"];

  return {
    data,
    layout: (width) => {
      const wide = width >= 640;
      const margin = wide ? { l: 165, r: 35, t: 10, b: 112 } : { l: 58, r: 10, t: 8, b: 56 };
      const annotations: Layout[] = [0, 1, 2, 3].map((k) => ({
        x: wide ? qx[k % 2] : qx[k % 2] + (k % 2 ? -1 : 1) * 0.07 * xSpan,
        y: qy[k < 2 ? 0 : 1], xref: "x", yref: "y", text: quad[k], showarrow: false,
        font: { color: C.axis, size: wide ? 14 : 10 }, bgcolor: "rgba(15,15,26,0.78)",
        bordercolor: C.grid, borderpad: wide ? 4 : 2,
      }));
      if (wide) {
        annotations.push(
          { x: 0, xref: "paper", xshift: -(margin.l - 30), y: 0.5, yref: "paper",
            text: "Better Shot Quality ─────▶", showarrow: false, textangle: -90,
            font: { color: C.muted, size: 15 } },
          { x: 0.5, xref: "paper", y: 0, yref: "paper", yshift: -(margin.b - 20),
            text: "Better Shooting Talent ─────▶", showarrow: false,
            font: { color: C.muted, size: 15 } },
        );
      }
      return {
        xaxis: {
          title: { text: scatterXTitle(role, metric), font: { color: C.axis, size: wide ? 16 : 12 }, standoff: 12 },
          range: xRange, zeroline: false, gridcolor: C.grid,
          tickfont: { color: C.axis, size: wide ? 14 : 11 }, tickformat: ".1f", fixedrange: !wide,
        },
        yaxis: {
          title: {
            text: wide ? "Shot Quality (Expected Points per Shot)" : "Shot Quality (xPts per Shot)",
            font: { color: C.axis, size: wide ? 16 : 12 }, standoff: wide ? 14 : 6,
          },
          range: yRange, zeroline: false, gridcolor: C.grid,
          tickfont: { color: C.axis, size: wide ? 14 : 11 }, tickformat: ".2f", fixedrange: !wide,
        },
        margin, annotations,
        paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: FONT,
        showlegend: false, hovermode: "closest", dragmode: wide ? "zoom" : false,
      };
    },
    images: (width) => {
      const scale = width >= 640 ? 1 : 0.62;
      return rows.flatMap((r, i) => r.img_url
        ? [{
            key: String(r.idx), src: mediaUrl(r.img_url), x: r.mid, y: r.shot_quality,
            w: sizes[i].w * scale, h: sizes[i].h * scale, lo: r.lo, hi: r.hi,
          }]
        : []);
    },
  };
}
