// Figure builders for the EPAA Leaderboard tab: output$lb_axis_plot and
// output$lb_plot in app.R. The fixed axis and the scrolling body share one
// x range and one left margin so their ticks line up.
import { mediaUrl } from "./format";
import { lbXRange } from "./leaderboard";
import { C, FONT, segments, type Layout, type Trace } from "./plotly";
import type { LbRow, Metric, OverlayImage } from "./types";

export const LB_AXIS_HEIGHT = 70;

// plot_h in output$lb_plot
export const lbPlotHeight = (nRows: number) => Math.max(360, 34 * nRows + 70);

// The Shiny layout reserves 280 px for names; phones get a narrower gutter.
function gutter(width: number) {
  if (width >= 720) return { l: 280, r: 30, font: 14, logo: 22, logoGap: 26, labelGap: 46 };
  if (width >= 480) return { l: 196, r: 18, font: 12, logo: 18, logoGap: 18, labelGap: 34 };
  return { l: 138, r: 10, font: 11, logo: 0, logoGap: 0, labelGap: 8 };
}

export function lbAxisFigure(rows: LbRow[], metric: Metric): { data: Trace[]; layout: (width: number) => Layout } {
  const xRange = lbXRange(rows);
  const xTitle = metric === "per100" ? "EPAA per 100 Shots" : "Total EPAA";
  return {
    data: [{
      type: "scatter", mode: "markers", x: xRange, y: [0, 0],
      marker: { opacity: 0 }, hoverinfo: "skip", showlegend: false,
    }],
    layout: (width) => {
      const g = gutter(width);
      return {
        xaxis: {
          title: { text: xTitle, font: { color: C.axis, size: width >= 480 ? 16 : 13 } },
          range: xRange, zeroline: false, gridcolor: C.grid,
          tickfont: { color: C.axis, size: width >= 480 ? 14 : 12 }, tickformat: ".1f",
          nticks: width >= 720 ? undefined : 5, fixedrange: true,
        },
        yaxis: { visible: false, showgrid: false, zeroline: false, fixedrange: true },
        margin: { l: g.l, r: g.r, t: 5, b: 46 },
        paper_bgcolor: C.card, plot_bgcolor: C.card, font: FONT,
      };
    },
  };
}

export function lbBodyFigure(rows: LbRow[]): {
  data: Trace[];
  layout: (width: number) => Layout;
  images: (width: number) => OverlayImage[];
} {
  const n = rows.length;
  const xRange = lbXRange(rows);
  // The first row of the table is drawn at the top.
  const ypos = rows.map((_, i) => n - i);

  // Every interval is split at zero: the part below is red, the part above green.
  const neg = { x0: [] as number[], x1: [] as number[], y: [] as number[] };
  const pos = { x0: [] as number[], x1: [] as number[], y: [] as number[] };
  rows.forEach((r, i) => {
    if (r.lo < 0 && Math.min(r.hi, 0) > r.lo) {
      neg.x0.push(r.lo); neg.x1.push(Math.min(r.hi, 0)); neg.y.push(ypos[i]);
    }
    if (r.hi > 0 && r.hi > Math.max(r.lo, 0)) {
      pos.x0.push(Math.max(r.lo, 0)); pos.x1.push(r.hi); pos.y.push(ypos[i]);
    }
  });

  const data: Trace[] = [
    { type: "scatter", mode: "lines", x: [0, 0], y: [0.4, n + 0.6],
      line: { color: C.rule, dash: "dash", width: 1 }, hoverinfo: "skip", showlegend: false },
  ];
  if (neg.y.length) data.push(segments(neg.x0, neg.x1, neg.y, { color: C.bad, width: 3 }, { opacity: 0.82 }));
  if (pos.y.length) data.push(segments(pos.x0, pos.x1, pos.y, { color: C.good, width: 3 }, { opacity: 0.82 }));
  data.push({
    type: "scatter", mode: "markers",
    x: rows.map((r) => r.mid), y: ypos,
    marker: {
      size: 12, color: rows.map((r) => (r.mid >= 0 ? C.good : C.bad)), opacity: 0.96,
      line: { color: C.bg, width: 1.5 },
    },
    customdata: rows.map((r) => String(r.idx)),
    hoverinfo: "none", showlegend: false,
  });

  return {
    data,
    layout: (width) => {
      const g = gutter(width);
      return {
        xaxis: {
          range: xRange, zeroline: false, showticklabels: false, ticks: "",
          gridcolor: C.grid, tickformat: ".1f", nticks: width >= 720 ? undefined : 5, fixedrange: true,
        },
        yaxis: {
          tickmode: "array", tickvals: ypos, showticklabels: false,
          range: [0.3, n + 0.7], showgrid: false, zeroline: false, fixedrange: true,
        },
        annotations: rows.map((r, i) => ({
          x: 0, xref: "paper", xshift: -g.labelGap, y: ypos[i], yref: "y",
          text: escapeHtml(r.label), showarrow: false, xanchor: "right", yanchor: "middle",
          font: { color: C.text, size: g.font },
        })),
        margin: { l: g.l, r: g.r, t: 10, b: 10 },
        paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: FONT,
        showlegend: false, hovermode: "closest", dragmode: false,
      };
    },
    images: (width) => {
      const g = gutter(width);
      if (!g.logo) return [];
      return rows.flatMap((r, i) => r.team_logo_url
        ? [{ key: String(r.idx), src: mediaUrl(r.team_logo_url), xPx: -g.logoGap, y: ypos[i], w: g.logo, h: g.logo }]
        : []);
    },
  };
}

export const escapeHtml = (s: string) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
