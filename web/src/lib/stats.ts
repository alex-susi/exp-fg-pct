// Statistics helpers that reproduce the R functions app.R relies on.

// R quantile(type = 7) for several probabilities from one sort.
export function quantiles(x: ArrayLike<number>, probs: number[]): number[] {
  const s = Float64Array.from(x).sort();
  const n = s.length;
  return probs.map((p) => {
    if (n === 0) return NaN;
    const h = (n - 1) * p;
    const lo = Math.floor(h);
    const hi = Math.min(lo + 1, n - 1);
    return s[lo] + (h - lo) * (s[hi] - s[lo]);
  });
}

// inv_logit() in app.R
export const invLogit = (x: number) => 1 / (1 + Math.exp(-x));

// stats::qlogis()
export const qlogis = (p: number) => Math.log(p / (1 - p));

// stats::approx(x, y, xout, rule = 2, ties = "ordered") for one value: the same
// bisection as approx1() in R, with both ends clamped.
export function approxOrdered(x: ArrayLike<number>, y: ArrayLike<number>, v: number): number {
  const n = x.length;
  if (!n || Number.isNaN(v)) return NaN;
  let i = 0;
  let j = n - 1;
  if (v < x[i]) return y[i];
  if (v > x[j]) return y[j];
  while (i < j - 1) {
    const ij = (i + j) >> 1;
    if (v < x[ij]) j = ij;
    else i = ij;
  }
  if (v === x[j]) return y[j];
  if (v === x[i]) return y[i];
  return y[i] + (y[j] - y[i]) * ((v - x[i]) / (x[j] - x[i]));
}

// stats::weighted.mean(x, w, na.rm = TRUE)
export function weightedMean(x: ArrayLike<number>, w: ArrayLike<number>): number {
  let sw = 0;
  let sx = 0;
  for (let i = 0; i < x.length; i++) {
    if (Number.isNaN(x[i])) continue;
    sw += w[i];
    sx += w[i] * x[i];
  }
  return sx / sw;
}
