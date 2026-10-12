// Number formats matching the sprintf() calls in app.R.

export const fixed = (v: number, digits: number) => (Number.isFinite(v) ? v.toFixed(digits) : "NA");

// sprintf("%+.1f")
export const signed = (v: number, digits = 1) => (Number.isFinite(v) ? (v >= 0 ? "+" : "") + v.toFixed(digits) : "NA");

// formatC(x, format = "d", big.mark = ",")
export const count = (v: number) => v.toLocaleString("en-US");

// Image paths from the export are relative to the site root unless absolute.
export const mediaUrl = (path: string) =>
  !path ? "" : /^https?:\/\//.test(path) ? path : `${import.meta.env.BASE_URL}${path.replace(/^\/+/, "")}`;
