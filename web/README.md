# EPAA web

Static version of the Shiny dashboard in `03_app/`, built for GitHub Pages. R still does all the
modeling; this site reads exported data files and runs in the browser, so there is no server.

The Shiny app (`03_app/app.R`) is unchanged and remains the reference. The export and the parity
reference both load it, so the numbers here cannot drift from it.

## Layout

| Path | Holds |
| --- | --- |
| `../04_web_export.R` | Loads `app.R` without launching it and writes `public/data/` and `public/epaa_img_cache/` |
| `scripts/parity_reference.R` | Runs test cases through the real Shiny server and writes `tests/parity_reference.json` |
| `tests/parity.test.ts` | Recomputes those cases with the TypeScript port and compares |
| `src/lib/xgb.ts` | XGBoost tree inference (the shot-quality model) |
| `src/lib/shot.ts` | Shot Explorer math: `classify_shot()`, `xfg_baseline()`, `posterior_results()` |
| `src/lib/leaderboard.ts` | `lb_data()` and `scatter_data()`: filtering, interval columns, sorting |
| `src/lib/*Charts.ts` | One Plotly figure builder per `renderPlotly` in `app.R` |
| `src/lib/data.ts`, `types.ts` | Fetch and decode loaders, and the shape of every exported file |
| `src/pages/*.svelte` | One file per Shiny tab |
| `src/components/*.svelte` | Chart wrapper, court, player search, hover card |

## Data files (`public/data/`, about 36 MB, committed)

| File | Contents |
| --- | --- |
| `meta.json` | Players, defenders, teams, feature levels, model index, `generated` stamp |
| `cal.bin` | Calibration draws: intercept and slope per shot family (float32) |
| `draws/<kind>/<idx>.bin` | Random-effect draws for one shooter, defender or team: rim, mid-range, three (float32, 29 KB each) |
| `xgb/<family>.bin` | The trees `predict()` uses for one shot family |
| `xpts_ecdf.json` | Expected-points percentile lookup behind the shot grade |
| `lb/<role>__<shot>.json` | Leaderboard and scatterplot table for one role and shot type |

A first visit loads `meta.json` (about 70 KB). The Shot Explorer then fetches one model and only the
draws of the players picked; the chart tabs fetch one table and Plotly.

## Commands

Run from `web/` (Node 24):

```bash
npm install
npm run dev      # local dev server
npm run check    # svelte-check, must report 0 errors and 0 warnings
npm test         # parity tests against the Shiny reference
npm run build    # production build in dist/
```

## After the model or the Shiny app changes

Run both scripts from the repo root and commit their output together:

```bash
Rscript 04_web_export.R
Rscript web/scripts/parity_reference.R
```

The first test compares the `generated` stamp in `meta.json` with the one stored in the reference.
If only the export was rerun, that test fails and the deploy stops.

## Deploy

`.github/workflows/deploy-web.yml` checks, tests, builds and publishes `web/dist` on every push to
`main` that touches `web/`. R does not run in CI. One-time setup: in the repository settings, under
Pages, set Source to "GitHub Actions".

## Differences from the Shiny app

- Tab titles have no icons.
- Sliders and menus are native browser controls. Player search ranks names by where the typed words match.
- The court is an SVG; the arrow keys also move the shot marker (1 ft, or 0.25 ft with Shift).
- Leaderboard: the axis sits inside the scrolling list so it always lines up with the rows, team logos
  are HTML overlays, and the rows cannot be drag-zoomed.
- Hover cards are centered above the point on both chart tabs.
- Role and shot type on the chart tabs are stored in the address (`#/leaderboard/defense/rim`), so a view can be shared.
- On phones the layouts stack into one column, the scatterplot drops its two arrow captions, and
  the leaderboard hides team logos.
- Random-effect draws are stored as float32. Results agree with the Shiny app to better than 1e-4
  percentage points in the parity tests.
