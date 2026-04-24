# Expected Points Above Average

**A two-stage framework for quantifying NBA shooting talent**

Standard shooting stats like FG% treat every shot the same — a wide-open dunk counts no differently than a contested fadeaway three. EPAA addresses this by first estimating how hard each shot was, then measuring who consistently beats (or falls short of) that difficulty baseline.

This project produces context-adjusted estimates of shooter skill, individual defender impact, and team defensive scheme quality, each with full uncertainty quantification.

> **Presented at [CSAS 2026]**(https://statds.org/events/csas2026/index.html)
---

## Methodology

### Shot Quality

An XGBoost model estimates the probability that a league-average player would make each shot, based purely on context: distance, angle, defender proximity, shot clock, dribbles taken before the shot, and more.

Crucially, the model never sees *who* is shooting or defending. This keeps the difficulty estimate free of player identity so that talent can be measured separately.

Three separate models are trained, one each for **rim attempts**, **mid-range jumpers**, and **three-pointers**. The features that make a shot difficult are fundamentally different across these shot types.

### Shooting Talent

A Bayesian hierarchical model (fit in Stan) estimates three parameters per entity, one for each shot type:

- **Shooter skill** — does this player make shots more often than the difficulty baseline predicts?
- **Individual defender impact** — does this defender suppress makes beyond what proximity alone would suggest?
- **Team defensive scheme** — after accounting for individual defenders, how much does the team's overall scheme matter?

The model uses random effects to model these parameters, which means players with limited shot volume get pulled toward the league average rather than producing noisy extremes.

---

## Key Findings

**Variance decomposition:** Shooter skill is the dominant source of variation across all three shot families. Individual defender impact is meaningful but smaller, and team scheme effects are smallest. Most of what a "good defense" does is already captured by having good individual defenders in the right positions.

**Year-over-year stability:** EPAA predicts next-season shooting performance with R² ≈ 0.31.



---

## Repo Structure

```
├── 01_data_pipeline.R             
├── 02_xgb_shot_difficulty.R       
├── 03_stan_random_effects.R       
├── 03a_posterior_predictive_calibration.R
├── 03b_stan_diagnostics.R         
├── 02_models/02_shootingTalent/shootingTalent.stan
├── nba_random_effects_prior_predictive.stan
└── poster_CSAS/                   # Conference poster
```

---

## Data

All data comes from the NBA Stats API (via the `hoopR` R package) for the 2024–25 season: play-by-play logs, player tracking, shot dashboard, and matchup/rotation endpoints. Approximately 200,000 field goal attempts across ~500 players, ~500 defenders, and 30 teams.

---

## Built With

- **R** — data wrangling, visualization, pipeline orchestration
- **Stan** (via `cmdstanr`) — Bayesian hierarchical modeling
- **XGBoost** — shot difficulty estimation
- **ggplot2 / patchwork** — figures and diagnostics

---

## Author

**Alex Susi**


---

## License

This project is intended for academic and research purposes.
