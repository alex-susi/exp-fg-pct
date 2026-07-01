# Expected Points Above Average

**A two-stage framework for quantifying NBA shooting talent**

[![Dashboard](https://img.shields.io/badge/Interactive%20Dashboard-Shiny-blue)](https://alexsusi2298.shinyapps.io/exp-fg-pct/)
[![R](https://img.shields.io/badge/R-Modeling%20%7C%20Visualization-276DC3)](https://www.r-project.org/)
[![Stan](https://img.shields.io/badge/Stan-Bayesian%20Modeling-B2011D)](https://mc-stan.org/)
[![XGBoost](https://img.shields.io/badge/XGBoost-Shot%20Quality-orange)](https://xgboost.readthedocs.io/)
[![CSAS 2026](https://img.shields.io/badge/CSAS%202026-Poster%20Awardee-purple)](https://statds.org/events/csas2026/index.html)

<br>

## 01 - Overview

This project estimates **Expected Points Above Average (EPAA)**, a context-adjusted measure of NBA shooting performance. Standard shooting statistics like FG% reward every made shot and penalize every missed shot equally, regardless of difficulty. EPAA instead separates **shot difficulty** from **shot-making talent**.

The framework has two stages:

1. **Shot Quality Model:** estimate how likely a league-average player would be to make each shot based only on shot context.
2. **Bayesian Shooting Talent Model:** estimate which shooters, defenders, and team schemes consistently outperform or underperform that difficulty baseline.

The result is a set of estimates for:

* **Shooter skill**
* **Individual defender impact**
* **Team defensive scheme quality**

The **[Interactive Dashboard](https://alexsusi2298.shinyapps.io/exp-fg-pct/)** lets users explore shot quality, shooting talent, defender effects, and model outputs across NBA players and teams.

> This is an independent research / portfolio project and is not an official NBA, team, or league model. Data were collected from public NBA Stats API endpoints via the `hoopR` R package for the 2024–25 NBA regular season.

<br>

## 02 - Dashboard Preview

| Dashboard view        | Use case                                                                        |
| --------------------- | ------------------------------------------------------------------------------- |
| Shot Quality Explorer | Analyze how shot context affects expected field goal percentage                 |
<!-- | Shooter Leaderboards  | Compare players by context-adjusted shooting talent                             |
| Defender Leaderboards | Identify defenders who suppress shot-making beyond shot-quality expectations    |
| Team Scheme View      | Estimate team-level defensive effects after accounting for individual defenders |
| Model Diagnostics     | Review calibration, uncertainty, and posterior summaries                        |


<!-- ### Main Dashboard View
<!-- TODO: Replace with actual screenshot path once committed to repo -->
<!-- ![Dashboard overview placeholder](docs/screenshots/dashboard_overview_placeholder.png)
<details>
<summary><h3>Additional Dashboard Screenshots</h3></summary>
<br>
Shot Quality Explorer

<!-- TODO: Replace with actual screenshot path once committed to repo -->

<!-- ![Shot quality explorer placeholder](docs/screenshots/shot_quality_explorer_placeholder.png)

Shooter Leaderboard

<!-- TODO: Replace with actual screenshot path once committed to repo -->

<!-- ![Shooter leaderboard placeholder](docs/screenshots/shooter_leaderboard_placeholder.png)

Defender Leaderboard

<!-- TODO: Replace with actual screenshot path once committed to repo -->

<!-- ![Defender leaderboard placeholder](docs/screenshots/defender_leaderboard_placeholder.png)

Team Scheme View

<!-- TODO: Replace with actual screenshot path once committed to repo -->

<!-- ![Team scheme view placeholder](docs/screenshots/team_scheme_placeholder.png)

Model Diagnostics

<!-- TODO: Replace with actual screenshot path once committed to repo -->

<!-- ![Model diagnostics placeholder](docs/screenshots/model_diagnostics_placeholder.png)

</details>
-->
<br>

## 03 - Key Findings

1. **Shooter skill explains the largest share of variation.** Across rim attempts, mid-range jumpers, and three-pointers, player shooting talent is the dominant source of variation after accounting for shot difficulty.

2. **Individual defender impact is more meaningful on rim attempts.** Certain defenders consistently reduce opponent shot-making at the rim, but this is less evident for midrange and 3-point shots.

3. **Team scheme effects are smaller than individual defender effects.** After accounting for the shooter, shot context, and nearest defender, team-level defensive scheme have the smallest impact.

4. **EPAA has year-over-year signal.** Context-adjusted shooting estimates predict next-season shooting performance with an out-of-sample relationship of approximately `R² ≈ 0.31`.

<br>

## 04 - Methodology

### Summary

The project uses a two-stage modeling pipeline.

1. **Shot Quality Model**

   * Train XGBoost models to estimate the probability that a league-average shooter would make each field goal attempt.
   * Fit separate models for rim attempts, mid-range jumpers, and three-pointers.
   * Exclude shooter and defender identity from the feature set so the model captures difficulty rather than player talent.

2. **Bayesian Shooting Talent Model**

   * Use the shot-quality estimate as a baseline input in a Bayesian hierarchical model.
   * Estimate shooter, defender, and team defensive scheme effects by shot family.
   * Apply partial pooling so low-volume players are pulled toward league average rather than producing unstable extremes.

3. **EPAA Aggregation**

   * Convert shot-level model estimates into player-level, defender-level, and team-level summaries.
   * Preserve uncertainty using posterior draws instead of reporting only point estimates.

<br>

### Shot Quality Model

The shot-quality model estimates:

$$
xFG_i = P(\text{make}_i = 1 \mid \text{shot context}_i)
$$

where `xFG_i` is the expected field goal probability for shot `i`.

The model uses contextual features available before the shot result is known, including:

| Feature group      | Examples                                        |
| ------------------ | ----------------------------------------------- |
| Location           | Shot distance, shot angle           |
| Defensive pressure | Defender distance |
| Clock context      | Shot clock                          |
| Shot creation      | Dribbles before shot                |
| Fatigue          | Minutes played in stint, minutes played in game    |
| Shot type          | Binary indicators for Second chance, fastbreak, and off a turnover    |

Three separate XGBoost models are trained:

| Model             | Shot Type                         |
| ----------------- | ----------------------------------- |
| Rim model         | Attempts near the basket            |
| Mid-range model   | Two-point jumpers away from the rim |
| Three-point model | Above-the-break and corner threes   |

The model intentionally excludes shooter identity and defender identity. This prevents the first-stage model from learning that certain players are good or bad shooters directly. Instead, it estimates how difficult the shot was based on context alone.

<br>

### Shooting Talent Model

The second-stage model estimates whether each shot was made more or less often than expected after accounting for shot quality.

<!--A simplified version of the model is:

$$
\text{make}_i \sim \text{Bernoulli}(p_i)
$$

$$
\text{logit}(p_i) =
\text{logit}(xFG_i)

* \alpha_{\text{shooter}[i], s[i]}
* \delta_{\text{defender}[i], s[i]}
* \gamma_{\text{team}[i], s[i]}
  $$

where:

| Term                       | Meaning                                               |
| -------------------------- | ----------------------------------------------------- |
| $xFG_i$                    | Baseline make probability from the shot-quality model |
| $s[i]$                     | Shot family: rim, mid-range, or three-pointer         |
| $\alpha_{\text{shooter}}$  | Shooter skill effect                                  |
| $\delta_{\text{defender}}$ | Individual defender impact                            |
| $\gamma_{\text{team}}$     | Team defensive scheme effect                          |
-->
Each shooter, defender, and team receives separate estimates by shot family. This allows the model to distinguish, for example, a player who is elite at finishing at the rim from one who is mainly adding value as a three-point shooter.


<details>
<summary><h3><code>shootingTalent.stan</code></h3></summary>

```stan
// shootingTalent.stan
// =============================================================================
// EPAA Pipeline — Stage 2: Hierarchical Bayesian shooting talent model
//
// PURPOSE:
//   Estimate shooter skill, individual defender impact, and defensive team
//   scheme effects that are not explained by the XGBoost context baseline
//   from Stage 1. The XGBoost model answered "how hard was this shot?"; this
//   model answers "who is better or worse than that baseline, and by how much?"
//
// LIKELIHOOD:
//   Each field goal attempt y[i] ~ Bernoulli(inv_logit(eta[i])), where:
//
//     eta[i] = cal_intercept[t] + cal_slope[t] * xfg_logit[i]
//              + a_player[shooter[i],   t]
//              + a_defender[defender[i], t]
//              + a_defteam[defteam[i],  t]
//
//   xfg_logit[i] is the log-odds from XGBoost passed in as data.
//   t = shot_type[i] ∈ {1=rim, 2=j2, 3=j3}.
//
// CALIBRATION LINK f(xfg_logit):
//   cal_intercept[t] + cal_slope[t] * xfg_logit[i]
//   A flexible monotonic link that allows Stan to recalibrate the XGBoost
//   output before adding random effects. If XGBoost is perfectly calibrated
//   for a given shot family, the posterior will recover intercept ≈ 0 and
//   slope ≈ 1. In practice, j3 slope is typically ~2.0 because XGBoost
//   compresses the logit range for three-pointers (defender distance varies
//   little in the tracking data, understating how hard contested threes are).
//
// RANDOM EFFECTS (non-centered parameterization):
//   a_player[j, t]   = mu_volume[t] * log_fga_rate[j, t]
//                      + sigma_player[t] * z_player[j, t]
//   a_defender[j, t] = sigma_defender[t] * z_defender[j, t]
//   a_defteam[j, t]  = sigma_defteam[t] * z_defteam[j, t]
//
//   Non-centering is essential for HMC efficiency: it replaces the
//   correlated (mu, sigma, a) geometry with independent (sigma, z) draws,
//   dramatically reducing divergences in hierarchical models.
//
// VOLUME COVARIATE (mu_volume):
//   Players who specialize in a shot family (take a larger share of attempts
//   from that family) tend to be better at it. The log_fga_rate term captures
//   this selection effect additively in transformed parameters — keeping it
//   additive rather than multiplying sigma_player prevents a funnel geometry
//   where HMC cannot reliably estimate both parameters simultaneously.
//
// PARALLELIZATION:
//   The Bernoulli log-likelihood is evaluated via reduce_sum(), which splits
//   the N shots into chunks of size grainsize and evaluates them in parallel
//   across threads_per_chain threads. Compile with stan_threads = TRUE.
//
// Shot types: 1 = rim, 2 = j2 (mid-range), 3 = j3 (three-point)
// =============================================================================

functions {
  // Evaluates the Bernoulli log-likelihood for one chunk of shots
  // Called by reduce_sum() to parallelize the likelihood
  real partial_sum_lpmf(
    array[] int y_slice,
    int start, int end,

    // XGBoost baseline
    vector xfg_logit,

    // Grouping indices
    array[] int shot_type,    // 1..3
    array[] int player,       // 1..J_player
    array[] int defender,     // 1..J_defender
    array[] int defteam,      // 1..J_defteam

    // Calibration parameters
    vector cal_intercept,     // [3]
    vector cal_slope,         // [3]

    // Scaled random effects
    matrix a_player,          // [J_player,   3]
    matrix a_defender,        // [J_defender, 3]
    matrix a_defteam          // [J_defteam,  3]
  ) {
    real lp = 0;

    for (i in 1:(end - start + 1)) {
      int n = start + i - 1;
      int t = shot_type[n];

      // Linear predictor: calibrated XGBoost baseline + three random effects
      real eta = cal_intercept[t] + cal_slope[t] * xfg_logit[n]
                 + a_player[player[n], t]
                 + a_defender[defender[n], t]
                 + a_defteam[defteam[n], t];

      lp += bernoulli_logit_lpmf(y_slice[i] | eta);
    }
    return lp;
  }
}



data {
  int<lower=1> N;                     // total field goal attempts
  array[N] int<lower=0, upper=1> y;   // shot outcome: 1 = made, 0 = missed

  // Shot difficulty on the logit scale
  vector[N] xfg_logit;

  // 1 = rim, 2 = mid-range, 3 = 3pt
  array[N] int<lower=1, upper=3> shot_type;
  
  // Shooter random effect
  int<lower=1> J_player;
  array[N] int<lower=1, upper=J_player> player;
  
  // Standardized log1p(FGA share) by shot family — J_player × 3 matrix.
  // Column t holds the standardized log-volume rate for shot family t.
  // Used as an additive volume covariate in the player random effect.
  matrix[J_player, 3] log_fga_rate;

  // Defender random effect
  int<lower=1> J_defender;
  array[N] int<lower=1, upper=J_defender> defender;

  // Defensive team scheme random effect
  int<lower=1> J_defteam;
  array[N] int<lower=1, upper=J_defteam> defteam;

  // Chunk size for reduce_sum() parallelization.
  int<lower=1> grainsize;
}



parameters {
  // ── Calibration parameters (per shot family) ──────────────────────────────
  // These allow Stan to recalibrate the XGBoost logit output per family before
  // adding random effects. Prior centers them at a near-identity transform.
  // If XGBoost is perfectly calibrated: posterior mean ≈ (0, 1).
  vector[3] cal_intercept;
  vector<lower=0>[3] cal_slope;  // constrained positive: better xfg → more likely to make

  // ── Volume-ability coefficient (per shot family) ───────────────────────────
  // Coefficient on the standardized log-FGA-rate covariate in the player effect.
  // Positive mu_volume[t] means specialists (who take more shots of type t)
  // are on average better at type t — a selection effect, not a skill effect.
  vector[3] mu_volume;

  // ── Hierarchical SDs (per shot family) ────────────────────────────────────
  // sigma_player:   overall spread of shooter talent, after controlling for volume
  // sigma_defender: spread of individual defender impact
  // sigma_defteam:  spread of team scheme effects (smallest by prior design)
  vector<lower=0>[3] sigma_player;
  vector<lower=0>[3] sigma_defender;
  vector<lower=0>[3] sigma_defteam;

  // ── Raw standard-normal draws for non-centered parameterization ───────────
  // a = mu + sigma * z  is constructed in transformed parameters.
  // HMC samples z (mean-zero, unit-variance) rather than a directly,
  // which eliminates the funnel geometry of the centered parameterization.
  matrix[J_player,   3] z_player;
  matrix[J_defender, 3] z_defender;
  matrix[J_defteam,  3] z_defteam;
}



transformed parameters {
  // Scale raw z draws into actual random effects on the logit scale.
  // For players: shift by the volume covariate first, then add residual skill.
  matrix[J_player,   3] a_player;
  matrix[J_defender, 3] a_defender;
  matrix[J_defteam,  3] a_defteam;

  for (t in 1:3) {
    // Volume shift + residual skill
    a_player[, t]   = mu_volume[t] * log_fga_rate[, t]
                      + sigma_player[t] * z_player[, t];
    a_defender[, t] = sigma_defender[t] * z_defender[, t];
    a_defteam[, t]  = sigma_defteam[t] * z_defteam[, t];
  }
}



model {
  // ===================== PRIORS =====================

  // Calibration
  cal_intercept ~ normal(0, 0.25);    // small offset from zero
  cal_slope     ~ normal(1, 0.25);    // identity scaling

  // Player skill dispersion
  sigma_player[1] ~ normal(0, 0.30);   // rim
  sigma_player[2] ~ normal(0, 0.30);   // j2
  sigma_player[3] ~ normal(0, 0.25);   // j3

  // Individual defender impact
  sigma_defender[1] ~ normal(0, 0.20);  // rim
  sigma_defender[2] ~ normal(0, 0.20);  // j2
  sigma_defender[3] ~ normal(0, 0.20);  // j3

  // Defensive team effects
  sigma_defteam[1] ~ normal(0, 0.12);
  sigma_defteam[2] ~ normal(0, 0.10);
  sigma_defteam[3] ~ normal(0, 0.06);
  
  // Volume-ability coefficient
  mu_volume ~ normal(0, 0.3);

  // Non-centered raw draws
  to_vector(z_player)   ~ std_normal();
  to_vector(z_defender) ~ std_normal();
  to_vector(z_defteam)  ~ std_normal();
  

  // ===================== LIKELIHOOD =====================
  target += reduce_sum(partial_sum_lpmf,
                       y, grainsize,
                       xfg_logit,
                       shot_type, 
                       player, defender, defteam,
                       cal_intercept, cal_slope,
                       a_player, a_defender, a_defteam);
}
```


</details>

<br>

<br>

### Partial Pooling

The Stan model uses hierarchical random effects. This is important because many players defend or attempt only a modest number of shots in a given shot family.

Without partial pooling, low-volume players can appear extreme because of noise. With partial pooling, the model estimates each player while also learning the broader league-wide distribution of shooting and defensive effects.

In practical terms:

* High-volume players are allowed to separate more clearly from league average.
* Low-volume players are shrunk more aggressively toward league average.
* Posterior uncertainty remains wider for players with less information.

<br>

### EPAA

Expected Points Above Average measures the point value added or lost relative to the shot-quality baseline.

For a two-point shot:

$$
EPAA_i = 2 \times (\text{Observed Make}_i - xFG_i)
$$

For a three-point shot:

$$
EPAA_i = 3 \times (\text{Observed Make}_i - xFG_i)
$$

The Bayesian model refines this idea by estimating the underlying shooter, defender, and team effects rather than treating every observed make-or-miss residual as equally informative.

<br>

<!--## 05 - Model Validation Summary

The project evaluates both the first-stage shot-quality model and the second-stage Bayesian random-effects model.
-->
<!-- <details>
<summary><strong>Model validation and diagnostics table</strong></summary>

<br>

| Model component           | Check                  | Metric / output                                            | Use case                                                                     | Status |
| ------------------------- | ---------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------- | ------ |
| Shot Quality XGBoost      | Calibration            | Predicted xFG% vs observed FG% by probability bucket       | Confirms that predicted shot difficulty matches observed make rates          | PASS   |
| Shot Quality XGBoost      | Shot-family split      | Separate rim, mid-range, and three-point models            | Allows each shot type to use the features that matter most for that area     | PASS   |
| Bayesian shooting model   | R-hat / ESS            | Stan sampler diagnostics                                   | Confirms posterior draws mixed reliably                                      | PASS   |
| Bayesian shooting model   | Posterior intervals    | Shooter, defender, and team credible intervals             | Shows uncertainty around every estimated effect                              | PASS   |
| Bayesian shooting model   | Variance decomposition | Shooter, defender, and team scheme variance by shot family | Quantifies the relative importance of each source of variation               | PASS   |
| Year-over-year validation | Predictive stability   | Next-season relationship of approximately `R² ≈ 0.31`      | Tests whether EPAA contains repeatable signal rather than only fitting noise | PASS   |

</details>

<br>

Additional validation checks include:

* **Calibration plots** comparing predicted shot quality to realized field goal percentage.
* **Posterior predictive checks** for replicated make rates by shot family.
* **Sampler diagnostics** for convergence, effective sample size, and divergent transitions.
* **Year-over-year stability testing** to evaluate whether estimated shooting talent carries forward.
* **Uncertainty review** to ensure low-volume players receive appropriately wider credible intervals.

<br>

## 06 - Example Analysis

### Separating Shot Difficulty from Shot-Making

A player can have a high raw FG% because he takes easy shots, or a modest raw FG% because he takes difficult shots. EPAA is designed to separate those cases.

For example, the dashboard can be used to compare:

| Player type              | Raw shooting profile                 | EPAA interpretation                                                |
| ------------------------ | ------------------------------------ | ------------------------------------------------------------------ |
| High-efficiency finisher | Strong FG%, many rim attempts        | May grade well, but only if finishing exceeds rim-shot expectation |
| Difficult-shot creator   | Moderate FG%, many contested jumpers | May grade better than raw FG% suggests                             |
| Spot-up shooter          | High 3P%, mostly open attempts       | Value depends on whether makes exceed shot-quality baseline        |
| Defensive stopper        | Opponents shoot poorly when guarded  | Positive defender impact if suppression remains after shot context |
-->
<!-- TODO: Replace with actual screenshot path once committed to repo

![Example player comparison placeholder](docs/screenshots/example_player_comparison_placeholder.png)
-->
<br>

## 05 - Data

All data come from the NBA Stats API via the `hoopR` R package for the 2024–25 NBA regular season.

The pipeline uses:

* Play-by-play logs
* Player tracking data
* Shot dashboard data
* Matchup data
* Rotation and lineup context

<!-- The modeling dataset contains approximately:

| Entity              | Approximate count |
| ------------------- | ----------------- |
| Field goal attempts | 200,000           |
| Shooters            | 500               |
| Defenders           | 500               |
| Teams               | 30                |
-->
<br>

## 06 - Glossary

| Term                   | Meaning                                                                                                                         |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **EPAA**               | Expected Points Above Average. Estimated point value added relative to shot difficulty.                                         |
| **xFG%**               | Expected field goal percentage. The estimated probability that a league-average player would make a shot with the same context. |
| **Shot Quality**       | The modeled difficulty of a shot before knowing who took it or whether it went in.                                              |
| **Shooter Skill**      | A player’s estimated shot-making ability after controlling for shot quality.                                                    |
| **Defender Impact**    | A defender’s estimated effect on opponent shot-making after controlling for shot context.                                       |
| **Team Scheme Effect** | A team-level defensive effect after accounting for individual defenders.                                                        |
| **Partial Pooling**    | A Bayesian modeling approach that shrinks low-sample estimates toward the league average.                                       |
| **Credible Interval**  | Bayesian uncertainty interval. A 90% credible interval means 90% of posterior draws fall inside the range.                      |
| **Posterior Draws**    | Samples from the estimated distribution of model parameters. Used to summarize uncertainty.                                     |

<br>

## 07 - Repo Structure

| File / folder                                     | Purpose                                               |
| ------------------------------------------------- | ----------------------------------------------------- |
| `01_data.R`                              | Scrape, clean, and join NBA Stats API data            |
| `02_shotQuality.R`                        | Train shot-quality XGBoost models                     |
| `03_03_shootingTalent.R`                        | Fit Bayesian shooter, defender, and team scheme model |
| `03a_posterior_predictive_calibration.R`          | Run posterior predictive calibration checks           |
| `03b_stan_diagnostics.R`                          | Summarize Stan diagnostics and posterior quality      |
| `02_models/02_shootingTalent/shootingTalent.stan` | Main Bayesian hierarchical shooting model             |
| `poster_CSAS/`                                    | Conference poster materials                           |
| `docs/screenshots/`                               | Placeholder location for dashboard screenshots        |

<br>

## 08 - Running the Project

There are two common ways to run the project.

### Quickstart: Launch the Dashboard from Existing Artifacts

Use this path if the processed data and fitted model objects are already available locally.

```r
install.packages(c(
  "tidyverse",
  "hoopR",
  "xgboost",
  "cmdstanr",
  "posterior",
  "bayesplot",
  "loo",
  "ggplot2",
  "patchwork",
  "shiny",
  "plotly",
  "DT",
  "bslib"
))

shiny::runApp("app.R")
```

> Adjust the app path if the Shiny application is stored in a subfolder.

<br>

### Full Rebuild: Scrape Data, Train Models, Fit Stan Model

Use this path to rebuild the project from raw public data.

```r
install.packages(c(
  "tidyverse",
  "hoopR",
  "xgboost",
  "cmdstanr",
  "posterior",
  "bayesplot",
  "loo",
  "ggplot2",
  "patchwork",
  "shiny",
  "plotly",
  "DT",
  "bslib"
))

cmdstanr::install_cmdstan()

source("01_data_pipeline.R")
source("02_xgb_shot_difficulty.R")
source("03_stan_random_effects.R")
source("03a_posterior_predictive_calibration.R")
source("03b_stan_diagnostics.R")
```

<br>

## 09 - Limitations and Future Work

* **Single-season data.** The current version uses the 2024–25 regular season. Multi-season modeling would improve stability and make year-over-year validation more robust.
* **Shot context is limited to available public data.** Public play-by-play data captures many important variables, but more detailed tracking data could provide richer features and a better estimate of xFG%.
* **Defense is likely undervalued.** A large part of defense is shot suppression. Because this project only looks at observed field goal attempts, a defender that excels at preventing players from taking shots in the first place will be undervalued in this model.
* **Richer data needed to properly model Perimeter defense.** Because the only defender feature included in the shot quality model is `defender_distance`, the project is essentially modeling "Who is the best at guarding shots after controlling for proximity to the shooter?" This masks a defender's ability (or inability) to put himself in a position to contest the shot, which is a large part of defense.
* **Nearest defender is an imperfect label.** The closest defender is not always the player most responsible for the shot quality allowed.
* **Team scheme effects are difficult to isolate.** Scheme, personnel, and role assignments are correlated. The model separates them statistically, but some overlap is unavoidable.
* **No playoff adjustment yet.** The model is regular-season only and does not estimate whether shooting or defensive effects translate differently in playoff settings.
* **No possession-value model yet.** EPAA focuses on field goal attempts, not turnovers, fouls drawn, offensive rebounds, or passing decisions that happen before the shot.

Future extensions could include:

* Multi-season Bayesian updating.
* Player aging curves for shooting/defensive talent.
* Lineup-level offensive spacing effects.
* Defender role classification.
* Player-specific priors based on college, G League, or international data.

<br>

## 10 - References

Public data and tools:

* NBA Stats API
* `hoopR`
* Stan / `cmdstanr`
* XGBoost


<br>


## 11 - License

This project is intended for academic and research purposes.
