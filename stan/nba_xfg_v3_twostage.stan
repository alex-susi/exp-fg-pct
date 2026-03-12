// nba_xfg_v3_twostage.stan
// =============================================================================
// Stage 2: Hierarchical random effects on top of XGBoost xFG
//
// The XGBoost model (Stage 1) handles all context-level shot difficulty.
// This model estimates WHO is better/worse than the XGBoost baseline:
//   - Player shooting skill (by shot type)
//   - Individual defender impact (by shot type)
//   - Defensive team scheme effect (by shot type)
//
// The key input is xfg_logit = qlogis(xfg) from XGBoost.
// We model: logit(p_make) = f(xfg_logit) + player + defender + defteam
//
// f() is a flexible monotonic link: intercept + slope * xfg_logit
// This allows the Stan model to recalibrate the XGBoost output
// (e.g., if XGBoost is systematically overconfident, slope < 1).
//
// Shot types: 1 = rim, 2 = j2, 3 = j3
// Threading via reduce_sum() for speed.
// =============================================================================

functions {
  real partial_sum_lpmf(
    array[] int y_slice,
    int start, int end,

    // XGBoost baseline (logit scale)
    vector xfg_logit,

    // indices
    array[] int shot_type,    // 1..3
    array[] int player,       // 1..J_player
    array[] int defender,     // 1..J_defender
    array[] int defteam,      // 1..J_defteam

    // calibration parameters (per shot type)
    vector cal_intercept,     // [3]
    vector cal_slope,         // [3]

    // random effects (already scaled in transformed parameters)
    matrix a_player,          // [J_player,   3]
    matrix a_defender,        // [J_defender, 3]
    matrix a_defteam          // [J_defteam,  3]
  ) {
    real lp = 0;

    for (i in 1:(end - start + 1)) {
      int n = start + i - 1;
      int t = shot_type[n];

      // Calibrated XGBoost baseline + all random effects
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
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;

  // XGBoost shot difficulty (logit scale)
  vector[N] xfg_logit;

  // Shot type index
  array[N] int<lower=1, upper=3> shot_type;

  // Grouping indices
  int<lower=1> J_player;
  array[N] int<lower=1, upper=J_player> player;

  int<lower=1> J_defender;
  array[N] int<lower=1, upper=J_defender> defender;

  int<lower=1> J_defteam;
  array[N] int<lower=1, upper=J_defteam> defteam;

  int<lower=1> grainsize;
}


parameters {
  // Calibration: allow Stan to recalibrate XGBoost predictions per family
  // If XGBoost is perfectly calibrated: intercept ≈ 0, slope ≈ 1
  vector[3] cal_intercept;
  vector<lower=0>[3] cal_slope;  // positive = monotonic (better xfg → better outcome)

  // Hierarchical SDs (per shot type)
  vector<lower=0>[3] sigma_player;
  vector<lower=0>[3] sigma_defender;
  vector<lower=0>[3] sigma_defteam;

  // Raw random effects (non-centered parameterization)
  matrix[J_player,   3] z_player;
  matrix[J_defender, 3] z_defender;
  matrix[J_defteam,  3] z_defteam;
}


transformed parameters {
  matrix[J_player,   3] a_player;
  matrix[J_defender, 3] a_defender;
  matrix[J_defteam,  3] a_defteam;

  for (t in 1:3) {
    a_player[, t]   = sigma_player[t]   * z_player[, t];
    a_defender[, t]  = sigma_defender[t]  * z_defender[, t];
    a_defteam[, t]  = sigma_defteam[t]  * z_defteam[, t];
  }
}


model {
  // ===================== PRIORS =====================

  // Calibration: expect XGBoost to be well-calibrated
  cal_intercept ~ normal(0, 0.25);    // small offset
  cal_slope     ~ normal(1, 0.25);    // near-identity transform

  // Player skill dispersion
  // On logit scale, a ±0.3 effect ≈ ±5-7 pp at the midrange.
  // Half-normal with moderate scale.
  sigma_player[1] ~ normal(0, 0.40);   // rim: higher variance (dunkers vs non)
  sigma_player[2] ~ normal(0, 0.35);   // j2
  sigma_player[3] ~ normal(0, 0.35);   // j3

  // Individual defender impact
  // Smaller than player skill — most defender impact is already captured
  // by defender_distance in the XGBoost model. This captures residual:
  // defenders who contest better/worse than their physical proximity implies.
  sigma_defender[1] ~ normal(0, 0.20);  // rim: shot-blocking skill
  sigma_defender[2] ~ normal(0, 0.15);  // j2
  sigma_defender[3] ~ normal(0, 0.15);  // j3

  // Defensive team scheme effects
  // Smallest: team-level after controlling for individual defenders.
  // Captures scheme quality (rotation help, switch discipline, etc.)
  sigma_defteam[1] ~ normal(0, 0.15);
  sigma_defteam[2] ~ normal(0, 0.15);
  sigma_defteam[3] ~ normal(0, 0.15);

  // Standard normal draws for non-centered parameterization
  to_vector(z_player)   ~ std_normal();
  to_vector(z_defender)  ~ std_normal();
  to_vector(z_defteam)  ~ std_normal();


  // ===================== LIKELIHOOD =====================
  target += reduce_sum(
    partial_sum_lpmf,
    y, grainsize,
    xfg_logit,
    shot_type, player, defender, defteam,
    cal_intercept, cal_slope,
    a_player, a_defender, a_defteam
  );
}

// NOTE: generated quantities omitted for speed.
// Compute all derived quantities in R from posterior draws.
