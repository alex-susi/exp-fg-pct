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

