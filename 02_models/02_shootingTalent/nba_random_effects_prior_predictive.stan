// nba_xfg_v3_prior_predictive.stan
// =========================================================================
// Prior predictive model — NO likelihood, only generated quantities.
//
// Draws parameters from the same priors as the full model, then simulates
// binary outcomes y_rep for each shot. Comparing the distribution of
// y_rep summaries to observed data tells us whether our priors encode
// reasonable beliefs about NBA shooting before seeing any data.
//
// Usage:  Compile this model, then call $sample() with fixed_param = TRUE
//         (no MCMC needed — just forward sampling from priors).
//         Alternatively, call $sample() normally with 0 warmup.
// =========================================================================

data {
  int<lower=1> N;
  vector[N] xfg_logit;                            // XGBoost baseline (logit)
  array[N] int<lower=1, upper=3> shot_type;        // 1=rim, 2=j2, 3=j3

  int<lower=1> J_player;
  array[N] int<lower=1, upper=J_player> player;
  matrix[J_player, 3] log_fga_rate;                // volume prior info

  int<lower=1> J_defender;
  array[N] int<lower=1, upper=J_defender> defender;

  int<lower=1> J_defteam;
  array[N] int<lower=1, upper=J_defteam> defteam;

  // Observed data for comparison (not used in likelihood — just passed through)
  array[N] int<lower=0, upper=1> y;
}

generated quantities {
  // ── Draw parameters from priors ──
  vector[3] cal_intercept;
  vector[3] cal_slope_raw;
  vector[3] cal_slope;
  vector[3] mu_volume;
  vector[3] sigma_player;
  vector[3] sigma_defender;
  vector[3] sigma_defteam;

  // Random effect draws
  matrix[J_player, 3] a_player;
  matrix[J_defender, 3] a_defender;
  matrix[J_defteam, 3] a_defteam;

  // Simulated outcomes
  array[N] int<lower=0, upper=1> y_rep;

  // Summary statistics for easy comparison
  real fg_pct_rep;                    // overall simulated FG%
  vector[3] fg_pct_by_type_rep;      // per shot-family simulated FG%
  real fg_pct_obs;                    // observed FG% (passed through)
  vector[3] fg_pct_by_type_obs;

  // Per-shot linear predictor (for distribution checks)
  vector[N] eta_rep;
  vector[N] p_rep;

  // ── Sample from priors ──
  for (t in 1:3) {
    cal_intercept[t] = normal_rng(0, 0.25);
    cal_slope_raw[t] = normal_rng(1, 0.25);
    cal_slope[t] = abs(cal_slope_raw[t]);  // enforce positivity
    mu_volume[t] = normal_rng(0, 0.3);
  }

  // Half-normal SDs (draw normal, take abs)
  sigma_player[1]   = abs(normal_rng(0, 0.30));
  sigma_player[2]   = abs(normal_rng(0, 0.30));
  sigma_player[3]   = abs(normal_rng(0, 0.25));
  sigma_defender[1] = abs(normal_rng(0, 0.20));
  sigma_defender[2] = abs(normal_rng(0, 0.20));
  sigma_defender[3] = abs(normal_rng(0, 0.20));
  sigma_defteam[1]  = abs(normal_rng(0, 0.12));
  sigma_defteam[2]  = abs(normal_rng(0, 0.10));
  sigma_defteam[3]  = abs(normal_rng(0, 0.06));

  // Random effects from implied priors
  for (t in 1:3) {
    for (j in 1:J_player) {
      a_player[j, t] = mu_volume[t] * log_fga_rate[j, t]
                        + sigma_player[t] * normal_rng(0, 1);
    }
    for (j in 1:J_defender) {
      a_defender[j, t] = sigma_defender[t] * normal_rng(0, 1);
    }
    for (j in 1:J_defteam) {
      a_defteam[j, t] = sigma_defteam[t] * normal_rng(0, 1);
    }
  }

  // ── Generate fake outcomes ──
  {
    int count_rep = 0;
    array[3] int count_by_type_rep = {0, 0, 0};
    array[3] int n_by_type = {0, 0, 0};
    int count_obs = 0;
    array[3] int count_by_type_obs = {0, 0, 0};

    for (i in 1:N) {
      int t = shot_type[i];
      eta_rep[i] = cal_intercept[t] + cal_slope[t] * xfg_logit[i]
                   + a_player[player[i], t]
                   + a_defender[defender[i], t]
                   + a_defteam[defteam[i], t];
      p_rep[i] = inv_logit(eta_rep[i]);
      y_rep[i] = bernoulli_logit_rng(eta_rep[i]);

      // Accumulate summaries
      count_rep += y_rep[i];
      count_by_type_rep[t] += y_rep[i];
      n_by_type[t] += 1;
      count_obs += y[i];
      count_by_type_obs[t] += y[i];
    }

    fg_pct_rep = count_rep * 1.0 / N;
    fg_pct_obs = count_obs * 1.0 / N;
    for (t in 1:3) {
      if (n_by_type[t] > 0) {
        fg_pct_by_type_rep[t] = count_by_type_rep[t] * 1.0 / n_by_type[t];
        fg_pct_by_type_obs[t] = count_by_type_obs[t] * 1.0 / n_by_type[t];
      } else {
        fg_pct_by_type_rep[t] = 0;
        fg_pct_by_type_obs[t] = 0;
      }
    }
  }
}
