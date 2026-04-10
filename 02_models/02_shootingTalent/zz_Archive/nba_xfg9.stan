// stan/nba_xfg_threeway_threads_player_by_type.stan
// Context + player-by-shot-type random effects
// Types: 1=rim, 2=j2, 3=j3
// Uses reduce_sum() threading.

functions {
  real partial_sum_lpmf(
    array[] int y_slice,
    int start, int end,
    vector D,
    vector A,
    array[] int is_dunk,
    array[] int shot_type,     // 1..3
    array[] int player,        // 1..J_player
    // context params
    real alpha_rim, real alpha_j2, real alpha_j3,
    real bD_rim, real bA_rim, real bDunk_rim,
    real bD_j2, real bA_j2,
    real bD_j3, real bA_j3,
    // player effects
    matrix a_player            // [J_player, 3]
  ) {
    real lp = 0;
    int N_slice = end - start + 1;

    for (i in 1:N_slice) {
      int n = start + i - 1;
      real eta_context;
      int t = shot_type[n];

      if (t == 1) { // rim
        eta_context = alpha_rim
          + bD_rim * D[n]
          + bA_rim * A[n]
          + bDunk_rim * is_dunk[n];
      } else if (t == 2) { // j2
        eta_context = alpha_j2
          + bD_j2 * D[n]
          + bA_j2 * A[n];
      } else { // t == 3, j3
        eta_context = alpha_j3
          + bD_j3 * D[n]
          + bA_j3 * A[n];
      }

      // player skill for that shot type
      lp += bernoulli_logit_lpmf(y_slice[i] | eta_context + a_player[player[n], t]);
    }
    return lp;
  }
}


data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;

  vector[N] D;
  vector[N] A;

  array[N] int<lower=0, upper=1> is_dunk;

  // NEW
  int<lower=1> J_player;
  array[N] int<lower=1, upper=J_player> player;

  // NEW: 1=rim, 2=j2, 3=j3
  array[N] int<lower=1, upper=3> shot_type;

  int<lower=1> grainsize;
}


parameters {
  // context-only
  real alpha_rim;
  real alpha_j2;
  real alpha_j3;

  real bD_rim;
  real bA_rim;
  real bDunk_rim;

  real bD_j2;
  real bA_j2;

  real bD_j3;
  real bA_j3;

  // player-by-type random effects (non-centered)
  vector<lower=0>[3] sigma_player_type;   // sd for rim/j2/j3
  matrix[J_player, 3] z_player;           // iid std normal
}


transformed parameters {
  matrix[J_player, 3] a_player;
  for (t in 1:3) {
    a_player[, t] = sigma_player_type[t] * z_player[, t];
  }
}


model {
  // context priors (your informative ones)
  alpha_rim ~ normal( 0.32, 0.25);
  alpha_j2  ~ normal(-0.28, 0.25);
  alpha_j3  ~ normal(-0.58, 0.20);

  bD_rim    ~ normal(-0.25, 0.15);
  bA_rim    ~ normal(0, 1);
  bDunk_rim ~ normal(1.87, 0.35);

  bD_j2     ~ normal(-0.22, 0.15);
  bA_j2     ~ normal(-0.12, 0.15);

  bD_j3     ~ normal(-0.18, 0.15);
  bA_j3     ~ normal(-0.10, 0.15);

  // player SD priors (this is where you control “informativeness”)
  // On logit scale, per-type skill spreads are usually not huge.
  // Tighter priors reduce divergences + runtime.
  sigma_player_type[1] ~ normal(0, 0.35); // rim skill dispersion
  sigma_player_type[2] ~ normal(0, 0.35); // j2
  sigma_player_type[3] ~ normal(0, 0.35); // j3
  to_vector(z_player) ~ std_normal();

  // threaded likelihood
  target += reduce_sum(
    partial_sum_lpmf,
    y, grainsize,
    D, A, is_dunk, shot_type, player,
    alpha_rim, alpha_j2, alpha_j3,
    bD_rim, bA_rim, bDunk_rim,
    bD_j2, bA_j2,
    bD_j3, bA_j3,
    a_player
  );
}

// NOTE: intentionally no generated quantities for speed
