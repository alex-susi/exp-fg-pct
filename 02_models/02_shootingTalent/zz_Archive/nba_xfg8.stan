// stan/nba_xfg_context_only_threeway_threads.stan
// Context-only xFG model (NO player effects)
// Separates: rim/finish vs 2PT jumpers vs 3PT jumpers
// Uses reduce_sum() for within-chain parallel likelihood evaluation.

functions {
  real partial_sum_lpmf(
    array[] int y_slice,
    int start, int end,
    vector D,
    vector A,
    array[] int is_jump,
    array[] int is_three,
    array[] int is_dunk,
    real alpha_rim, real alpha_j2, real alpha_j3,
    real bD_rim, real bA_rim, real bDunk_rim,
    real bD_j2, real bA_j2,
    real bD_j3, real bA_j3
  ) {
    real lp = 0;
    int N_slice = end - start + 1;

    for (i in 1:N_slice) {
      int n = start + i - 1;
      real eta;

      if (is_jump[n] == 0) {
        // rim/finish
        eta = alpha_rim
          + bD_rim * D[n]
          + bA_rim * A[n]
          + bDunk_rim * is_dunk[n];
      } else {
        // jumpers: split 2PT vs 3PT
        if (is_three[n] == 1) {
          eta = alpha_j3
            + bD_j3 * D[n]
            + bA_j3 * A[n];
        } else {
          eta = alpha_j2
            + bD_j2 * D[n]
            + bA_j2 * A[n];
        }
      }

      lp += bernoulli_logit_lpmf(y_slice[i] | eta);
    }
    return lp;
  }
}


data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;

  // shared predictors
  vector[N] D;
  vector[N] A;

  // indicators
  array[N] int<lower=0, upper=1> is_jump;   // 1=jumper, 0=rim/finish
  array[N] int<lower=0, upper=1> is_three;  // split 2PT jumper vs 3PT jumper
  array[N] int<lower=0, upper=1> is_dunk;   // meaningful for rim/finish

  // threading control
  int<lower=1> grainsize;
}


parameters {
  // separate intercepts by shot family
  real alpha_rim;
  real alpha_j2;
  real alpha_j3;

  // rim coefficients
  real bD_rim;
  real bA_rim;
  real bDunk_rim;

  // 2PT jumper coefficients
  real bD_j2;
  real bA_j2;

  // 3PT jumper coefficients
  real bD_j3;
  real bA_j3;
}


model {
  // priors (same as yours)
  alpha_rim ~ normal( 0.32, 0.25);  // ~58%
  alpha_j2  ~ normal(-0.28, 0.25);  // ~43%
  alpha_j3  ~ normal(-0.58, 0.20);  // ~36% with some wiggle

  bD_rim ~ normal(-0.25, 0.15);
  bA_rim    ~ normal(0, 1);
  bDunk_rim ~ normal(1.87, 0.35);

  bD_j2  ~ normal(-0.22, 0.15);
  bA_j2  ~ normal(-0.12, 0.15);

  bD_j3  ~ normal(-0.18, 0.15);
  bA_j3  ~ normal(-0.10, 0.15);

  // threaded likelihood
  target += reduce_sum(
    partial_sum_lpmf,
    y, grainsize,
    D, A, is_jump, is_three, is_dunk,
    alpha_rim, alpha_j2, alpha_j3,
    bD_rim, bA_rim, bDunk_rim,
    bD_j2, bA_j2,
    bD_j3, bA_j3
  );
}


generated quantities {
  // vector[N] log_lik;
  vector[N] p_xfg;

  for (n in 1:N) {
    real eta;

    if (is_jump[n] == 0) {
      eta = alpha_rim
        + bD_rim * D[n]
        + bA_rim * A[n]
        + bDunk_rim * is_dunk[n];
    } else {
      if (is_three[n] == 1) {
        eta = alpha_j3
          + bD_j3 * D[n]
          + bA_j3 * A[n];
      } else {
        eta = alpha_j2
          + bD_j2 * D[n]
          + bA_j2 * A[n];
      }
    }

    p_xfg[n] = inv_logit(eta);
    // log_lik[n] = bernoulli_logit_lpmf(y[n] | eta);
  }
}
