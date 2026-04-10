// stan/nba_xfg_context_only_threeway.stan
// Context-only xFG model (NO player effects)
// Separates: rim/finish vs 2PT jumpers vs 3PT jumpers

data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;

  // shared predictors
  vector[N] D;
  vector[N] A;

  // indicators
  array[N] int<lower=0, upper=1> is_jump;   // 1=jumper, 0=rim/finish
  array[N] int<lower=0, upper=1> is_three;  // used to split 2PT jumper vs 3PT jumper
  array[N] int<lower=0, upper=1> is_dunk;   // meaningful for rim/finish
}


parameters {
  // separate intercepts by shot family (recommended)
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
  // priors
  alpha_rim ~ normal(0, 1.5);
  alpha_j2  ~ normal(0, 1.5);
  alpha_j3  ~ normal(0, 1.5);

  bD_rim    ~ normal(0, 1);
  bA_rim    ~ normal(0, 1);
  bDunk_rim ~ normal(0, 1);

  bD_j2 ~ normal(0, 1);
  bA_j2 ~ normal(0, 1);

  bD_j3 ~ normal(0, 1);
  bA_j3 ~ normal(0, 1);

  // likelihood
  for (n in 1:N) {
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
        // 3PT jumper
        eta = alpha_j3
          + bD_j3 * D[n]
          + bA_j3 * A[n];
      } else {
        // 2PT jumper
        eta = alpha_j2
          + bD_j2 * D[n]
          + bA_j2 * A[n];
      }
    }

    y[n] ~ bernoulli_logit(eta);
  }
}


generated quantities {
  vector[N] log_lik;
  vector[N] p_xfg; // context-only xFG%

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
    log_lik[n] = bernoulli_logit_lpmf(y[n] | eta);
  }
}

