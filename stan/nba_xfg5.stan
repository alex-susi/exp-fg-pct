// stan/nba_xfg_final.stan
data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;

  // shared predictors
  vector[N] D;
  vector[N] A;
  array[N] int<lower=0, upper=1> is_three;

  // shot type split
  array[N] int<lower=0, upper=1> is_jump; // 1=jumper, 0=rim/finish

  // hierarchical categories
  int<lower=1> J_zone;
  array[N] int<lower=1, upper=J_zone> zone;

  int<lower=1> J_action;
  array[N] int<lower=1, upper=J_action> action;

  // player random effect (captures skill; xFG% should be computed WITHOUT this)
  int<lower=1> J_player;
  array[N] int<lower=1, upper=J_player> player;
}


parameters {
  // global intercept
  real alpha;

  // jumper coefficients
  real bD_jump;
  real bA_jump;
  real b3_jump;

  // rim coefficients (three is usually 0 here, but keep for flexibility)
  real bD_rim;
  real bA_rim;
  real b3_rim;

  // zone and action effects
  real mu_zone;
  real<lower=0> sigma_zone;
  vector[J_zone] z_zone;

  real mu_action;
  real<lower=0> sigma_action;
  vector[J_action] z_action;

  // player effect (non-centered)
  real<lower=0> sigma_player;
  vector[J_player] z_player;
}


transformed parameters{
  vector[J_zone] a_zone = mu_zone + sigma_zone * z_zone;
  vector[J_action] a_action = mu_action + sigma_action * z_action;
  vector[J_player] a_player = sigma_player * z_player; // mean 0 (player skill)
}


model {
  alpha ~ normal(0, 1.5);

  // mildly regularizing priors
  bD_jump ~ normal(0, 1);
  bA_jump ~ normal(0, 1);
  b3_jump ~ normal(0, 1);

  bD_rim ~ normal(0, 1);
  bA_rim ~ normal(0, 1);
  b3_rim ~ normal(0, 1);

  mu_zone ~ normal(0, 0.5);
  sigma_zone ~ normal(0, 0.5);
  z_zone ~ std_normal();

  mu_action ~ normal(0, 0.5);
  sigma_action ~ normal(0, 0.5);
  z_action ~ std_normal();

  sigma_player ~ normal(0, 0.5);
  z_player ~ std_normal();

  for (n in 1:N) {
    real eta_context; // “average player” context-only (xFG%)
    real eta_skill;   // includes shooter skill

    if (is_jump[n] == 1) {
      eta_context = alpha
        + bD_jump * D[n]
        + bA_jump * A[n]
        + b3_jump * is_three[n]
        + a_zone[zone[n]]
        + a_action[action[n]];
    } else {
      eta_context = alpha
        + bD_rim * D[n]
        + bA_rim * A[n]
        + b3_rim * is_three[n]
        + a_zone[zone[n]]
        + a_action[action[n]];
    }

    eta_skill = eta_context + a_player[player[n]];
    y[n] ~ bernoulli_logit(eta_skill);
  }
}


generated quantities{
  vector[N] log_lik;
  vector[N] p_xfg;      // context-only expected make prob (average player)
  vector[N] p_skill;    // includes shooter skill

  for (n in 1:N) {
    real eta_context;
    if (is_jump[n] == 1) {
      eta_context = alpha
        + bD_jump * D[n]
        + bA_jump * A[n]
        + b3_jump * is_three[n]
        + a_zone[zone[n]]
        + a_action[action[n]];
    } else {
      eta_context = alpha
        + bD_rim * D[n]
        + bA_rim * A[n]
        + b3_rim * is_three[n]
        + a_zone[zone[n]]
        + a_action[action[n]];
    }

    p_xfg[n] = inv_logit(eta_context);
    p_skill[n] = inv_logit(eta_context + a_player[player[n]]);
    log_lik[n] = bernoulli_logit_lpmf(y[n] | logit(p_skill[n]));
  }
}
