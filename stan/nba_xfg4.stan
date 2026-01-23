// stan/nba_xfg4.stan
data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;

  vector[N] D;
  vector[N] A;
  array[N] int<lower=0, upper=1> is_three;

  int<lower=1> J_zone;
  array[N] int<lower=1, upper=J_zone> zone;

  int<lower=1> J_action;
  array[N] int<lower=1, upper=J_action> action;
}
parameters {
  real alpha;
  real beta_D;
  real beta_A;
  real beta_3;

  real mu_zone;
  real<lower=0> sigma_zone;
  vector[J_zone] z_zone;

  real mu_action;
  real<lower=0> sigma_action;
  vector[J_action] z_action;
}
transformed parameters{
  vector[J_zone] a_zone = mu_zone + sigma_zone * z_zone;
  vector[J_action] a_action = mu_action + sigma_action * z_action;
}
model {
  alpha ~ normal(0, 1.5);
  beta_D ~ normal(0, 1);
  beta_A ~ normal(0, 1);
  beta_3 ~ normal(0, 1);

  mu_zone ~ normal(0, 0.5);
  sigma_zone ~ normal(0, 0.5);
  z_zone ~ std_normal();

  mu_action ~ normal(0, 0.5);
  sigma_action ~ normal(0, 0.5);
  z_action ~ std_normal();

  for (n in 1:N) {
    real eta = alpha
      + beta_D * D[n]
      + beta_A * A[n]
      + beta_3 * is_three[n]
      + a_zone[zone[n]]
      + a_action[action[n]];
    y[n] ~ bernoulli_logit(eta);
  }
}
generated quantities{
  vector[N] log_lik;
  for (n in 1:N) {
    real eta = alpha
      + beta_D * D[n]
      + beta_A * A[n]
      + beta_3 * is_three[n]
      + a_zone[zone[n]]
      + a_action[action[n]];
    log_lik[n] = bernoulli_logit_lpmf(y[n] | eta);
  }
}
