// stan/nba_xfg1.stan
data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;
}
parameters {
  real alpha;
}
model {
  alpha ~ normal(0, 1.5);
  y ~ bernoulli_logit(alpha);
}
generated quantities{
  vector[N] log_lik;
  real pi = inv_logit(alpha);
  for (n in 1:N) log_lik[n] = bernoulli_logit_lpmf(y[n] | alpha);
}
