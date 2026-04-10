// stan/nba_xfg2.stan
data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;
  vector[N] D; // standardized shot distance
}


parameters {
  real alpha;
  real beta_D;
}


model {
  alpha ~ normal(0, 1.5);
  beta_D ~ normal(0, 1);
  y ~ bernoulli_logit(alpha + beta_D * D);
}


generated quantities{
  vector[N] log_lik;
  vector[N] p_xfg;      // context-only expected make prob (average player)
  
  for (n in 1:N){
    log_lik[n] = bernoulli_logit_lpmf(y[n] | alpha + beta_D * D[n]);
    p_xfg[n] = inv_logit(alpha + beta_D * D[n]);
  }
}
