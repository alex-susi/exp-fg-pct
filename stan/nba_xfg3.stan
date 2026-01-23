// stan/nba_xfg3.stan
data {
  int<lower=1> N;
  array[N] int<lower=0, upper=1> y;
  vector[N] D;
  vector[N] A;
  array[N] int<lower=0, upper=1> is_three;
  array[N] int<lower=0, upper=1> is_dunk;
}


parameters {
  real alpha;
  real beta_D;
  real beta_A;
  real beta_3;
  real beta_dunk;
}


model {
  alpha  ~ normal(0, 1.5);
  beta_D ~ normal(0, 1);
  beta_A ~ normal(0, 1);
  beta_3 ~ normal(0, 1);
  y ~ bernoulli_logit(alpha + beta_D * D + beta_A * A + beta_3 * to_vector(is_three) + beta_dunk * to_vector(is_dunk));
}


generated quantities{
  vector[N] log_lik;
  vector[N] p_xfg;      // context-only expected make prob (average player)
  
  for (n in 1:N){
    log_lik[n] = bernoulli_logit_lpmf(
      y[n] | alpha + beta_D * D[n] + beta_A * A[n] + beta_3 * is_three[n]
    );
    
    p_xfg[n] = inv_logit(alpha + beta_D * D[n] + beta_A * A[n] + beta_3 * is_three[n] + beta_dunk * is_dunk[n]);
    
  }
}
