## ═════════════════════════════════════════════════════════════════════════════
## 03b_stan_diagnostics.R
## NBA xFG — Stage 2: Comprehensive Model Diagnostics
##
## Run AFTER 03_stan_random_effects.R (requires fit_nba, stan_data, shots, etc.)
##
## Contents:
##   SECTION A — Prior Predictive Checks
##   SECTION B — HMC Sampler Diagnostics
##   SECTION C — Posterior Predictive Checks
##   SECTION D — Shrinkage & Partial Pooling Diagnostics
##   SECTION E — LOO-CV & Model Comparison
##   SECTION F — Residual & Calibration Diagnostics
##   SECTION G — Sensitivity Analysis (Prior Widths)
## ═════════════════════════════════════════════════════════════════════════════


## ═════════════════════════════════════════════════════════════════════════════
## 00 — SETUP  =================================================================
## ═════════════════════════════════════════════════════════════════════════════

library(dplyr)
library(readr)
library(tidyr)
library(stringr)
library(ggplot2)
library(patchwork)
library(scales)
library(cmdstanr)
library(posterior)
library(loo)

set_cmdstan_path(path = cmdstanr::cmdstan_path())
inv_logit <- function(x) 1 / (1 + exp(-x))

# Output directory for diagnostic plots
diag_dir <- "diagnostics"
if (!dir.exists(diag_dir)) dir.create(diag_dir)

# ── Check that prerequisite objects exist ──

stopifnot("fit_nba must exist (run 03_stan_random_effects.R first)" =
            exists("fit_nba"))
stopifnot("stan_data must exist" = exists("stan_data"))
stopifnot("shots must exist"     = exists("shots"))

cat("\n╔══════════════════════════════════════════════════════════════╗\n")
cat("║            STAGE 2 MODEL DIAGNOSTICS PIPELINE              ║\n")
cat("╚══════════════════════════════════════════════════════════════╝\n\n")

fit_nba <- readRDS("stan_fit_nba.rds")



## ═════════════════════════════════════════════════════════════════════════════
## SECTION A — PRIOR PREDICTIVE CHECKS  ========================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Question: Do our priors encode reasonable beliefs about NBA shooting?
## Method:   Sample parameters from priors only (no likelihood), generate
##           fake outcomes, and compare summary statistics to observed data.
##
## Key checks:
##   1. Prior-implied overall FG% should be broadly consistent with reality
##      (~45-50%), but with wide uncertainty (we don't want overly tight priors)
##   2. Prior-implied FG% by shot family should span plausible ranges
##   3. Prior-implied effect sizes shouldn't produce degenerate probabilities
##      (all 0% or all 100%)
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ SECTION A: Prior Predictive Checks ═══\n\n")

# Compile the prior predictive model
pp_model <- cmdstan_model("stan/nba_random_effects_prior_predictive.stan")

# Sample from priors only — use fixed_param for pure forward sampling
# Each "draw" is one independent prior sample
n_pp_draws <- 500

pp_fit <- pp_model$sample(data          = stan_data,
                          chains        = 1,
                          iter_warmup   = 0,
                          iter_sampling = n_pp_draws,
                          fixed_param   = TRUE,
                          refresh       = 100)


# ── A1: Prior-implied FG% distributions ──

pp_fg_overall <- pp_fit$draws("fg_pct_rep", format = "draws_matrix")[, 1]
pp_fg_by_type <- pp_fit$draws("fg_pct_by_type_rep", format = "draws_matrix")

obs_fg_overall <- mean(shots$fgm)
obs_fg_by_type <- shots %>%
  group_by(shot_family) %>%
  summarise(fg_pct = mean(fgm), .groups = "drop") %>%
  arrange(match(shot_family, c("rim", "j2", "j3")))

pp_fg_df <- tibble(draw   = rep(1:n_pp_draws, 4),
                   family = rep(c("Overall", "Rim", "J2", "J3"),
                                each = n_pp_draws),
                   fg_pct = c(as.numeric(pp_fg_overall),
                              as.numeric(pp_fg_by_type[, 1]),
                              as.numeric(pp_fg_by_type[, 2]),
                              as.numeric(pp_fg_by_type[, 3]))) %>%
  mutate(family = factor(family, levels = c("Overall", "Rim", "J2", "J3")))

obs_lines <- tibble(family = factor(c("Overall", "Rim", "J2", "J3"),
                                    levels = c("Overall", "Rim", "J2", "J3")),
                    fg_pct = c(obs_fg_overall, obs_fg_by_type$fg_pct))

p_pp_fg <- ggplot(pp_fg_df, aes(x = fg_pct)) +
  geom_histogram(bins = 40, fill = "steelblue", alpha = 0.6, color = "white") +
  geom_vline(data = obs_lines, aes(xintercept = fg_pct),
             color = "red", linewidth = 1, linetype = "dashed") +
  facet_wrap(~family, scales = "free_y", ncol = 2) +
  labs(title = "Prior Predictive: Implied FG% Distribution",
       subtitle = "Red dashed = observed FG%. Histogram = prior-implied range.",
       x = "Simulated FG%", y = "Count") +
  theme_minimal() +
  theme(strip.text = element_text(face = "bold"))

ggsave(file.path(diag_dir, "A1_prior_predictive_fg_pct.png"),
       p_pp_fg, width = 10, height = 8, dpi = 150)

cat("  A1: Prior-implied FG% — saved\n")
cat("      Observed overall FG%:", round(obs_fg_overall, 3), "\n")
cat("      Prior 90% interval:  [",
    round(quantile(pp_fg_overall, 0.05), 3), ",",
    round(quantile(pp_fg_overall, 0.95), 3), "]\n")


# ── A2: Prior-implied p(make) distribution across shots ──
# Check for degenerate probabilities (p near 0 or 1)

pp_p_draws <- pp_fit$draws("p_rep", format = "draws_matrix")

# Sample a few prior draws and plot p distribution
set.seed(42)
sample_draws <- sample(1:n_pp_draws, min(8, n_pp_draws))
pp_p_df <- bind_rows(lapply(sample_draws, function(d) {
  tibble(draw = d, p = as.numeric(pp_p_draws[d, ]))
}))

p_pp_pdist <- ggplot(pp_p_df, aes(x = p)) +
  geom_histogram(bins = 50, fill = "steelblue", alpha = 0.6, color = "white") +
  facet_wrap(~paste("Draw", draw), ncol = 4) +
  labs(title = "Prior Predictive: Implied P(make) Across Shots",
       x = "P(make)", y = "Count") +
  theme_minimal()

ggsave(file.path(diag_dir, "A2_prior_predictive_p_distribution.png"),
       p_pp_pdist, width = 12, height = 6, dpi = 150)

# Fraction of shots with extreme probabilities
extreme_frac <- mean(pp_p_draws < 0.01 | pp_p_draws > 0.99, na.rm = TRUE)
cat("  A2: Fraction of prior-implied p in (0, 0.01) ∪ (0.99, 1):",
    round(extreme_frac * 100, 2), "%\n")
cat("      (should be small — <5% is good, >20% suggests priors too wide)\n")


# ── A3: Prior-implied effect size distributions ──
# What range of player/defender effects do the priors imply?

pp_sigma_player   <- pp_fit$draws("sigma_player",   format = "draws_matrix")
pp_sigma_defender <- pp_fit$draws("sigma_defender", format = "draws_matrix")
pp_sigma_defteam  <- pp_fit$draws("sigma_defteam",  format = "draws_matrix")

sigma_prior_df <- bind_rows(tibble(entity = "Player",
                                   family = "rim", 
                                   sigma  = as.numeric(pp_sigma_player[, 1])),
                            tibble(entity = "Player",
                                   family = "j2",
                                   sigma  = as.numeric(pp_sigma_player[, 2])),
                            tibble(entity = "Player",   
                                   family = "j3",  
                                   sigma  = as.numeric(pp_sigma_player[, 3])),
                            tibble(entity = "Defender", 
                                   family = "rim", 
                                   sigma  = as.numeric(pp_sigma_defender[, 1])),
                            tibble(entity = "Defender", 
                                   family = "j2",  
                                   sigma  = as.numeric(pp_sigma_defender[, 2])),
                            tibble(entity = "Defender", 
                                   family = "j3",  
                                   sigma  = as.numeric(pp_sigma_defender[, 3])),
                            tibble(entity = "Def Team", 
                                   family = "rim", 
                                   sigma  = as.numeric(pp_sigma_defteam[, 1])),
                            tibble(entity = "Def Team", 
                                   family = "j2",  
                                   sigma  = as.numeric(pp_sigma_defteam[, 2])),
                            tibble(entity = "Def Team", 
                                   family = "j3",  
                                   sigma  = as.numeric(pp_sigma_defteam[, 3])))

p_prior_sigma <- ggplot(sigma_prior_df, aes(x = sigma, fill = entity)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~family, ncol = 3) +
  labs(title = "Prior Distributions of Hierarchical SDs",
       subtitle = "Half-normal priors",
       x = "σ (logit scale)", fill = NULL) +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave(file.path(diag_dir, "A3_prior_sigma_distributions.png"),
       p_prior_sigma, width = 12, height = 5, dpi = 150)

cat("  A3: Prior sigma distributions — saved\n\n")


# ── A4: Prior-implied SPREAD across defenders and teams ──
# For each prior draw, extract the full vector of defender/team effects
# and summarize: what's the implied best-to-worst gap in opponent FG%?

family_labels  <- c("rim", "j2", "j3")
family_base_fg <- c(0.65, 0.42, 0.36)  # approx league-average FG% by family

# Helper: logit-scale effect → approximate pp impact at a given baseline
logit_to_pp <- function(effect, base_p) {
  inv_logit(qlogis(base_p) + effect) - base_p
}

spread_summaries <- list()

# Extract full draws matrix ONCE (avoid re-calling pp_fit$draws inside loop)
pp_draws_mat <- pp_fit$draws(format = "draws_matrix")

for (d in 1:n_pp_draws) {
  for (t in 1:3) {
    fam    <- family_labels[t]
    base_p <- family_base_fg[t]

    # Defender effects for this draw (vector of length J_defender)
    def_cols <- paste0("a_defender[", 1:stan_data$J_defender, ",", t, "]")
    def_effs <- as.numeric(pp_draws_mat[d, def_cols])

    # Defensive team effects (vector of length J_defteam)
    dt_cols <- paste0("a_defteam[", 1:stan_data$J_defteam, ",", t, "]")
    dt_effs <- as.numeric(pp_draws_mat[d, dt_cols])

    # Player effects (for matchup checks)
    pl_cols <- paste0("a_player[", 1:stan_data$J_player, ",", t, "]")
    pl_effs <- as.numeric(pp_draws_mat[d, pl_cols])

    spread_summaries[[length(spread_summaries) + 1]] <- tibble(
      draw        = d,
      family      = fam,
      # Defender spread (logit scale)
      def_sd      = sd(def_effs),
      def_range   = max(def_effs) - min(def_effs),
      def_best    = min(def_effs),          # most negative = best defender
      def_worst   = max(def_effs),
      # → pp impact of best/worst defender
      def_best_pp  = logit_to_pp(min(def_effs), base_p) * 100,
      def_worst_pp = logit_to_pp(max(def_effs), base_p) * 100,
      # Defensive team spread
      dt_sd       = sd(dt_effs),
      dt_range    = max(dt_effs) - min(dt_effs),
      dt_best_pp  = logit_to_pp(min(dt_effs), base_p) * 100,
      dt_worst_pp = logit_to_pp(max(dt_effs), base_p) * 100,
      # Player spread (for matchup context)
      pl_best     = max(pl_effs),
      pl_worst    = min(pl_effs)
    )
  }
}

spread_df <- bind_rows(spread_summaries)

# Defender implied opponent FG% gap (best minus worst, in pp)
def_gap_df <- spread_df %>%
  mutate(def_gap_pp = def_worst_pp - def_best_pp,
         dt_gap_pp  = dt_worst_pp - dt_best_pp)

p_def_spread <- ggplot(def_gap_df, aes(x = def_gap_pp)) +
  geom_histogram(bins = 40, fill = "#e74c3c", alpha = 0.6, color = "white") +
  facet_wrap(~toupper(family), scales = "free_x") +
  labs(title = "Prior-Implied Spread: Best vs Worst Individual Defender",
       subtitle = "Gap in opponent FG%",
       x = "Best-to-worst defender gap (percentage points)", y = "Count") +
  theme_minimal()

ggsave(file.path(diag_dir, "A4_prior_defender_spread.png"),
       p_def_spread, width = 12, height = 5, dpi = 150)

cat("  A4: Prior-implied defender spread\n")
for (fam in family_labels) {
  gap_vals <- def_gap_df %>% filter(family == fam) %>% pull(def_gap_pp)
  cat("      ", toupper(fam), "— best-to-worst gap 90% interval: [",
      round(quantile(gap_vals, 0.05), 1), ",",
      round(quantile(gap_vals, 0.95), 1), "] pp\n")
}


# ── A5: Defensive team spread ──
p_dt_spread <- ggplot(def_gap_df, aes(x = dt_gap_pp)) +
  geom_histogram(bins = 40, fill = "#8e44ad", alpha = 0.6, color = "white") +
  facet_wrap(~toupper(family), scales = "free_x") +
  labs(title = "Prior-Implied Spread: Best vs Worst Defensive Team Scheme",
       subtitle = "Gap in opponent FG%",
       x = "Best-to-worst team gap (percentage points)", y = "Count") +
  theme_minimal()

ggsave(file.path(diag_dir, "A5_prior_defteam_spread.png"),
       p_dt_spread, width = 12, height = 5, dpi = 150)

cat("  A5: Prior-implied defensive team spread\n")
for (fam in family_labels) {
  gap_vals <- def_gap_df %>% filter(family == fam) %>% pull(dt_gap_pp)
  cat("      ", toupper(fam), "— best-to-worst gap 90% interval: [",
      round(quantile(gap_vals, 0.05), 1), ",",
      round(quantile(gap_vals, 0.95), 1), "] pp\n")
}


# ── A6: Extreme matchup scenarios ──
# What FG% does the prior imply for:
#   (a) Best shooter + worst defender + worst def team (ceiling)
#   (b) Worst shooter + best defender + best def team (floor)
#   (c) Average shooter + best defender (isolated defender impact)
# These should produce non-degenerate, basketball-plausible probabilities.

matchup_df <- spread_df %>%
  mutate(base_p = family_base_fg[match(family, family_labels)]) %>%
  rowwise() %>%
  mutate(
    # Extreme ceiling: best shooter + worst defender + worst def team
    eta_ceiling = qlogis(base_p) + pl_best + def_worst +
      logit_to_pp(0, 0) * 0,  # placeholder — compute properly:
    p_ceiling   = inv_logit(qlogis(base_p) + pl_best + def_worst),

    # Extreme floor: worst shooter + best defender + best def team
    p_floor     = inv_logit(qlogis(base_p) + pl_worst + def_best),

    # Isolated: average shooter + best individual defender
    p_best_def  = inv_logit(qlogis(base_p) + def_best),

    # Isolated: average shooter + worst individual defender
    p_worst_def = inv_logit(qlogis(base_p) + def_worst)
  ) %>%
  ungroup()

matchup_long <- matchup_df %>%
  select(draw, family, p_ceiling, p_floor, p_best_def, p_worst_def) %>%
  pivot_longer(cols = starts_with("p_"),
               names_to = "scenario", values_to = "fg_pct") %>%
  mutate(scenario = recode(scenario,
    p_ceiling   = "Best shooter +\nworst defense",
    p_floor     = "Worst shooter +\nbest defense",
    p_best_def  = "Avg shooter vs\nbest defender",
    p_worst_def = "Avg shooter vs\nworst defender"
  ))

p_matchup <- ggplot(matchup_long, aes(x = fg_pct, fill = scenario)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~toupper(family), ncol = 3) +
  scale_x_continuous(labels = percent_format(), limits = c(0, 1)) +
  labs(title = "Prior-Implied Extreme Matchup Scenarios",
       subtitle = "No density pileup at 0% or 100% = priors are plausible.",
       x = "Implied FG%", fill = NULL) +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave(file.path(diag_dir, "A6_prior_extreme_matchups.png"),
       p_matchup, width = 14, height = 6, dpi = 150)

cat("  A6: Prior-implied extreme matchups\n")
for (fam in family_labels) {
  ceil <- matchup_df %>% filter(family == fam) %>% pull(p_ceiling)
  flr  <- matchup_df %>% filter(family == fam) %>% pull(p_floor)
  cat("      ", toupper(fam), "— ceiling 90%: [",
      round(quantile(ceil, 0.05), 2), ",", round(quantile(ceil, 0.95), 2),
      "]  floor 90%: [",
      round(quantile(flr, 0.05), 2), ",", round(quantile(flr, 0.95), 2), "]\n")
}


# ── A7: Defender effect distribution (marginal across players) ──
# For a handful of prior draws, plot the full distribution of defender effects
# to check shape and tail behavior

set.seed(99)
sample_draws_def <- sample(1:n_pp_draws, min(6, n_pp_draws))
def_eff_samples <- list()

for (d in sample_draws_def) {
  for (t in 1:3) {
    def_cols <- paste0("a_defender[", 1:stan_data$J_defender, ",", t, "]")
    effs <- as.numeric(pp_draws_mat[d, def_cols])
    def_eff_samples[[length(def_eff_samples) + 1]] <- tibble(
      draw   = d,
      family = family_labels[t],
      effect = effs,
      pp_impact = logit_to_pp(effs, family_base_fg[t]) * 100
    )
  }
}

def_eff_df <- bind_rows(def_eff_samples)

p_def_marginal <- ggplot(def_eff_df, aes(x = pp_impact)) +
  geom_histogram(bins = 40, fill = "#e74c3c", alpha = 0.6, color = "white") +
  facet_grid(paste("Draw", draw) ~ toupper(family), scales = "free") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "grey40") +
  labs(title = "Prior-Implied Defender Effect Distributions on Opponent FG%",
       subtitle = "Each row = one prior draw",
       x = "Defender effect (pp impact on opponent FG%)", y = "Count") +
  theme_minimal() +
  theme(strip.text = element_text(size = 8))

ggsave(file.path(diag_dir, "A7_prior_defender_marginal.png"),
       p_def_marginal, width = 12, height = 10, dpi = 150)

cat("  A7: Defender marginal effect distributions — saved\n\n")



## ═════════════════════════════════════════════════════════════════════════════
## SECTION B — HMC SAMPLER DIAGNOSTICS  ========================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Checks:
##   1. Divergent transitions (should be 0)
##   2. Max treedepth saturation
##   3. E-BFMI (energy Bayesian fraction of missing information)
##   4. R-hat for all parameters (should be < 1.01)
##   5. Effective sample size (bulk & tail ESS)
##   6. Trace plots for key parameters
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ SECTION B: HMC Sampler Diagnostics ═══\n\n")

# ── B1: High-level sampler diagnostics ──
diag_summary <- fit_nba$diagnostic_summary()

cat("  Divergent transitions per chain: ",
    paste(diag_summary$num_divergent, collapse = ", "), "\n")
cat("  Max treedepth hits per chain:    ",
    paste(diag_summary$num_max_treedepth, collapse = ", "), "\n")
cat("  E-BFMI per chain:               ",
    paste(round(diag_summary$ebfmi, 3), collapse = ", "), "\n")

# Flag issues
n_divergent <- sum(diag_summary$num_divergent)
if (n_divergent > 0) {
  cat("  ⚠️  ", n_divergent, "total divergent transitions!\n")
  cat("      Consider: increase adapt_delta, reparameterize, or check priors\n")
} else {
  cat("  ✓ No divergent transitions\n")
}

low_ebfmi <- which(diag_summary$ebfmi < 0.3)
if (length(low_ebfmi) > 0) {
  cat("  ⚠️  Low E-BFMI on chain(s):", paste(low_ebfmi, collapse = ", "), "\n")
  cat("      May indicate problematic posterior geometry\n")
} else {
  cat("  ✓ E-BFMI adequate on all chains\n")
}


# ── B2: R-hat and ESS for all monitored parameters ──
all_summary <- fit_nba$summary()

rhat_bad <- all_summary %>% filter(rhat > 1.01)
ess_low  <- all_summary %>% filter(ess_bulk < 100 | ess_tail < 100)

cat("\n  R-hat > 1.01:     ", nrow(rhat_bad), "parameters\n")
if (nrow(rhat_bad) > 0) {
  cat("  ⚠️  Worst offenders:\n")
  rhat_bad %>%
    arrange(desc(rhat)) %>%
    head(10) %>%
    select(variable, mean, rhat, ess_bulk, ess_tail) %>%
    mutate(across(where(is.numeric), ~round(., 3))) %>%
    print()
}

cat("  ESS_bulk < 100:   ", sum(all_summary$ess_bulk < 100, na.rm = TRUE), "\n")
cat("  ESS_tail < 100:   ", sum(all_summary$ess_tail < 100, na.rm = TRUE), "\n")

# ESS distribution plot
ess_df <- all_summary %>%
  filter(!is.na(ess_bulk)) %>%
  select(variable, ess_bulk, ess_tail) %>%
  pivot_longer(cols = c(ess_bulk, ess_tail),
               names_to = "ess_type", values_to = "ess")

p_ess <- ggplot(ess_df, aes(x = ess, fill = ess_type)) +
  geom_histogram(bins = 50, alpha = 0.6, position = "identity") +
  geom_vline(xintercept = 400, linetype = "dashed", color = "red") +
  scale_x_log10(labels = comma) +
  labs(title = "Effective Sample Size Distribution (All Parameters)",
       subtitle = "Red line = 400 (conventional minimum). Log scale.",
       x = "ESS", y = "Count", fill = NULL) +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave(file.path(diag_dir, "B2_ess_distribution.png"),
       p_ess, width = 8, height = 5, dpi = 150)


# ── B3: Trace plots for key parameters ──
key_params <- c("cal_intercept[1]", "cal_intercept[2]", "cal_intercept[3]",
                "cal_slope[1]", "cal_slope[2]", "cal_slope[3]",
                "sigma_player[1]", "sigma_player[2]", "sigma_player[3]",
                "sigma_defender[1]", "sigma_defender[2]", "sigma_defender[3]",
                "sigma_defteam[1]", "sigma_defteam[2]", "sigma_defteam[3]",
                "mu_volume[1]", "mu_volume[2]", "mu_volume[3]")

trace_draws <- fit_nba$draws(variables = key_params, format = "draws_df")

trace_long <- trace_draws %>%
  pivot_longer(cols = -c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "value")

p_trace <- ggplot(trace_long, aes(x = .iteration, y = value,
                                   color = factor(.chain))) +
  geom_line(alpha = 0.4, linewidth = 0.3) +
  facet_wrap(~parameter, scales = "free_y", ncol = 3) +
  labs(title = "Trace Plots — Key Parameters",
       subtitle = "Chains should mix well (\"fuzzy caterpillars\")",
       x = "Iteration", y = "Value", color = "Chain") +
  theme_minimal() +
  theme(legend.position = "bottom",
        strip.text = element_text(size = 7))

ggsave(file.path(diag_dir, "B3_trace_plots.png"),
       p_trace, width = 14, height = 12, dpi = 150)


# ── B4: Rank histograms for key parameters ──
# Uniformity across chains indicates good mixing
rank_draws <- fit_nba$draws(variables = key_params, format = "draws_array")

# Use posterior package's rank histogram
p_rank_plots <- list()
for (param in key_params[1:9]) {  # subset for readability
  param_draws <- rank_draws[, , param]
  ranks <- apply(param_draws, 1, rank)
  rank_df <- tibble(
    chain = rep(1:ncol(param_draws), each = nrow(param_draws)),
    rank  = as.vector(ranks)
  )
  p_rank_plots[[param]] <- ggplot(rank_df, aes(x = rank)) +
    geom_histogram(bins = 20, fill = "steelblue", alpha = 0.7, color = "white") +
    facet_wrap(~paste("Chain", chain), nrow = 1) +
    labs(title = param, x = "Rank", y = NULL) +
    theme_minimal() +
    theme(axis.text = element_text(size = 6),
          plot.title = element_text(size = 8))
}

p_rank <- wrap_plots(p_rank_plots, ncol = 1) +
  plot_annotation(title = "Rank Histograms (should be approximately uniform)")

ggsave(file.path(diag_dir, "B4_rank_histograms.png"),
       p_rank, width = 12, height = 18, dpi = 150)

cat("  B1–B4: Sampler diagnostics — saved\n\n")



## ═════════════════════════════════════════════════════════════════════════════
## SECTION C — POSTERIOR PREDICTIVE CHECKS  ====================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Compare model-predicted summaries (from posterior draws) to observed data.
## Since generated quantities were omitted from the main Stan model for speed,
## we compute y_rep in R from the posterior draws.
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ SECTION C: Posterior Predictive Checks ═══\n\n")

draws_mat <- fit_nba$draws(format = "draws_matrix")
n_draws   <- nrow(draws_mat)

# Extract posterior means of key quantities for all draws
cal_int_draws   <- draws_mat[, paste0("cal_intercept[", 1:3, "]")]
cal_slope_draws <- draws_mat[, paste0("cal_slope[", 1:3, "]")]

# For PPC, use a subsample of draws (full posterior × N shots is huge)
n_ppc <- min(200, n_draws)
ppc_idx <- sample(1:n_draws, n_ppc)

cat("  Computing posterior predictive with", n_ppc, "draws...\n")

# Pre-extract player/defender/defteam effect matrices for efficiency
# Store as list of [J × 3] matrices, one per draw
ppc_summaries <- list()

for (d_idx in seq_along(ppc_idx)) {
  d <- ppc_idx[d_idx]

  # For this draw, compute eta for all shots
  eta <- numeric(stan_data$N)
  for (i in 1:stan_data$N) {
    t <- stan_data$shot_type[i]
    ci <- cal_int_draws[d, t]
    cs <- cal_slope_draws[d, t]
    ap <- draws_mat[d, paste0("a_player[", stan_data$player[i], ",", t, "]")]
    ad <- draws_mat[d, paste0("a_defender[", stan_data$defender[i], ",", t, "]")]
    at <- draws_mat[d, paste0("a_defteam[", stan_data$defteam[i], ",", t, "]")]

    eta[i] <- ci + cs * stan_data$xfg_logit[i] + ap + ad + at
  }

  p_pred <- inv_logit(eta)
  y_rep  <- rbinom(stan_data$N, 1, p_pred)

  # Summary statistics
  ppc_summaries[[d_idx]] <- tibble(
    draw         = d,
    fg_pct_all   = mean(y_rep),
    fg_pct_rim   = mean(y_rep[stan_data$shot_type == 1]),
    fg_pct_j2    = mean(y_rep[stan_data$shot_type == 2]),
    fg_pct_j3    = mean(y_rep[stan_data$shot_type == 3]),
    mean_p       = mean(p_pred),
    sd_p         = sd(p_pred)
  )

  if (d_idx %% 50 == 0) cat("    Draw", d_idx, "/", n_ppc, "\n")
}

ppc_df <- bind_rows(ppc_summaries)

# ── C1: Posterior predictive FG% vs observed ──
obs_stats <- tibble(stat   = c("Overall", "Rim", "J2", "J3"),
                    observed = c(mean(shots$fgm),
                                 mean(shots$fgm[shots$shot_type == 1]),
                                 mean(shots$fgm[shots$shot_type == 2]),
                                 mean(shots$fgm[shots$shot_type == 3])))

ppc_long <- ppc_df %>%
  select(draw, fg_pct_all, fg_pct_rim, fg_pct_j2, fg_pct_j3) %>%
  pivot_longer(-draw, names_to = "stat", values_to = "fg_pct") %>%
  mutate(stat = recode(stat,
                       fg_pct_all = "Overall",
                       fg_pct_rim = "Rim",
                       fg_pct_j2  = "J2",
                       fg_pct_j3  = "J3"))

p_ppc_fg <- ggplot(ppc_long, aes(x = fg_pct)) +
  geom_histogram(bins = 30, fill = "steelblue", alpha = 0.6, color = "white") +
  geom_vline(data = obs_stats, aes(xintercept = observed),
             color = "red", linewidth = 1, linetype = "dashed") +
  facet_wrap(~stat, scales = "free", ncol = 2) +
  labs(title = "Posterior Predictive Check: FG% Distribution",
       subtitle = "Red dashed = observed.",
       x = "Simulated FG%", y = "Count") +
  theme_minimal() +
  theme(strip.text = element_text(face = "bold"))

ggsave(file.path(diag_dir, "C1_posterior_predictive_fg_pct.png"),
       p_ppc_fg, width = 10, height = 8, dpi = 150)


# ── C2: Bayesian p-values ──
# Fraction of posterior predictive draws where T(y_rep) > T(y_obs)
# Good calibration → p ≈ 0.5
cat("\n  Bayesian p-values (ideal ≈ 0.5):\n")
cat("Overall FG%: ", round(mean(ppc_df$fg_pct_all > obs_stats$observed[1]), 3), "\n")
cat("Rim FG%:     ", round(mean(ppc_df$fg_pct_rim > obs_stats$observed[2]), 3), "\n")
cat("J2 FG%:      ", round(mean(ppc_df$fg_pct_j2  > obs_stats$observed[3]), 3), "\n")
cat("J3 FG%:      ", round(mean(ppc_df$fg_pct_j3  > obs_stats$observed[4]), 3), "\n")


# ── C3: PPC by volume bins ──
# Does the model capture volume-dependent effects correctly?
player_vol <- shots %>%
  group_by(player_idx) %>%
  summarise(fga = n(), .groups = "drop") %>%
  mutate(vol_bin = cut(fga, breaks = c(0, 50, 150, 400, Inf),
                       labels = c("1-50", "51-150", "151-400", "400+")))

# Observed FG% by volume bin
obs_by_vol <- shots %>%
  left_join(player_vol %>% select(player_idx, vol_bin), by = "player_idx") %>%
  group_by(vol_bin) %>%
  summarise(fg_pct = mean(fgm), n = n(), .groups = "drop")

cat("\n  Observed FG% by shooter volume:\n")
print(obs_by_vol)

cat("\n  C1–C3: Posterior predictive checks — saved\n\n")



## ═════════════════════════════════════════════════════════════════════════════
## SECTION D — SHRINKAGE & PARTIAL POOLING DIAGNOSTICS  ========================
## ═════════════════════════════════════════════════════════════════════════════
##
## Key diagnostic for hierarchical models:
## Do effects shrink toward 0 appropriately as sample size decreases?
## Low-volume players should be shrunk heavily; high-volume less so.
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ SECTION D: Shrinkage & Partial Pooling ═══\n\n")

# ── D1: Shrinkage plot — raw FG% residual vs posterior effect ──
# The "residual" = observed FG% - xFG (what's left for Stan to explain)
# Posterior effect should be shrunk toward 0, especially for low-volume

player_shrinkage <- shots %>%
  group_by(player_idx, shot_family) %>%
  summarise(fga     = n(),
            fg_pct  = mean(fgm),
            mean_xfg = mean(xfg),
            raw_resid = fg_pct - mean_xfg,  # naive "skill" estimate
            .groups = "drop")

# Get posterior player effects
player_effects_post <- bind_rows(lapply(1:3, function(t) {
  fam <- c("rim", "j2", "j3")[t]
  cols <- paste0("a_player[", 1:stan_data$J_player, ",", t, "]")
  mat  <- draws_mat[, cols, drop = FALSE]
  tibble(player_idx  = 1:stan_data$J_player,
         shot_family = fam,
         post_mean   = colMeans(mat),
         post_sd     = apply(mat, 2, sd))
}))

shrinkage_df <- player_shrinkage %>%
  inner_join(player_effects_post, by = c("player_idx", "shot_family"))

shrinkage_plots <- list()
for (fam in c("rim", "j2", "j3")) {
  df_fam <- shrinkage_df %>% filter(shot_family == fam)
  shrinkage_plots[[fam]] <- ggplot(df_fam, aes(x = raw_resid, y = post_mean)) +
    geom_point(aes(size = fga, alpha = fga), color = "steelblue") +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey50") +
    geom_hline(yintercept = 0, color = "red", linewidth = 0.5) +
    scale_size_continuous(range = c(0.5, 4)) +
    scale_alpha_continuous(range = c(0.2, 0.8)) +
    labs(title = toupper(fam),
         x = "Raw FG% residual (observed - xFG)",
         y = "Posterior player effect (logit)") +
    theme_minimal() +
    theme(legend.position = "none")
}

p_shrinkage <- wrap_plots(shrinkage_plots, ncol = 3) +
  plot_annotation(
    title = "Shrinkage Diagnostic: Raw Residual vs Posterior Effect",
    subtitle = "Points below diagonal = shrinkage toward 0. Larger dots = more FGA."
  )

ggsave(file.path(diag_dir, "D1_shrinkage_plots.png"),
       p_shrinkage, width = 14, height = 5, dpi = 150)


# ── D2: Posterior SD vs volume ──
# Low-volume players should have wider posterior intervals

sd_vol_df <- shrinkage_df %>%
  filter(fga >= 5)  # exclude very tiny counts

sd_vol_plots <- list()
for (fam in c("rim", "j2", "j3")) {
  df_fam <- sd_vol_df %>% filter(shot_family == fam)
  sd_vol_plots[[fam]] <- ggplot(df_fam, aes(x = fga, y = post_sd)) +
    geom_point(alpha = 0.3, size = 1, color = "steelblue") +
    geom_smooth(method = "loess", se = FALSE, color = "red", linewidth = 0.8) +
    scale_x_log10() +
    labs(title = toupper(fam),
         x = "FGA (log scale)",
         y = "Posterior SD of player effect") +
    theme_minimal()
}

p_sd_vol <- wrap_plots(sd_vol_plots, ncol = 3) +
  plot_annotation(
    title = "Uncertainty vs Volume: Posterior SD Should Decrease with More Shots",
    subtitle = "Monotone decrease confirms proper partial pooling."
  )

ggsave(file.path(diag_dir, "D2_posterior_sd_vs_volume.png"),
       p_sd_vol, width = 14, height = 5, dpi = 150)


# ── D3: Effective shrinkage factor ──
# shrinkage_k ≈ posterior_var / (posterior_var + prior_var)
# Values near 0 = fully shrunk; near 1 = data dominates

sigma_post <- fit_nba$summary(variables = paste0("sigma_player[", 1:3, "]"))

for (t in 1:3) {
  fam <- c("rim", "j2", "j3")[t]
  sigma_hat <- sigma_post$mean[t]
  df_fam <- shrinkage_df %>% filter(shot_family == fam, fga >= 5)
  # Approximate shrinkage: 1 - (posterior_sd^2 / prior_sd^2)
  # where prior_sd ≈ sigma_hat (the hierarchical SD)
  df_fam <- df_fam %>%
    mutate(shrinkage_k = 1 - pmin(post_sd^2 / sigma_hat^2, 1))
  cat("  ", toupper(fam), "— median shrinkage factor:",
      round(median(df_fam$shrinkage_k, na.rm = TRUE), 3),
      " (0=no shrinkage, 1=full shrinkage)\n")
  cat("    Players with <50 FGA:", round(median(
    df_fam$shrinkage_k[df_fam$fga < 50], na.rm = TRUE), 3), "\n")
  cat("    Players with 50+ FGA:", round(median(
    df_fam$shrinkage_k[df_fam$fga >= 50], na.rm = TRUE), 3), "\n")
}

cat("\n  D1–D3: Shrinkage diagnostics — saved\n\n")



## ═════════════════════════════════════════════════════════════════════════════
## SECTION E — LOO-CV & MODEL COMPARISON  ======================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Leave-one-out cross-validation via Pareto-smoothed importance sampling (PSIS).
## Requires pointwise log-likelihood, which we compute from posterior draws.
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ SECTION E: LOO-CV ═══\n\n")

# Compute pointwise log-likelihood matrix: [n_draws × N]
cat("  Computing pointwise log-likelihoods for LOO-CV...\n")

# Use all draws for LOO
n_loo_draws <- n_draws
log_lik <- matrix(NA_real_, nrow = n_loo_draws, ncol = stan_data$N)

for (d in 1:n_loo_draws) {
  for (i in 1:stan_data$N) {
    t <- stan_data$shot_type[i]
    ci <- cal_int_draws[d, t]
    cs <- cal_slope_draws[d, t]
    ap <- draws_mat[d, paste0("a_player[", stan_data$player[i], ",", t, "]")]
    ad <- draws_mat[d, paste0("a_defender[", stan_data$defender[i], ",", t, "]")]
    at <- draws_mat[d, paste0("a_defteam[", stan_data$defteam[i], ",", t, "]")]

    eta <- ci + cs * stan_data$xfg_logit[i] + ap + ad + at
    p   <- inv_logit(eta)
    log_lik[d, i] <- dbinom(stan_data$y[i], 1, p, log = TRUE)
  }
  if (d %% 100 == 0) cat("    Draw", d, "/", n_loo_draws, "\n")
}

# Compute LOO
loo_result <- loo(log_lik, r_eff = relative_eff(exp(log_lik)))

cat("\n  LOO-CV Summary:\n")
print(loo_result)

# Pareto k diagnostic
k_values <- loo_result$diagnostics$pareto_k
cat("\n  Pareto k diagnostic:\n")
cat("    k < 0.5 (good):     ", sum(k_values < 0.5), "\n")
cat("    0.5 < k < 0.7 (ok): ", sum(k_values >= 0.5 & k_values < 0.7), "\n")
cat("    0.7 < k < 1 (bad):  ", sum(k_values >= 0.7 & k_values < 1), "\n")
cat("    k > 1 (very bad):   ", sum(k_values >= 1), "\n")

# Plot Pareto k values
k_df <- tibble(shot_idx = 1:length(k_values),
               pareto_k = k_values,
               shot_family = c("rim", "j2", "j3")[stan_data$shot_type])

p_pareto <- ggplot(k_df, aes(x = shot_idx, y = pareto_k, color = shot_family)) +
  geom_point(alpha = 0.15, size = 0.3) +
  geom_hline(yintercept = c(0.5, 0.7, 1.0),
             linetype = c("dashed", "dashed", "solid"),
             color = c("orange", "red", "darkred"),
             linewidth = 0.5) +
  labs(title = "LOO-CV Pareto k Diagnostics",
       subtitle = "k < 0.5 = reliable; 0.5-0.7 = marginal; > 0.7 = unreliable",
       x = "Shot index", y = "Pareto k", color = "Shot family") +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave(file.path(diag_dir, "E1_pareto_k_diagnostics.png"),
       p_pareto, width = 10, height = 6, dpi = 150)

# Save LOO result
saveRDS(loo_result, file.path(diag_dir, "loo_result.rds"))
cat("\n  E1: LOO-CV diagnostics — saved\n\n")



## ═════════════════════════════════════════════════════════════════════════════
## SECTION F — RESIDUAL & CALIBRATION DIAGNOSTICS  =============================
## ═════════════════════════════════════════════════════════════════════════════
##
## Binned residual plots and calibration curves using the full two-stage
## predictions (XGB + Stan player effects).
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ SECTION F: Residual & Calibration Diagnostics ═══\n\n")

# ── F1: Binned residual plot ──
# Group shots into bins of predicted p_skill, compute mean residual per bin

cal_hat <- colMeans(draws_mat[, c(paste0("cal_intercept[", 1:3, "]"),
                                   paste0("cal_slope[", 1:3, "]")),
                               drop = FALSE])

a_player_hat <- matrix(0, nrow = stan_data$J_player, ncol = 3)
for (t in 1:3) {
  cols <- paste0("a_player[", 1:stan_data$J_player, ",", t, "]")
  a_player_hat[, t] <- colMeans(draws_mat[, cols, drop = FALSE])
}

shots <- shots %>%
  mutate(eta_full = cal_hat[paste0("cal_intercept[", shot_type, "]")] +
           cal_hat[paste0("cal_slope[", shot_type, "]")] * xfg_logit +
           a_player_hat[cbind(player_idx, shot_type)],
         p_full = inv_logit(eta_full),
         resid  = fgm - p_full)

binned_resid <- shots %>%
  mutate(p_bin = ntile(p_full, 50)) %>%
  group_by(p_bin, shot_family) %>%
  summarise(n        = n(),
            mean_p   = mean(p_full),
            mean_obs = mean(fgm),
            resid    = mean_obs - mean_p,
            se       = sqrt(mean_p * (1 - mean_p) / n),
            .groups  = "drop")

p_binresid <- ggplot(binned_resid, aes(x = mean_p, y = resid)) +
  geom_point(aes(size = n), alpha = 0.6, color = "steelblue") +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_ribbon(aes(ymin = -2 * se, ymax = 2 * se), alpha = 0.15, fill = "grey") +
  facet_wrap(~shot_family, ncol = 3) +
  scale_size_continuous(range = c(1, 5)) +
  labs(title = "Binned Residual Plot (Two-Stage Model)",
       subtitle = "Points within grey band (±2 SE) indicate good fit",
       x = "Mean predicted P(make)", y = "Mean residual (obs - pred)") +
  theme_minimal() +
  theme(legend.position = "none")

ggsave(file.path(diag_dir, "F1_binned_residuals.png"),
       p_binresid, width = 14, height = 5, dpi = 150)


# ── F2: Calibration by shot context covariates ──
# Does the model calibrate well across different game situations?

# By quarter
cal_by_quarter <- shots %>%
  group_by(period, shot_family) %>%
  summarise(n = n(), pred = mean(p_full), obs = mean(fgm), .groups = "drop") %>%
  filter(period <= 4)

p_cal_qtr <- ggplot(cal_by_quarter, aes(x = pred, y = obs, color = shot_family)) +
  geom_point(aes(size = n)) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  facet_wrap(~paste("Q", period)) +
  labs(title = "Calibration by Quarter",
       x = "Mean predicted", y = "Mean observed") +
  theme_minimal()

ggsave(file.path(diag_dir, "F2_calibration_by_quarter.png"),
       p_cal_qtr, width = 12, height = 4, dpi = 150)


# ── F3: Prior vs Posterior comparison for sigma parameters ──
# How much did the data update our beliefs?

sigma_params <- c(paste0("sigma_player[", 1:3, "]"),
                  paste0("sigma_defender[", 1:3, "]"),
                  paste0("sigma_defteam[", 1:3, "]"))

sigma_post_draws <- fit_nba$draws(variables = sigma_params, format = "draws_df") %>%
  pivot_longer(cols = -c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "posterior")

# Generate prior draws for comparison
set.seed(123)
n_prior <- 4000
prior_sigmas <- bind_rows(tibble(parameter = "sigma_player[1]",  
                                 prior = abs(rnorm(n_prior, 0, 0.40))),
                          tibble(parameter = "sigma_player[2]",  
                                 prior = abs(rnorm(n_prior, 0, 0.35))),
                          tibble(parameter = "sigma_player[3]",  
                                 prior = abs(rnorm(n_prior, 0, 0.35))),
                          tibble(parameter = "sigma_defender[1]", 
                                 prior = abs(rnorm(n_prior, 0, 0.20))),
                          tibble(parameter = "sigma_defender[2]", 
                                 prior = abs(rnorm(n_prior, 0, 0.15))),
                          tibble(parameter = "sigma_defender[3]", 
                                 prior = abs(rnorm(n_prior, 0, 0.15))),
                          tibble(parameter = "sigma_defteam[1]", 
                                 prior = abs(rnorm(n_prior, 0, 0.15))),
                          tibble(parameter = "sigma_defteam[2]", 
                                 prior = abs(rnorm(n_prior, 0, 0.15))),
                          tibble(parameter = "sigma_defteam[3]", 
                                 prior = abs(rnorm(n_prior, 0, 0.15))))

prior_vs_post <- sigma_post_draws %>%
  select(parameter, posterior) %>%
  full_join(prior_sigmas, by = "parameter", relationship = "many-to-many")

# This is a large join — instead plot separately
p_pvp_list <- list()
for (param in sigma_params) {
  post_vals  <- sigma_post_draws %>% filter(parameter == param) %>% pull(posterior)
  prior_vals <- prior_sigmas %>% filter(parameter == param) %>% pull(prior)

  df_both <- bind_rows(tibble(source = "Prior",     value = prior_vals),
                       tibble(source = "Posterior", value = post_vals))

  p_pvp_list[[param]] <- ggplot(df_both, aes(x = value, fill = source)) +
    geom_density(alpha = 0.5) +
    labs(title = param, x = NULL, y = NULL) +
    theme_minimal() +
    theme(legend.position = "none",
          plot.title = element_text(size = 8))
}

p_prior_vs_post <- wrap_plots(p_pvp_list, ncol = 3) +
  plot_annotation(
    title = "Prior (blue) vs Posterior (red) for Hierarchical SDs",
    subtitle = "Substantial posterior concentration indicates data is informative"
  )

ggsave(file.path(diag_dir, "F3_prior_vs_posterior_sigma.png"),
       p_prior_vs_post, width = 14, height = 10, dpi = 150)

cat("  F1–F3: Residual & calibration diagnostics — saved\n\n")



## ═════════════════════════════════════════════════════════════════════════════
## SECTION G — SENSITIVITY ANALYSIS  ===========================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Quick check: how sensitive are key posterior summaries to prior widths?
## Compare posterior median sigma under current priors vs doubled priors.
## This is NOT a re-fit — it's a diagnostic summary using the existing fit
## to flag whether priors are dominating the posterior.
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ SECTION G: Prior Sensitivity Summary ═══\n\n")
sigma_post_all <- fit_nba$summary(variables = sigma_params)

# Effective prior width vs posterior width
sensitivity_df <- tibble(parameter = sigma_params,
                         prior_sd  = c(0.30, 0.30, 0.25,   # sigma_player
                                       0.20, 0.20, 0.20,    # sigma_defender
                                       0.12, 0.10, 0.06),   # sigma_defteam
                         post_mean = sigma_post_all$mean,
                         post_sd   = sigma_post_all$sd) %>%
  mutate(prior_95_upper = qnorm(0.975) * prior_sd,  # half-normal 95th percentile
         ratio = post_sd / prior_sd,  # < 1 means data is informative
         dominated = if_else(ratio > 0.8, 
                             "⚠️ prior may dominate", "✓ data informative"))

cat("  Prior sensitivity summary:\n")
cat("  (ratio < 0.5 = strongly informed by data; > 0.8 = prior-dominated)\n\n")
sensitivity_df %>%
  mutate(across(where(is.numeric), ~round(., 3))) %>%
  print()

cat("\n")



## ═════════════════════════════════════════════════════════════════════════════
## WRAP-UP  ====================================================================
## ═════════════════════════════════════════════════════════════════════════════

cat("╔══════════════════════════════════════════════════════════════╗\n")
cat("║                 DIAGNOSTICS COMPLETE                        ║\n")
cat("╚══════════════════════════════════════════════════════════════╝\n\n")
cat("All outputs saved to:", diag_dir, "/\n\n")
cat("  A1  Prior predictive FG%\n")
cat("  A2  Prior predictive P(make) distribution\n")
cat("  A3  Prior sigma distributions\n")
cat("  B2  ESS distribution\n")
cat("  B3  Trace plots\n")
cat("  B4  Rank histograms\n")
cat("  C1  Posterior predictive FG%\n")
cat("  D1  Shrinkage plots\n")
cat("  D2  Posterior SD vs volume\n")
cat("  E1  Pareto k diagnostics\n")
cat("  F1  Binned residuals\n")
cat("  F2  Calibration by quarter\n")
cat("  F3  Prior vs posterior sigma\n")
cat("  loo_result.rds\n")









## ═════════════════════════════════════════════════════════════════════════════
## 03c_prior_empirical_benchmarks.R
## NBA xFG — Empirical benchmarks for prior validation
##
## Computes observed distributions of player/defender/team FG% from raw data,
## providing the "ground truth" that priors should be consistent with.
##
## Run AFTER 03_stan_random_effects.R (needs shots with xfg).
## Produces console output + diagnostic plots in diagnostics/
## ═════════════════════════════════════════════════════════════════════════════

library(dplyr)
library(tidyr)
library(ggplot2)
library(patchwork)
library(scales)

inv_logit <- function(x) 1 / (1 + exp(-x))

diag_dir <- "diagnostics"
if (!dir.exists(diag_dir)) dir.create(diag_dir)

stopifnot("shots must exist" = exists("shots"))

cat("\n╔══════════════════════════════════════════════════════════════╗\n")
cat("║         EMPIRICAL BENCHMARKS FOR PRIOR VALIDATION          ║\n")
cat("╚══════════════════════════════════════════════════════════════╝\n\n")


## ═════════════════════════════════════════════════════════════════════════════
## 1 — OVERALL SHOOTING BENCHMARKS  ============================================
## ═════════════════════════════════════════════════════════════════════════════

cat("═══ 1. OVERALL SHOOTING ═══\n\n")

overall <- shots %>%
  summarise(n = n(), fg_pct = mean(fgm), sd = sd(fgm))

by_family <- shots %>%
  group_by(shot_family) %>%
  summarise(n = n(), fg_pct = mean(fgm), xfg_mean = mean(xfg),
            .groups = "drop") %>%
  arrange(match(shot_family, c("rim", "j2", "j3")))

cat("  Overall FG%: ", round(overall$fg_pct, 4), " (n =", overall$n, ")\n")
cat("  By family:\n")
for (i in 1:nrow(by_family)) {
  cat("    ", toupper(by_family$shot_family[i]),
      ": FG% =", round(by_family$fg_pct[i], 3),
      "  xFG% =", round(by_family$xfg_mean[i], 3),
      "  (n =", by_family$n[i], ")\n")
}


## ═════════════════════════════════════════════════════════════════════════════
## 2 — PLAYER SHOOTING SKILL DISTRIBUTION  =====================================
## ═════════════════════════════════════════════════════════════════════════════
## What does the real spread of player shooting look like?
## This is what sigma_player needs to be consistent with.

cat("\n═══ 2. PLAYER SHOOTING SPREAD ═══\n")
cat("  (What sigma_player must be consistent with)\n\n")

player_stats <- shots %>%
  group_by(player_idx, player_name_full, shot_family) %>%
  summarise(fga     = n(),
            fg_pct  = mean(fgm),
            xfg_pct = mean(xfg),
            resid   = fg_pct - xfg_pct,   # raw "skill" before Stan
            .groups = "drop") %>%
  ungroup() %>% 
  # Logit-scale residual (what Stan actually models)
  mutate(resid_logit = qlogis(pmin(pmax(fg_pct, 0.01), 0.99)) -
           qlogis(pmin(pmax(xfg_pct, 0.01), 0.99)))

for (fam in c("rim", "j2", "j3")) {
  # Volume-filtered: players with enough shots to be meaningful
  for (min_fga in c(50, 100, 200)) {
    df <- player_stats %>% filter(shot_family == fam, fga >= min_fga)
    if (nrow(df) == 0) next
    cat("  ", toupper(fam), "(n >=", min_fga, "FGA,", nrow(df), "players):\n")
    cat("    FG% — mean:", round(mean(df$fg_pct), 3),
        " sd:", round(sd(df$fg_pct), 3),
        " 90% interval: [", round(quantile(df$fg_pct, 0.05), 3), ",",
        round(quantile(df$fg_pct, 0.95), 3), "]\n")
    cat("    Residual (FG% - xFG%) — mean:", round(mean(df$resid), 3),
        " sd:", round(sd(df$resid), 3),
        " 90%: [", round(quantile(df$resid, 0.05), 3), ",",
        round(quantile(df$resid, 0.95), 3), "]\n")
    cat("    Logit-scale residual — sd:", round(sd(df$resid_logit), 3),
        " 90%: [", round(quantile(df$resid_logit, 0.05), 3), ",",
        round(quantile(df$resid_logit, 0.95), 3), "]\n")
    # Best-to-worst gap
    cat("    Best-to-worst gap:",
        round((max(df$fg_pct) - min(df$fg_pct)) * 100, 1), "pp\n")
    cat("    Best-to-worst gap (residual):",
        round((max(df$resid) - min(df$resid)) * 100, 1), "pp\n\n")
  }
}

# ── Plot: player FG% residual distributions ──
p_player_resid <- player_stats %>%
  filter(fga >= 100) %>%
  ggplot(aes(x = resid * 100)) +
  geom_histogram(bins = 30, fill = "steelblue", alpha = 0.7, color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~toupper(shot_family), scales = "free") +
  labs(title = "Observed Player Skill: FG% Minus xFG% (100+ FGA)",
       subtitle = "SD of this distribution ≈ what sigma_player should capture",
       x = "FG% - xFG% (percentage points)", y = "Count") +
  theme_minimal()

ggsave(file.path(diag_dir, "empirical_player_resid_dist.png"),
       p_player_resid, width = 12, height = 5, dpi = 150)

# ── Plot: logit-scale residual (direct comparison to sigma_player) ──
p_player_logit <- player_stats %>%
  filter(fga >= 100) %>%
  ggplot(aes(x = resid_logit)) +
  geom_histogram(bins = 30, fill = "steelblue", alpha = 0.7, color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~toupper(shot_family), scales = "free") +
  labs(title = "Observed Player Logit-Scale Residuals (100+ FGA)",
       subtitle = "SD here is an UPPER BOUND on sigma_player",
       x = "Logit-scale residual (observed - xFG)", y = "Count") +
  theme_minimal()

ggsave(file.path(diag_dir, "empirical_player_logit_resid.png"),
       p_player_logit, width = 12, height = 5, dpi = 150)


## ═════════════════════════════════════════════════════════════════════════════
## 3 — INDIVIDUAL DEFENDER IMPACT DISTRIBUTION  ================================
## ═════════════════════════════════════════════════════════════════════════════
## Opponent FG% by defender — what sigma_defender needs to be consistent with.

cat("═══ 3. INDIVIDUAL DEFENDER SPREAD ═══\n")
cat("  (What sigma_defender must be consistent with)\n\n")

defender_stats <- shots %>%
  group_by(defender_idx, shot_family) %>%
  summarise(contests  = n(),
            opp_fg    = mean(fgm),
            opp_xfg   = mean(xfg),
            resid     = opp_fg - opp_xfg,
            .groups   = "drop") %>%
  mutate(resid_logit = qlogis(pmin(pmax(opp_fg, 0.01), 0.99)) -
           qlogis(pmin(pmax(opp_xfg, 0.01), 0.99)))

for (fam in c("rim", "j2", "j3")) {
  for (min_n in c(50, 100, 200)) {
    df <- defender_stats %>% filter(shot_family == fam, contests >= min_n)
    if (nrow(df) == 0) next
    cat("  ", toupper(fam), "(n >=", min_n, "contests,", nrow(df), "defenders):\n")
    cat("    Opp FG% — mean:", round(mean(df$opp_fg), 3),
        " sd:", round(sd(df$opp_fg), 3),
        " 90%: [", round(quantile(df$opp_fg, 0.05), 3), ",",
        round(quantile(df$opp_fg, 0.95), 3), "]\n")
    cat("    Residual (opp FG% - opp xFG%) — sd:", round(sd(df$resid), 3),
        " 90%: [", round(quantile(df$resid, 0.05), 3), ",",
        round(quantile(df$resid, 0.95), 3), "]\n")
    cat("    Logit-scale residual — sd:", round(sd(df$resid_logit), 3),
        " 90%: [", round(quantile(df$resid_logit, 0.05), 3), ",",
        round(quantile(df$resid_logit, 0.95), 3), "]\n")
    cat("    Best-to-worst gap (opp FG%):",
        round((max(df$opp_fg) - min(df$opp_fg)) * 100, 1), "pp\n")
    cat("    Best-to-worst gap (residual):",
        round((max(df$resid) - min(df$resid)) * 100, 1), "pp\n\n")
  }
}

# ── Plot: defender residual distributions ──
p_def_resid <- defender_stats %>%
  filter(contests >= 100) %>%
  ggplot(aes(x = resid * 100)) +
  geom_histogram(bins = 30, fill = "#e74c3c", alpha = 0.7, color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~toupper(shot_family), scales = "free") +
  labs(title = "Observed Defender Impact: Opp FG% Minus Opp xFG% (100+ Contests)",
       subtitle = "SD is an UPPER BOUND on sigma_defender",
       x = "Opponent FG% - Opponent xFG% (pp)", y = "Count") +
  theme_minimal()

ggsave(file.path(diag_dir, "empirical_defender_resid_dist.png"),
       p_def_resid, width = 12, height = 5, dpi = 150)

# ── Plot: logit-scale defender residual ──
p_def_logit <- defender_stats %>%
  filter(contests >= 100) %>%
  ggplot(aes(x = resid_logit)) +
  geom_histogram(bins = 30, fill = "#e74c3c", alpha = 0.7, color = "white") +
  geom_vline(xintercept = 0, linetype = "dashed") +
  facet_wrap(~toupper(shot_family), scales = "free") +
  labs(title = "Observed Defender Logit-Scale Residuals (100+ Contests)",
       subtitle = "SD here is an UPPER BOUND on sigma_defender",
       x = "Logit-scale residual", y = "Count") +
  theme_minimal()

ggsave(file.path(diag_dir, "empirical_defender_logit_resid.png"),
       p_def_logit, width = 12, height = 5, dpi = 150)


## ═════════════════════════════════════════════════════════════════════════════
## 4 — DEFENSIVE TEAM IMPACT DISTRIBUTION  =====================================
## ═════════════════════════════════════════════════════════════════════════════
## Team-level opponent FG% after controlling for shot difficulty.

cat("═══ 4. DEFENSIVE TEAM SPREAD ═══\n")
cat("  (What sigma_defteam must be consistent with)\n\n")

defteam_stats <- shots %>%
  group_by(defteam_idx, defender_team, shot_family) %>%
  summarise(fga_against = n(),
            opp_fg      = mean(fgm),
            opp_xfg     = mean(xfg),
            resid       = opp_fg - opp_xfg,
            .groups     = "drop") %>%
  mutate(resid_logit = qlogis(pmin(pmax(opp_fg, 0.01), 0.99)) -
           qlogis(pmin(pmax(opp_xfg, 0.01), 0.99)))

for (fam in c("rim", "j2", "j3")) {
  df <- defteam_stats %>% filter(shot_family == fam)
  cat("  ", toupper(fam), "(", nrow(df), "teams):\n")
  cat("    Opp FG% — mean:", round(mean(df$opp_fg), 3),
      " sd:", round(sd(df$opp_fg), 3),
      " 90%: [", round(quantile(df$opp_fg, 0.05), 3), ",",
      round(quantile(df$opp_fg, 0.95), 3), "]\n")
  cat("    Residual (opp FG% - opp xFG%) — sd:", round(sd(df$resid), 3),
      " 90%: [", round(quantile(df$resid, 0.05), 3), ",",
      round(quantile(df$resid, 0.95), 3), "]\n")
  cat("    Logit-scale residual — sd:", round(sd(df$resid_logit), 3), "\n")
  cat("    Best-to-worst gap (opp FG%):",
      round((max(df$opp_fg) - min(df$opp_fg)) * 100, 1), "pp\n")
  cat("    Best-to-worst gap (residual):",
      round((max(df$resid) - min(df$resid)) * 100, 1), "pp\n")
  # Name best and worst
  best <- df %>% arrange(resid) %>% head(1)
  worst <- df %>% arrange(desc(resid)) %>% head(1)
  cat("Best:", best$defender_team,
      "(resid =", round(best$resid * 100, 1), "pp)\n")
  cat("Worst:", worst$defender_team,
      "(resid =", round(worst$resid * 100, 1), "pp)\n\n")
}

# ── Plot: team residuals with team labels ──
p_team_resid <- defteam_stats %>%
  ggplot(aes(x = reorder(defender_team, resid), y = resid * 100)) +
  geom_col(aes(fill = resid > 0), show.legend = FALSE, alpha = 0.8) +
  scale_fill_manual(values = c("TRUE" = "#e74c3c", "FALSE" = "#27ae60")) +
  geom_hline(yintercept = 0, linewidth = 0.5) +
  coord_flip() +
  facet_wrap(~toupper(shot_family), scales = "free_x") +
  labs(title = "Defensive Team Impact: Opp FG% Minus Opp xFG%",
       subtitle = "Green = better than xFG (good defense). Red = worse.",
       x = NULL, y = "Opponent FG% residual (pp)") +
  theme_minimal() +
  theme(axis.text.y = element_text(size = 6))

ggsave(file.path(diag_dir, "empirical_defteam_resid_bars.png"),
       p_team_resid, width = 14, height = 8, dpi = 150)


## ═════════════════════════════════════════════════════════════════════════════
## 5 — COMPARISON SUMMARY TABLE  ===============================================
## ═════════════════════════════════════════════════════════════════════════════
## Side-by-side: empirical logit-scale SD vs prior implied SD

cat("═══ 5. PRIOR vs EMPIRICAL COMPARISON ═══\n")
cat("  (logit-scale SD: empirical is UPPER BOUND due to sampling noise)\n\n")

# Prior implied SD: for a half-normal(0, s), the SD is s * sqrt(1 - 2/pi)
# But sigma_player IS the SD of the random effects, not a parameter of a
# half-normal on the effects themselves. So the prior on sigma says
# "I believe sigma is drawn from half-normal(0, s)".
# The expected value of sigma under the prior is s * sqrt(2/pi).
prior_expected <- function(s) s * sqrt(2 / pi)

comparison_rows <- list()

for (fam in c("rim", "j2", "j3")) {
  t <- match(fam, c("rim", "j2", "j3"))
  
  # Player
  pl <- player_stats %>% filter(shot_family == fam, fga >= 100)
  # Defender
  de <- defender_stats %>% filter(shot_family == fam, contests >= 100)
  # Team
  tm <- defteam_stats %>% filter(shot_family == fam)
  
  prior_sd_player   <- c(0.40, 0.35, 0.35)[t]
  prior_sd_defender  <- c(0.20, 0.15, 0.15)[t]
  prior_sd_defteam   <- c(0.15, 0.15, 0.15)[t]
  
  comparison_rows[[length(comparison_rows) + 1]] <- tibble(
    family          = fam,
    entity          = "Player",
    n_entities      = nrow(pl),
    empirical_sd    = round(sd(pl$resid_logit), 3),
    prior_scale     = prior_sd_player,
    prior_E_sigma   = round(prior_expected(prior_sd_player), 3),
    empirical_gap_pp = round((max(pl$fg_pct) - min(pl$fg_pct)) * 100, 1)
  )
  comparison_rows[[length(comparison_rows) + 1]] <- tibble(
    family          = fam,
    entity          = "Defender",
    n_entities      = nrow(de),
    empirical_sd    = round(sd(de$resid_logit), 3),
    prior_scale     = prior_sd_defender,
    prior_E_sigma   = round(prior_expected(prior_sd_defender), 3),
    empirical_gap_pp = round((max(de$opp_fg) - min(de$opp_fg)) * 100, 1)
  )
  comparison_rows[[length(comparison_rows) + 1]] <- tibble(
    family          = fam,
    entity          = "Def Team",
    n_entities      = nrow(tm),
    empirical_sd    = round(sd(tm$resid_logit), 3),
    prior_scale     = prior_sd_defteam,
    prior_E_sigma   = round(prior_expected(prior_sd_defteam), 3),
    empirical_gap_pp = round((max(tm$opp_fg) - min(tm$opp_fg)) * 100, 1)
  )
}

comparison_tbl <- bind_rows(comparison_rows)

cat("empirical_sd = observed SD of logit-scale residuals (upper bound on true sigma)")
cat("prior_E_sigma = E[sigma] under the half-normal prior\n")
cat("Rule of thumb: prior_E_sigma should be in the NEIGHBORHOOD of empirical_sd,")
cat("               but can be somewhat larger (priors should not be too tight).")
cat("               If prior_E_sigma >> 2× empirical_sd, prior may be too wide.")

print(comparison_tbl, n = Inf)


## ═════════════════════════════════════════════════════════════════════════════
## 6 — COMBINED VISUALIZATION  =================================================
## ═════════════════════════════════════════════════════════════════════════════
## Overlay: empirical logit-scale residual distribution vs prior implied range

set.seed(42)

overlay_plots <- list()
for (fam in c("rim", "j2", "j3")) {
  t <- match(fam, c("rim", "j2", "j3"))
  
  for (entity in c("Player", "Defender")) {
    if (entity == "Player") {
      emp_df <- player_stats %>% filter(shot_family == fam, fga >= 100)
      emp_vals <- emp_df$resid_logit
      prior_scale <- c(0.40, 0.35, 0.35)[t]
    } else {
      emp_df <- defender_stats %>% filter(shot_family == fam, contests >= 100)
      emp_vals <- emp_df$resid_logit
      prior_scale <- c(0.20, 0.15, 0.15)[t]
    }
    
    # Simulate prior-implied effect distribution:
    # Draw sigma from half-normal, then draw effects ~ N(0, sigma)
    n_sim <- 5000
    sigma_draws <- abs(rnorm(n_sim, 0, prior_scale))
    prior_effects <- rnorm(n_sim, 0, sigma_draws)
    
    overlay_df <- bind_rows(
      tibble(source = "Empirical residuals", value = emp_vals),
      tibble(source = "Prior-implied effects", value = prior_effects)
    )
    
    overlay_plots[[paste(fam, entity)]] <-
      ggplot(overlay_df, aes(x = value, fill = source)) +
      geom_density(alpha = 0.45) +
      geom_vline(xintercept = 0, linetype = "dashed", color = "grey40") +
      scale_fill_manual(values = c("Empirical residuals" = "steelblue",
                                   "Prior-implied effects" = "tomato")) +
      labs(title = paste(toupper(fam), "—", entity),
           x = "Logit-scale effect", y = "Density") +
      theme_minimal() +
      theme(legend.position = "bottom",
            legend.title = element_blank(),
            plot.title = element_text(size = 10))
  }
}

p_overlay <- wrap_plots(overlay_plots, ncol = 3) +
  plot_annotation(
    title = "Prior-Implied vs Empirical Effect Distributions (Logit Scale)",
    subtitle = "Empirical includes sampling noise that inflates spread"
  )

ggsave(file.path(diag_dir, "empirical_vs_prior_overlay.png"),
       p_overlay, width = 16, height = 8, dpi = 150)


cat("\n\n╔══════════════════════════════════════════════════════════════╗\n")
cat("║              EMPIRICAL BENCHMARKS COMPLETE                  ║\n")
cat("╚══════════════════════════════════════════════════════════════╝\n\n")
cat("Outputs in:", diag_dir, "/\n")
cat("  empirical_player_resid_dist.png\n")
cat("  empirical_player_logit_resid.png\n")
cat("  empirical_defender_resid_dist.png\n")
cat("  empirical_defender_logit_resid.png\n")
cat("  empirical_defteam_resid_bars.png\n")
cat("  empirical_vs_prior_overlay.png\n")