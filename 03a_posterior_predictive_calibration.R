## ═════════════════════════════════════════════════════════════════════════════
## 03a_posterior_predictive_calibration.R
## NBA xFG — Posterior Predictive Calibration (Osborn Table 2 style)
##
## PURPOSE:
## Checks whether the full two-stage model's posterior predictive intervals
## are well-calibrated at the PLAYER level. This goes beyond bin-level
## calibration (twostage_calibration.png) to ask: "For each player, does
## their observed FG% fall where the model says it should?"
##
## If well-calibrated:
## - ~5% of players should exceed their 95th percentile prediction
## - ~50% should exceed their 50th percentile prediction
## - The quantile-of-observed should be ~ Uniform(0,1)
##
## Deviations tell you:
## - Overconfidence = too many at extremes (intervals too narrow)
## - Underconfidence = too few at extremes (intervals too wide)
## - Bias = asymmetry (e.g., 40% exceed median → upward bias)
##
## REQUIRES: 03_stan_random_effects.R already run (needs fit_v3, draws_mat,
## shots, cal_params, stan_data, player_map)
## ═════════════════════════════════════════════════════════════════════════════



## ═════════════════════════════════════════════════════════════════════════════
## 00 — LIBRARIES (if running standalone) ======================================
## ═════════════════════════════════════════════════════════════════════════════

library(dplyr)
library(tidyr)
library(ggplot2)
library(patchwork)
library(scales)

inv_logit <- function(x) 1 / (1 + exp(-x))





## ═════════════════════════════════════════════════════════════════════════════
## 01 — EXTRACT FULL POSTERIOR DRAWS FOR PLAYER-LEVEL PREDICTIONS ==============
## ═════════════════════════════════════════════════════════════════════════════
##
## For each posterior draw d and each player i, we need:
## p_i^(d) = inv_logit(cal_intercept[t]^(d) + cal_slope[t]^(d) * mean_xfg_logit_i
## + a_player[i,t]^(d))
##
## Then simulate Y_i^(d) ~ Binomial(N_i, p_i^(d)) and compare to observed.
##
## Key decision: we aggregate to player × shot_family level, computing each
## player's expected FG% across their actual shot mix, then simulate total
## makes from a Binomial(FGA, mean_p).

cat("═══ POSTERIOR PREDICTIVE CALIBRATION ═══\n\n")

## ─── Player-level observed summaries ───
player_obs <- shots %>%
  group_by(player_idx, shot_type, shot_family) %>%
  summarise(fga = n(),
            fgm_obs = sum(fgm),
            fg_pct_obs = mean(fgm),
            mean_xfg_logit = mean(xfg_logit),
            .groups = "drop")

## ─── Draw indices for calibration + player effect parameters ───
n_draws <- nrow(draws_mat)
cat("Using", n_draws, "posterior draws\n")

## Pre-extract calibration draws (n_draws × 6)
cal_intercept_draws <- draws_mat[, paste0("cal_intercept[", 1:3, "]")]
cal_slope_draws <- draws_mat[, paste0("cal_slope[", 1:3, "]")]





## ═════════════════════════════════════════════════════════════════════════════
## 02 — SIMULATE POSTERIOR PREDICTIVE FGM FOR EACH PLAYER × FAMILY ============
## ═════════════════════════════════════════════════════════════════════════════
##
## For computational tractability, we work at the player × shot_family level
## using each player's mean xfg_logit. This is an approximation (Jensen's
## inequality means E[inv_logit(x)] ≠ inv_logit(E[x])), but the bias is
## negligible when the within-player xfg_logit spread is small relative to
## the scale of the logistic function — which it is for most players.

## Minimum FGA filter — mirrors Osborn's 150 BIP threshold.
## Adjust as needed: lower catches more players but noisier calibration.
MIN_FGA <- 50

player_obs_filtered <- player_obs %>% filter(fga >= MIN_FGA)
cat("Players × families with >=", MIN_FGA, "FGA:",
    nrow(player_obs_filtered), "\n")

## For each player × family, compute the quantile of the observed FGM
## in the posterior predictive distribution.

compute_ppc_quantile <- function(player_shots_df, draws_mat,
                                    cal_intercept_draws, cal_slope_draws) {
  # player_shots_df: all shots for one player × family
  idx <- player_shots_df$player_idx[1]
  t   <- player_shots_df$shot_type[1]
  fga <- nrow(player_shots_df)
  fgm <- sum(player_shots_df$fgm)
  
  a_draws <- draws_mat[, paste0("a_player[", idx, ",", t, "]")]
  n_draws <- length(a_draws)
  
  # For each draw, compute p for EACH shot, then simulate makes
  # To keep this tractable, use vectorized matrix ops:
  # eta_matrix: n_draws × fga
  xfg_vec <- player_shots_df$xfg_logit  # length = fga
  
  eta_matrix <- outer(cal_intercept_draws[, t], rep(1, fga)) +
    outer(cal_slope_draws[, t], xfg_vec) +
    outer(a_draws, rep(1, fga))
  
  p_matrix <- 1 / (1 + exp(-eta_matrix))  # n_draws × fga
  
  # Per-draw: simulate each shot independently, sum to get FGM
  # This correctly handles heterogeneous shot difficulty
  fgm_sim <- rowSums(matrix(rbinom(n_draws * fga, size = 1, 
                                   prob = as.vector(p_matrix)),
                            nrow = n_draws, ncol = fga))
  
  # Randomized PIT
  p_below <- mean(fgm_sim < fgm)
  p_equal <- mean(fgm_sim == fgm)
  pit     <- p_below + runif(1) * p_equal
  
  tibble(player_idx  = idx,
         shot_type   = t,
         shot_family = player_shots_df$shot_family[1],
         fga         = fga,
         fgm_obs     = fgm,
         fg_pct_obs  = fgm / fga,
         p_mean      = mean(rowMeans(p_matrix)),
         pit_value   = pit)
}

## ─── Run the corrected PPC ───
set.seed(2025)

# Build player-family groups
player_groups <- shots %>%
  group_by(player_idx, shot_type, shot_family) %>%
  filter(n() >= MIN_FGA) %>%
  group_split()

cat("Computing corrected PPC for", length(player_groups), "player-families...\n")

ppc_results <- bind_rows(lapply(seq_along(player_groups), 
                                function(i) {
                                  if (i %% 100 == 0) 
                                    cat("  ", i, "/", 
                                        length(player_groups), 
                                        "\n")
                                  compute_ppc_quantile(player_groups[[i]], draws_mat,
                                                       cal_intercept_draws, cal_slope_draws)
  })
)

cat("Done. PPC results:", nrow(ppc_results), "rows\n\n")





## ═════════════════════════════════════════════════════════════════════════════
## 03 — CALIBRATION TABLE (Osborn Table 2 style) ==============================
## ═════════════════════════════════════════════════════════════════════════════

## Overall calibration: what % of players exceed their Xth percentile?
percentiles_to_check <- c(0.95, 0.90, 0.80, 0.70, 0.60, 0.50,
                          0.40, 0.30, 0.20, 0.10, 0.05)

calibration_table <- tibble(percentile = paste0(100 * percentiles_to_check, "th"),
                            expected = paste0(round(100 * (1 - percentiles_to_check), 1), "%"),
                            pct_exceeded = sapply(percentiles_to_check, function(q) {
                              mean(ppc_results$pit_value > q)
                              }),
                            n_exceeded = sapply(percentiles_to_check, function(q) {
                              sum(ppc_results$pit_value > q)
                              }),
                            total = nrow(ppc_results)) %>%
  mutate(observed = paste0(round(100 * pct_exceeded, 1), "%"),
         breakdown = paste0(n_exceeded, "/", total),
         # Deviation from expectation (positive = overconfident)
         deviation = pct_exceeded - (1 - percentiles_to_check))

cat("═══ POSTERIOR PREDICTIVE CALIBRATION TABLE ═══\n")
cat(" (All shot families pooled, min", MIN_FGA, "FGA)\n\n")
cat(sprintf("%-12s %-10s %-10s %-12s %-10s\n",
            "Percentile", "Expected", "Observed", "Breakdown", "Deviation"))
cat(strrep("─", 58), "\n")
for (i in seq_len(nrow(calibration_table))) {
  r <- calibration_table[i, ]
  cat(sprintf("%-12s %-10s %-10s %-12s %+.1f pp\n",
              r$percentile, r$expected, r$observed, r$breakdown,
              100 * r$deviation))
}

## ─── By shot family ───
cat("\n\n═══ CALIBRATION BY SHOT FAMILY ═══\n")

for (fam in c("rim", "j2", "j3")) {
  ppc_fam <- ppc_results %>% filter(shot_family == fam)
  cat("\n── ", toupper(fam), " (n =", nrow(ppc_fam), ") ──\n")
  cat(sprintf("%-12s %-10s %-10s\n", "Percentile", "Expected", "Observed"))
  cat(strrep("─", 36), "\n")
  for (q in c(0.95, 0.90, 0.50, 0.10, 0.05)) {
    obs <- mean(ppc_fam$pit_value > q)
    cat(sprintf("%-12s %-10s %-10s\n",
                paste0(100 * q, "th"),
                paste0(round(100 * (1 - q), 1), "%"),
                paste0(round(100 * obs, 1), "%")))
  }
}





## ═════════════════════════════════════════════════════════════════════════════
## 04 — PIT HISTOGRAM (should be uniform if well-calibrated) ==================
## ═════════════════════════════════════════════════════════════════════════════

p_pit_all <- ggplot(ppc_results, aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 20, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red", linewidth = 0.8) +
  labs(title = "Posterior Predictive PIT Histogram (All Families)",
       subtitle = paste0("Uniform = well-calibrated | n = ", nrow(ppc_results),
                         " player-families, min ", MIN_FGA, " FGA"),
       x = "PIT value (quantile of observed in posterior predictive)",
       y = "Density") +
  theme_minimal()

p_pit_by_fam <- ggplot(ppc_results, aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 20, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red", linewidth = 0.8) +
  facet_wrap(~shot_family, ncol = 3) +
  labs(title = "PIT Histogram by Shot Family",
       subtitle = "U-shape = overconfident | Inverted-U = underconfident | Skew = bias",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_combined <- p_pit_all / p_pit_by_fam +
  plot_annotation(title = "Posterior Predictive Calibration Diagnostics")

ggsave("ppc_calibration.png", p_pit_combined, width = 12, height = 10, dpi = 150)
cat("\n\nPIT histogram saved: ppc_calibration.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 05 — CALIBRATION CURVE (observed quantile vs expected quantile) =============
## ═════════════════════════════════════════════════════════════════════════════
##
## If the model is perfectly calibrated, the ECDF of PIT values should be
## the identity line. Deviations show where the model is off.

p_ecdf <- ggplot(ppc_results, aes(x = pit_value, color = shot_family)) +
  stat_ecdf(linewidth = 0.8) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey40") +
  labs(title = "PIT ECDF by Shot Family",
       subtitle = "Diagonal = perfect calibration",
       x = "Expected quantile (PIT value)",
       y = "Observed proportion ≤ quantile",
       color = "Shot Family") +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave("ppc_ecdf.png", p_ecdf, width = 8, height = 7, dpi = 150)
cat("PIT ECDF saved: ppc_ecdf.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 06 — VOLUME-STRATIFIED CALIBRATION ==========================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Are low-volume players worse calibrated than high-volume ones?
## This is directly analogous to Osborn's concern about small-sample
## instability and validates that the volume prior is doing its job.

ppc_results <- ppc_results %>%
  mutate(volume_bin = case_when(fga < 100 ~ "50-99 FGA",
                                fga < 250 ~ "100-249 FGA",
                                fga < 500 ~ "250-499 FGA",
                                TRUE ~ "500+ FGA") %>% 
           factor(levels = c("50-99 FGA", "100-249 FGA",
                             "250-499 FGA", "500+ FGA")))

p_pit_volume <- ggplot(ppc_results, aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by Shot Volume",
       subtitle = "Low-volume players should still be well-calibrated if priors are good",
       x = "PIT value", y = "Density") +
  theme_minimal()

ggsave("ppc_calibration_by_volume.png", p_pit_volume,
       width = 10, height = 8, dpi = 150)
cat("Volume-stratified PIT saved: ppc_calibration_by_volume.png\n")


p_pit_volume_rim <- ggplot(ppc_results %>% filter(shot_family == "rim"), aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by Rim Shot Volume",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_volume_j2 <- ggplot(ppc_results %>% filter(shot_family == "j2"), aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by Midrange Shot Volume",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_volume_j3 <- ggplot(ppc_results %>% filter(shot_family == "j3"), aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by 3-point Shot Volume",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_combined <- p_pit_volume_rim / p_pit_volume_j2 / p_pit_volume_j3 +
  plot_annotation(title = "PIT Histogram Stratified by Shot Family Volume")

ggsave("ppc_calibration_by_rim_volume.png", p_pit_volume_rim,
       width = 10, height = 8, dpi = 150)
ggsave("ppc_calibration_by_j2_volume.png", p_pit_volume_j2,
       width = 10, height = 8, dpi = 150)
ggsave("ppc_calibration_by_j3_volume.png", p_pit_volume_j3,
       width = 10, height = 8, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 07 — INDIVIDUAL EXTREME CASES ===============================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Which players had outcomes that the model found very surprising?
## PIT near 0 = performed much worse than expected
## PIT near 1 = performed much better than expected

extreme_low <- ppc_results %>%
  filter(pit_value < 0.025) %>%
  left_join(player_map, by = "player_idx") %>%
  arrange(pit_value) %>%
  select(player_name_full, shot_family, fga, fg_pct_obs, p_mean, pit_value)

extreme_high <- ppc_results %>%
  filter(pit_value > 0.975) %>%
  left_join(player_map, by = "player_idx") %>%
  arrange(desc(pit_value)) %>%
  select(player_name_full, shot_family, fga, fg_pct_obs, p_mean, pit_value)

cat("\n═══ EXTREME UNDERPERFORMERS (PIT < 2.5%) ═══\n")
print(head(extreme_low, 15), n = 15)
cat("\n═══ EXTREME OVERPERFORMERS (PIT > 97.5%) ═══\n")
print(head(extreme_high, 15), n = 15)





## ═════════════════════════════════════════════════════════════════════════════
## 08 — SAVE OUTPUTS ===========================================================
## ═════════════════════════════════════════════════════════════════════════════
write.csv(calibration_table %>%
            select(percentile, expected, observed, breakdown, deviation),
          "ppc_calibration_table.csv", row.names = FALSE)
write.csv(ppc_results, "ppc_player_results.csv", row.names = FALSE)
cat("\n── Posterior predictive calibration complete ──\n")
cat(" ppc_calibration_table.csv — Osborn-style Table 2\n")
cat(" ppc_player_results.csv — per-player PIT values\n")
cat(" ppc_calibration.png — PIT histograms\n")
cat(" ppc_ecdf.png — PIT ECDF curves\n")
cat(" ppc_calibration_by_volume.png — volume-stratified PIT\n")










## ─── DIAGNOSTIC 1: Is the bias in XGBoost or Stan? ───
## Compare raw XGBoost xFG to observed FG% at the player level for rim
rim_player_check <- shots %>%
  filter(shot_family == "rim") %>%
  group_by(player_idx, player_name_full) %>%
  summarise(fga     = n(),
            fg_obs  = mean(fgm),
            xfg_mean = mean(xfg),          # XGBoost prediction (Stage 1 only)
            p_skill  = mean(p_skill),       # Full two-stage prediction
            .groups = "drop") %>%
  filter(fga >= 50) %>%
  mutate(xgb_resid  = fg_obs - xfg_mean,   # Stage 1 residual
         stan_resid = fg_obs - p_skill)    # Full model residual

cat("── Rim bias diagnostic ──\n")
cat("Stage 1 (XGBoost only):\n")
cat("  Mean residual:", round(mean(rim_player_check$xgb_resid), 4), "\n")
cat("  % undershoot:", round(mean(rim_player_check$xgb_resid < 0) * 100, 1), "%\n")
cat("\nFull model (XGB + Stan):\n")
cat("  Mean residual:", round(mean(rim_player_check$stan_resid), 4), "\n")
cat("  % undershoot:", round(mean(rim_player_check$stan_resid < 0) * 100, 1), "%\n")


## ─── DIAGNOSTIC 2: Volume prior contribution for rim ───
cat("\n── mu_volume posterior (rim = index 1) ──\n")
print(fit_v3$summary(variables = "mu_volume[1]"))

## How much is the volume prior shifting rim player effects?
vol_shift_rim <- vol_matrix[, 1] *
  mean(draws_mat[, "mu_volume[1]"])  # posterior mean of mu_volume for rim

cat("\nVolume shift distribution for rim (logit scale):\n")
cat("  Mean:", round(mean(vol_shift_rim), 4), "\n")
cat("  SD:  ", round(sd(vol_shift_rim), 4), "\n")
cat("  Range:", round(range(vol_shift_rim), 4), "\n")


## ─── DIAGNOSTIC 3: Are rim player effects systematically positive? ───
rim_effects <- draws_mat[, paste0("a_player[", 1:stan_data$J_player, ",1]")]
rim_effect_means <- colMeans(rim_effects)

cat("\n── Distribution of rim player effect posterior means ──\n")
cat("  Mean of means:", round(mean(rim_effect_means), 4), "\n")
cat("  Should be ~0 if centered properly\n")
cat("  % positive:", round(mean(rim_effect_means > 0) * 100, 1), "%\n")

# Compare to j2/j3
for (t in 2:3) {
  fam <- c("j2", "j3")[t - 1]
  eff <- colMeans(draws_mat[, paste0("a_player[", 1:stan_data$J_player, ",", t, "]")])
  cat("  ", fam, "mean of means:", round(mean(eff), 4),
      " | % positive:", round(mean(eff > 0) * 100, 1), "%\n")
}



































## ═════════════════════════════════════════════════════════════════════════════
## 03b_overdispersion_diagnostic.R
## NBA xFG — Overdispersion Diagnostic & v4 Model Fitting
##
## PURPOSE:
## 1. Diagnose whether v3 is overconfident (Osborn's Table 2 problem)
## 2. Fit v4 with overdispersion + stochastic volume
## 3. Compare v3 vs v4 via LOO-CV and calibration
##
## WORKFLOW:
## Step 1: Run 03a to get PPC calibration table from v3
## Step 2: If overconfident → run this script to fit v4 and compare
## Step 3: If v4 wins on LOO + calibration → adopt it
##
## REQUIRES: 03_stan_random_effects.R already run (fit_v3, draws_mat, shots,
## stan_data, player_map, all mapping tables)
## ═════════════════════════════════════════════════════════════════════════════





## ═════════════════════════════════════════════════════════════════════════════
## 00 — LIBRARIES ==============================================================
## ═════════════════════════════════════════════════════════════════════════════

library(dplyr)
library(tidyr)
library(cmdstanr)
library(posterior)
library(loo)
library(ggplot2)
library(patchwork)
library(scales)
library(moments)

set_cmdstan_path(path = cmdstanr::cmdstan_path())
options(mc.cores = parallel::detectCores())
inv_logit <- function(x) 1 / (1 + exp(-x))





## ═════════════════════════════════════════════════════════════════════════════
## 01 — PRE-FIT DIAGNOSTIC: RESIDUAL OVERDISPERSION IN v3 =====================
## ═════════════════════════════════════════════════════════════════════════════
##
## Before fitting a whole new model, check whether overdispersion is actually
## a problem. If the v3 residuals are well-behaved, sigma_od will just
## collapse to zero and the model wasted compute.
##
## Diagnostic: for each player × family, compute observed FG% minus the
## posterior mean predicted FG%. If these residuals have fatter tails than
## a Binomial process would produce, we have overdispersion.
cat("═══ OVERDISPERSION DIAGNOSTIC FOR v3 ═══\n\n")

## Compute expected SD under pure Binomial for each player-family
player_resid <- shots %>%
  group_by(player_idx, shot_type, shot_family) %>%
  summarise(fga = n(),
            fgm = sum(fgm),
            fg_obs = mean(fgm),
            p_skill = mean(p_skill), # posterior mean predicted FG% from v3
            .groups = "drop") %>%
  filter(fga >= 50) %>%
  mutate(
    # Expected Binomial variance of FG%
    binom_var = p_skill * (1 - p_skill) / fga,
    binom_sd = sqrt(binom_var),
    # Observed residual
    residual = fg_obs - p_skill,
    # Standardized residual: if Binomial is correct, should be ~ N(0,1)
    z_residual = residual / binom_sd)

cat("Standardized residual summary (should be ~N(0,1) if no overdispersion):\n")
cat(" Mean: ", round(mean(player_resid$z_residual, na.rm = TRUE), 3), "\n")
cat(" SD: ", round(sd(player_resid$z_residual, na.rm = TRUE), 3), "\n")
cat(" Kurtosis: ", round(moments::kurtosis(player_resid$z_residual) - 3, 3),
    " (excess, 0 = normal)\n")
cat(" |z| > 2: ", round(100 * mean(abs(player_resid$z_residual) > 2, na.rm = TRUE), 1),
    "% (expected: 4.6%)\n")
cat(" |z| > 3: ", round(100 * mean(abs(player_resid$z_residual) > 3, na.rm = TRUE), 1),
    "% (expected: 0.3%)\n\n")

## Visual: QQ-plot of standardized residuals
p_qq <- ggplot(player_resid, aes(sample = z_residual)) +
  stat_qq(alpha = 0.3, size = 1) +
  stat_qq_line(color = "red") +
  facet_wrap(~shot_family, ncol = 3) +
  labs(title = "QQ-Plot of Standardized Residuals (v3)",
       subtitle = "Heavier tails than the line → overdispersion exists",
       x = "Theoretical quantiles", y = "Sample quantiles") +
  theme_minimal()
p_resid_hist <- ggplot(player_resid, aes(x = z_residual)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 40, fill = "steelblue", color = "white", alpha = 0.7) +
  stat_function(fun = dnorm, color = "red", linewidth = 0.8) +
  facet_wrap(~shot_family, ncol = 3) +
  labs(title = "Standardized Residual Distribution (v3)",
       subtitle = "Red = N(0,1). Wider shoulders → overdispersion",
       x = "Standardized residual", y = "Density") +
  theme_minimal()
p_od_diag <- p_qq / p_resid_hist +
  plot_annotation(title = "Overdispersion Diagnostic: v3 Residuals")
ggsave("overdispersion_diagnostic_v3.png", p_od_diag,
       width = 12, height = 10, dpi = 150)
cat("Diagnostic plot saved: overdispersion_diagnostic_v3.png\n\n")

## Decision gate
resid_sd <- sd(player_resid$z_residual, na.rm = TRUE)
if (resid_sd < 1.10) {
  cat("NOTE: Residual SD =", round(resid_sd, 3),
      "— mild or no overdispersion detected.\n")
  cat("Fitting v4 anyway for comparison, but sigma_od may collapse to ~0.\n\n")
} else {
  cat("OVERDISPERSION DETECTED: Residual SD =", round(resid_sd, 3),
      "(expected 1.0).\n")
  cat("This justifies the v4 overdispersion extension.\n\n")
}





## ═════════════════════════════════════════════════════════════════════════════
## 02 — COMPILE & FIT v4 =======================================================
## ═════════════════════════════════════════════════════════════════════════════
cat("═══ FITTING v4 MODEL ═══\n\n")
m_v4 <- cmdstan_model("stan/nba_xfg_v4.stan",
                      cpp_options = list(stan_threads = TRUE))

## Same data as v3 — no changes needed.
## v4 adds parameters but the data block is identical.
fit_v4 <- m_v4$sample(data = stan_data,
                      chains = 4,
                      parallel_chains = 4,
                      threads_per_chain = 4,
                      iter_warmup = 600,
                      iter_sampling = 600,
                      refresh = 100,
                      adapt_delta = 0.92, # slightly higher for extra params
                      max_treedepth = 10)





## ═════════════════════════════════════════════════════════════════════════════
## 03 — v4 DIAGNOSTICS =========================================================
## ═════════════════════════════════════════════════════════════════════════════
fit_v4$cmdstan_diagnose()

cat("\n═══ v4 CALIBRATION PARAMETERS ═══\n")
print(fit_v4$summary(variables = c(paste0("cal_intercept[", 1:3, "]"),
                                   paste0("cal_slope[", 1:3, "]"))))

cat("\n═══ v4 HIERARCHICAL SDs ═══\n")
print(fit_v4$summary(variables = c(paste0("sigma_player[", 1:3, "]"),
                                   paste0("sigma_defender[", 1:3, "]"),
                                   paste0("sigma_defteam[", 1:3, "]"))))

cat("\n═══ v4 STOCHASTIC VOLUME (mu_volume_raw) ═══\n")
cat(" LogNormal prior median = 0.10. Posterior tells you how strongly\n")
cat(" volume predicts ability per family.\n\n")
print(fit_v4$summary(variables = paste0("mu_volume_raw[", 1:3, "]")))

cat("\n═══ v4 OVERDISPERSION SDs (sigma_od) ═══\n")
cat(" Near 0 = no overdispersion needed. > 0.05 = meaningful.\n\n")
print(fit_v4$summary(variables = paste0("sigma_od[", 1:3, "]")))





## ═════════════════════════════════════════════════════════════════════════════
## 04 — COMPARE SIGMA POSTERIORS: v3 vs v4 =====================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Key question: does sigma_player shrink when sigma_od is added?
## If yes, v3 was attributing overdispersion to skill — sigma_od is doing
## the right thing by absorbing the noise.
sigma_v3 <- fit_v3$draws(variables = paste0("sigma_player[", 1:3, "]"),
                         format = "draws_df") %>%
  pivot_longer(-c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "value") %>%
  mutate(model = "v3")
sigma_v4 <- fit_v4$draws(variables = paste0("sigma_player[", 1:3, "]"),
                         format = "draws_df") %>%
  pivot_longer(-c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "value") %>%
  mutate(model = "v4")
# Overdispersion draws
od_draws <- fit_v4$draws(variables = paste0("sigma_od[", 1:3, "]"),
                         format = "draws_df") %>%
  pivot_longer(-c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "value") %>%
  mutate(model = "v4 (sigma_od)")
sigma_compare <- bind_rows(sigma_v3, sigma_v4) %>%
  mutate(shot_family = case_when(str_detect(parameter, "\\[1\\]") ~ "rim",
                                 str_detect(parameter, "\\[2\\]") ~ "j2",
                                 str_detect(parameter, "\\[3\\]") ~ "j3"))
od_draws <- od_draws %>%
  mutate(shot_family = case_when(str_detect(parameter, "\\[1\\]") ~ "rim",
                                 str_detect(parameter, "\\[2\\]") ~ "j2",
                                 str_detect(parameter, "\\[3\\]") ~ "j3"))
p_sigma_compare <- ggplot(sigma_compare, aes(x = value, fill = model)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~shot_family, ncol = 3, scales = "free_x") +
  labs(title = "sigma_player: v3 vs v4",
       subtitle = "If v4 < v3, overdispersion was inflating skill variance in v3",
       x = "σ_player (logit scale)", fill = NULL) +
  theme_minimal() +
  theme(legend.position = "bottom")
p_od <- ggplot(od_draws, aes(x = value, fill = shot_family)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~shot_family, ncol = 3) +
  labs(title = "sigma_od posterior (v4)",
       subtitle = "Near 0 = no overdispersion | > 0.05 = meaningful extra noise",
       x = "σ_od (logit scale)") +
  theme_minimal() +
  theme(legend.position = "none")
p_v3v4 <- p_sigma_compare / p_od +
  plot_annotation(title = "v3 vs v4: Overdispersion Decomposition")
ggsave("v3_vs_v4_sigma.png", p_v3v4, width = 12, height = 10, dpi = 150)
cat("\nSigma comparison saved: v3_vs_v4_sigma.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 05 — COMPARE STOCHASTIC VOLUME: v3 mu_volume vs v4 mu_volume_raw ===========
## ═════════════════════════════════════════════════════════════════════════════
vol_v3 <- fit_v3$draws(variables = paste0("mu_volume[", 1:3, "]"),
                       format = "draws_df") %>%
  pivot_longer(-c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "value") %>%
  mutate(model = "v3 (Normal prior)",
         shot_family = case_when(str_detect(parameter, "\\[1\\]") ~ "rim",
                                 str_detect(parameter, "\\[2\\]") ~ "j2",
                                 str_detect(parameter, "\\[3\\]") ~ "j3"))
vol_v4 <- fit_v4$draws(variables = paste0("mu_volume_raw[", 1:3, "]"),
                       format = "draws_df") %>%
  pivot_longer(-c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "value") %>%
  mutate(model = "v4 (LogNormal prior)",
         shot_family = case_when(str_detect(parameter, "\\[1\\]") ~ "rim",
                                 str_detect(parameter, "\\[2\\]") ~ "j2",
                                 str_detect(parameter, "\\[3\\]") ~ "j3"))
p_vol <- ggplot(bind_rows(vol_v3, vol_v4), aes(x = value, fill = model)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~shot_family, ncol = 3, scales = "free_x") +
  geom_vline(xintercept = 0, linetype = "dashed", color = "grey50") +
  labs(title = "Volume Coefficient: v3 (Normal) vs v4 (LogNormal)",
       subtitle = "v4 constrains positive; larger = volume more predictive of skill",
       x = "mu_volume (logit scale)", fill = NULL) +
  theme_minimal() +
  theme(legend.position = "bottom")
ggsave("v3_vs_v4_volume.png", p_vol, width = 12, height = 5, dpi = 150)
cat("Volume comparison saved: v3_vs_v4_volume.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 06 — LOO-CV COMPARISON: v3 vs v4 ============================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Use pointwise log-likelihood from generated quantities.
## Since both models omit generated quantities for speed, we compute
## log_lik manually from the posterior draws.
cat("\n═══ LOO-CV COMPARISON ═══\n")
cat("Computing pointwise log-lik from posterior draws...\n")
cat("(This is memory-intensive for large N. Subsample if needed.)\n\n")
compute_log_lik <- function(fit, stan_data, model_name, has_od = FALSE) {
  ## Extract all draws
  dm <- fit$draws(format = "draws_matrix")
  n_draws <- nrow(dm)
  N <- stan_data$N
  ## For memory: process in chunks of shots
  ## Each draw d, each shot n: log_lik[d,n] = bernoulli_logit_lpmf(y[n] | eta[d,n])
  ## Pre-extract global parameters
  cal_int <- dm[, paste0("cal_intercept[", 1:3, "]")]
  cal_sl <- dm[, paste0("cal_slope[", 1:3, "]")]
  ## Process in chunks to avoid N × n_draws memory blowup
  chunk_size <- 5000
  n_chunks <- ceiling(N / chunk_size)
  log_lik_list <- vector("list", n_chunks)
  for (ch in seq_len(n_chunks)) {
    idx_start <- (ch - 1) * chunk_size + 1
    idx_end <- min(ch * chunk_size, N)
    n_sub <- idx_end - idx_start + 1
    # Pre-allocate matrix for this chunk
    ll_chunk <- matrix(0, nrow = n_draws, ncol = n_sub)
    for (j in seq_len(n_sub)) {
      n <- idx_start + j - 1
      t <- stan_data$shot_type[n]
      p_idx <- stan_data$player[n]
      d_idx <- stan_data$defender[n]
      dt_idx <- stan_data$defteam[n]
      a_p <- dm[, paste0("a_player[", p_idx, ",", t, "]")]
      a_d <- dm[, paste0("a_defender[", d_idx, ",", t, "]")]
      a_dt <- dm[, paste0("a_defteam[", dt_idx, ",", t, "]")]
      eta <- cal_int[, t] + cal_sl[, t] * stan_data$xfg_logit[n] +
        a_p + a_d + a_dt
      if (has_od) {
        eps <- dm[, paste0("eps_player[", p_idx, ",", t, "]")]
        eta <- eta + eps
      }
      # bernoulli_logit log-pmf
      y_n <- stan_data$y[n]
      if (y_n == 1) {
        ll_chunk[, j] <- -log1p(exp(-eta))
      } else {
        ll_chunk[, j] <- -log1p(exp(eta))
      }
    }
    log_lik_list[[ch]] <- ll_chunk
    if (ch %% 10 == 0) cat(" chunk", ch, "/", n_chunks, "\n")
  }
  log_lik <- do.call(cbind, log_lik_list)
  cat(model_name, ": log_lik matrix", nrow(log_lik), "×", ncol(log_lik), "\n")
  return(log_lik)
}

## NOTE: For large N (>100k shots), computing full pointwise log-lik is slow.
## Consider subsampling: take a random 20k shots and compare LOO on those.
## The relative ranking (v3 vs v4) will be stable.
USE_SUBSAMPLE <- TRUE
SUBSAMPLE_N <- 20000

if (USE_SUBSAMPLE && stan_data$N > SUBSAMPLE_N) {
  set.seed(42)
  sub_idx <- sort(sample(stan_data$N, SUBSAMPLE_N))
  stan_data_sub <- stan_data
  stan_data_sub$N <- SUBSAMPLE_N
  stan_data_sub$y <- stan_data$y[sub_idx]
  stan_data_sub$xfg_logit <- stan_data$xfg_logit[sub_idx]
  stan_data_sub$shot_type <- stan_data$shot_type[sub_idx]
  stan_data_sub$player <- stan_data$player[sub_idx]
  stan_data_sub$defender <- stan_data$defender[sub_idx]
  stan_data_sub$defteam <- stan_data$defteam[sub_idx]
  cat("Using subsample of", SUBSAMPLE_N, "shots for LOO comparison\n\n")
  data_for_loo <- stan_data_sub
} else {
  data_for_loo <- stan_data
}

log_lik_v3 <- compute_log_lik(fit_v3, data_for_loo, "v3", has_od = FALSE)
log_lik_v4 <- compute_log_lik(fit_v4, data_for_loo, "v4", has_od = TRUE)
loo_v3 <- loo(log_lik_v3, cores = parallel::detectCores())
loo_v4 <- loo(log_lik_v4, cores = parallel::detectCores())

cat("\n═══ LOO-CV RESULTS ═══\n\n")
cat("v3 (original):\n")
print(loo_v3)
cat("\nv4 (overdispersion + stochastic volume):\n")
print(loo_v4)
cat("\n═══ LOO COMPARISON ═══\n")
comp <- loo_compare(loo_v3, loo_v4)
print(comp)
cat("\nInterpretation:\n")
cat(" Positive elpd_diff for v4 = v4 is better\n")
cat(" |elpd_diff| / se_diff > 2 = significant difference\n\n")





## ═════════════════════════════════════════════════════════════════════════════
## 07 — RE-RUN PPC CALIBRATION ON v4 ==========================================
## ═════════════════════════════════════════════════════════════════════════════
##
## If v4 wins on LOO, check whether it also fixes the calibration problem.
## Source the 03a script logic but using v4 draws.



draws_mat_v4 <- fit_v4$draws(format = "draws_matrix")
## Compute p_skill for v4 (with overdispersion included in eta)
ppc_results_v4 <- bind_rows(lapply(seq_along(player_groups), 
                                function(i) {
                                  if (i %% 100 == 0) 
                                    cat("  ", i, "/", 
                                        length(player_groups), 
                                        "\n")
                                  compute_ppc_quantile(player_groups[[i]], draws_mat_v4,
                                                       cal_intercept_draws, cal_slope_draws)
                                })
)

cal_hat_v4 <- colMeans(draws_mat_v4[, c(paste0("cal_intercept[", 1:3, "]"),
                                        paste0("cal_slope[", 1:3, "]"))])
a_player_hat_v4 <- matrix(0, nrow = stan_data$J_player, ncol = 3)
eps_hat_v4 <- matrix(0, nrow = stan_data$J_player, ncol = 3)
for (t in 1:3) {
  cols_a <- paste0("a_player[", 1:stan_data$J_player, ",", t, "]")
  cols_e <- paste0("eps_player[", 1:stan_data$J_player, ",", t, "]")
  a_player_hat_v4[, t] <- colMeans(draws_mat_v4[, cols_a])
  eps_hat_v4[, t] <- colMeans(draws_mat_v4[, cols_e])
}
shots_v4 <- shots %>%
  mutate(
    eta_context_v4 = cal_hat_v4[paste0("cal_intercept[", shot_type, "]")] +
      cal_hat_v4[paste0("cal_slope[", shot_type, "]")] * xfg_logit,
    p_context_v4 = inv_logit(eta_context_v4),
    a_player_v4 = a_player_hat_v4[cbind(player_idx, shot_type)],
    eps_player_v4 = eps_hat_v4[cbind(player_idx, shot_type)],
    p_skill_v4 = inv_logit(eta_context_v4 + a_player_v4 + eps_player_v4)
  )
## Two-stage calibration plot for v4
cal_check_v4 <- shots_v4 %>%
  mutate(p_bin = cut(p_skill_v4, breaks = seq(0, 1, by = 0.05))) %>%
  group_by(p_bin) %>%
  summarise(n = n(), predicted = mean(p_skill_v4),
            observed = mean(fgm), .groups = "drop") %>%
  filter(n >= 50)
p_cal_v4 <- ggplot(cal_check_v4, aes(x = predicted, y = observed)) +
  geom_point(aes(size = n), alpha = 0.7, color = "darkorange") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
  scale_size_continuous(range = c(1, 8)) +
  labs(title = "Two-Stage Calibration: v4 (with overdispersion)",
       x = "Predicted FG%", y = "Observed FG%") +
  theme_minimal()
ggsave("twostage_calibration_v4.png", p_cal_v4, width = 7, height = 6, dpi = 150)
cat("v4 calibration plot saved: twostage_calibration_v4.png\n")




calibration_table_v4 <- tibble(percentile = paste0(100 * percentiles_to_check, "th"),
                            expected = paste0(round(100 * (1 - percentiles_to_check), 1), "%"),
                            pct_exceeded = sapply(percentiles_to_check, function(q) {
                              mean(ppc_results_v4$pit_value > q)
                            }),
                            n_exceeded = sapply(percentiles_to_check, function(q) {
                              sum(ppc_results_v4$pit_value > q)
                            }),
                            total = nrow(ppc_results_v4)) %>%
  mutate(observed = paste0(round(100 * pct_exceeded, 1), "%"),
         breakdown = paste0(n_exceeded, "/", total),
         # Deviation from expectation (positive = overconfident)
         deviation = pct_exceeded - (1 - percentiles_to_check))

cat("═══ POSTERIOR PREDICTIVE CALIBRATION TABLE ═══\n")
cat(" (All shot families pooled, min", MIN_FGA, "FGA)\n\n")
cat(sprintf("%-12s %-10s %-10s %-12s %-10s\n",
            "Percentile", "Expected", "Observed", "Breakdown", "Deviation"))
cat(strrep("─", 58), "\n")
for (i in seq_len(nrow(calibration_table_v4))) {
  r <- calibration_table_v4[i, ]
  cat(sprintf("%-12s %-10s %-10s %-12s %+.1f pp\n",
              r$percentile, r$expected, r$observed, r$breakdown,
              100 * r$deviation))
}

## ─── By shot family ───
cat("\n\n═══ CALIBRATION BY SHOT FAMILY ═══\n")

for (fam in c("rim", "j2", "j3")) {
  ppc_fam <- ppc_results_v4 %>% filter(shot_family == fam)
  cat("\n── ", toupper(fam), " (n =", nrow(ppc_fam), ") ──\n")
  cat(sprintf("%-12s %-10s %-10s\n", "Percentile", "Expected", "Observed"))
  cat(strrep("─", 36), "\n")
  for (q in c(0.95, 0.90, 0.50, 0.10, 0.05)) {
    obs <- mean(ppc_fam$pit_value > q)
    cat(sprintf("%-12s %-10s %-10s\n",
                paste0(100 * q, "th"),
                paste0(round(100 * (1 - q), 1), "%"),
                paste0(round(100 * obs, 1), "%")))
  }
}


## ═════════════════════════════════════════════════════════════════════════════
## 04 — PIT HISTOGRAM (should be uniform if well-calibrated) ==================
## ═════════════════════════════════════════════════════════════════════════════

p_pit_all_v4 <- ggplot(ppc_results_v4, aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 20, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red", linewidth = 0.8) +
  labs(title = "Posterior Predictive PIT Histogram (All Families)",
       subtitle = paste0("Uniform = well-calibrated | n = ", nrow(ppc_results_v4),
                         " player-families, min ", MIN_FGA, " FGA"),
       x = "PIT value (quantile of observed in posterior predictive)",
       y = "Density") +
  theme_minimal()

p_pit_by_fam_v4 <- ggplot(ppc_results_v4, aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 20, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red", linewidth = 0.8) +
  facet_wrap(~shot_family, ncol = 3) +
  labs(title = "PIT Histogram by Shot Family",
       subtitle = "U-shape = overconfident | Inverted-U = underconfident | Skew = bias",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_combined_v4 <- p_pit_all_v4 / p_pit_by_fam_v4 +
  plot_annotation(title = "Posterior Predictive Calibration Diagnostics")

ggsave("ppc_calibration.png", p_pit_combined, width = 12, height = 10, dpi = 150)
cat("\n\nPIT histogram saved: ppc_calibration.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 05 — CALIBRATION CURVE (observed quantile vs expected quantile) =============
## ═════════════════════════════════════════════════════════════════════════════
##
## If the model is perfectly calibrated, the ECDF of PIT values should be
## the identity line. Deviations show where the model is off.

p_ecdf_v4 <- ggplot(ppc_results_v4, aes(x = pit_value, color = shot_family)) +
  stat_ecdf(linewidth = 0.8) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey40") +
  labs(title = "PIT ECDF by Shot Family",
       subtitle = "Diagonal = perfect calibration",
       x = "Expected quantile (PIT value)",
       y = "Observed proportion ≤ quantile",
       color = "Shot Family") +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave("ppc_ecdf.png", p_ecdf, width = 8, height = 7, dpi = 150)
cat("PIT ECDF saved: ppc_ecdf.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 06 — VOLUME-STRATIFIED CALIBRATION ==========================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Are low-volume players worse calibrated than high-volume ones?
## This is directly analogous to Osborn's concern about small-sample
## instability and validates that the volume prior is doing its job.

ppc_results_v4 <- ppc_results_v4 %>%
  mutate(volume_bin = case_when(fga < 100 ~ "50-99 FGA",
                                fga < 250 ~ "100-249 FGA",
                                fga < 500 ~ "250-499 FGA",
                                TRUE ~ "500+ FGA") %>% 
           factor(levels = c("50-99 FGA", "100-249 FGA",
                             "250-499 FGA", "500+ FGA")))

p_pit_volume_v4 <- ggplot(ppc_results_v4, aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by Shot Volume",
       subtitle = "Low-volume players should still be well-calibrated if priors are good",
       x = "PIT value", y = "Density") +
  theme_minimal()

ggsave("ppc_calibration_by_volume.png", p_pit_volume,
       width = 10, height = 8, dpi = 150)
cat("Volume-stratified PIT saved: ppc_calibration_by_volume.png\n")


p_pit_volume_rim_v4 <- ggplot(ppc_results_v4 %>% filter(shot_family == "rim"), aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by Rim Shot Volume",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_volume_j2_v4 <- ggplot(ppc_results_v4 %>% filter(shot_family == "j2"), aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by Midrange Shot Volume",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_volume_j3_v4 <- ggplot(ppc_results_v4 %>% filter(shot_family == "j3"), aes(x = pit_value)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 15, fill = "steelblue", color = "white", alpha = 0.8) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "red") +
  facet_wrap(~volume_bin, ncol = 2) +
  labs(title = "PIT Histogram Stratified by 3-point Shot Volume",
       x = "PIT value", y = "Density") +
  theme_minimal()

p_pit_combined <- p_pit_volume_rim / p_pit_volume_j2 / p_pit_volume_j3 +
  plot_annotation(title = "PIT Histogram Stratified by Shot Family Volume")

ggsave("ppc_calibration_by_rim_volume.png", p_pit_volume_rim,
       width = 10, height = 8, dpi = 150)
ggsave("ppc_calibration_by_j2_volume.png", p_pit_volume_j2,
       width = 10, height = 8, dpi = 150)
ggsave("ppc_calibration_by_j3_volume.png", p_pit_volume_j3,
       width = 10, height = 8, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 07 — INDIVIDUAL EXTREME CASES ===============================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Which players had outcomes that the model found very surprising?
## PIT near 0 = performed much worse than expected
## PIT near 1 = performed much better than expected

extreme_low <- ppc_results_v4 %>%
  filter(pit_value < 0.025) %>%
  left_join(player_map, by = "player_idx") %>%
  arrange(pit_value) %>%
  select(player_name_full, shot_family, fga, fg_pct_obs, p_mean, pit_value)

extreme_high <- ppc_results_v4 %>%
  filter(pit_value > 0.975) %>%
  left_join(player_map, by = "player_idx") %>%
  arrange(desc(pit_value)) %>%
  select(player_name_full, shot_family, fga, fg_pct_obs, p_mean, pit_value)

cat("\n═══ EXTREME UNDERPERFORMERS (PIT < 2.5%) ═══\n")
print(head(extreme_low, 15), n = 15)
cat("\n═══ EXTREME OVERPERFORMERS (PIT > 97.5%) ═══\n")
print(head(extreme_high, 15), n = 15)





## ═════════════════════════════════════════════════════════════════════════════
## 08 — SAVE v4 OUTPUTS ========================================================
## ═════════════════════════════════════════════════════════════════════════════
saveRDS(fit_v4, "stan_fit_v4.rds")
saveRDS(draws_mat_v4, "draws_v4_twostage.rds")
cat("\n── v4 fitting & comparison complete ──\n")
cat(" stan_fit_v4.rds — CmdStan fit object\n")
cat(" draws_v4_twostage.rds — posterior draws matrix\n")
cat(" overdispersion_diagnostic_v3.png — residual QQ/hist\n")
cat(" v3_vs_v4_sigma.png — sigma decomposition\n")
cat(" v3_vs_v4_volume.png — volume coefficient comparison\n")
cat(" twostage_calibration_v4.png — v4 bin-level calibration\n")
cat("\nNext: re-run 03a_posterior_predictive_calibration.R with v4 draws\n")
cat("to produce the updated Osborn Table 2 and compare.\n")