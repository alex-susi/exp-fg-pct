## ═════════════════════════════════════════════════════════════════════════════
## 03_stan_random_effects.R
## NBA xFG — Stage 2: Hierarchical random effects via Stan
##
## Takes XGBoost xFG predictions from Stage 1 and estimates:
##   - Player shooting skill (by shot type: rim/j2/j3)
##   - Individual defender impact (by shot type)
##   - Defensive team scheme effects (by shot type)
##
## These are the quantities we actually care about for player evaluation.
## XGBoost handles "how hard was this shot?", Stan handles "who is good?"
##
## Inputs:  shots_with_xfg.csv (from 02_xgb_shot_difficulty.R)
## Outputs: player/defender/team effect tables, EPAA, diagnostics
## ═════════════════════════════════════════════════════════════════════════════


## ═════════════════════════════════════════════════════════════════════════════
## 00 — CONFIGURE   ============================================================
## ═════════════════════════════════════════════════════════════════════════════

library(dplyr)
library(readr)
library(tidyr)
library(stringr)
library(cmdstanr)
library(posterior)
library(loo)
library(ggplot2)
library(ggrepel)
library(patchwork)
library(scales)

set_cmdstan_path(path = cmdstanr::cmdstan_path())
options(mc.cores = parallel::detectCores())
inv_logit <- function(x) 1 / (1 + exp(-x))





## ═════════════════════════════════════════════════════════════════════════════
## 01 — LOAD DATA ==============================================================
## ═════════════════════════════════════════════════════════════════════════════

shots <- read_csv("shots_with_xfg.csv", show_col_types = FALSE)

cat("Loaded", nrow(shots), "shots with XGBoost xFG predictions\n")
cat("  xFG range:", round(min(shots$xfg, na.rm = TRUE), 3), "—",
    round(max(shots$xfg, na.rm = TRUE), 3), "\n")
cat("  Mean xFG:", round(mean(shots$xfg, na.rm = TRUE), 4),
    "  Obs FG%:", round(mean(shots$fgm, na.rm = TRUE), 4), "\n")





## ═════════════════════════════════════════════════════════════════════════════
## 02 — BUILD STAN INDICES =====================================================
## ═════════════════════════════════════════════════════════════════════════════
## Re-index player, defender, defteam as contiguous 1..J integers.
## Defender = imputed_def_id from the pipeline.

shots <- shots %>%
  mutate(player_idx   = as.integer(factor(player1_id)),
         defender_idx = as.integer(factor(defender_id)),
         defteam_idx  = as.integer(factor(defender_team)),
         shot_type    = case_when(shot_family == "rim" ~ 1L,
                                  shot_family == "j2"  ~ 2L,
                                  shot_family == "j3"  ~ 3L)) %>% 
  filter(!is.na(xfg_logit))

# Mapping tables for later extraction
player_map <- shots %>%
  distinct(player_idx, player1_id, player_name) %>%
  arrange(player_idx)

defender_map <- shots %>%
  filter(!is.na(defender_idx)) %>%
  distinct(defender_idx, defender_id, defender_name) %>%
  arrange(defender_idx)

defteam_map <- shots %>%
  filter(!is.na(defteam_idx)) %>%
  distinct(defteam_idx, defender_team) %>%
  arrange(defteam_idx)

cat("\nStan indices:\n")
cat("  J_player   =", max(shots$player_idx), "\n")
cat("  J_defender  =", max(shots$defender_idx, na.rm = TRUE), "\n")
cat("  J_defteam  =", max(shots$defteam_idx, na.rm = TRUE), "\n")





## ═════════════════════════════════════════════════════════════════════════════
## 03 — PREPARE STAN DATA ======================================================
## ═════════════════════════════════════════════════════════════════════════════

stan_data <- list(N         = nrow(shots),
                  y         = as.integer(shots$fgm),
                  xfg_logit = shots$xfg_logit,
                  shot_type = shots$shot_type,
                
                  J_player  = max(shots$player_idx),
                  player    = shots$player_idx,
                
                  J_defender = max(shots$defender_idx, na.rm = TRUE),
                  defender   = shots$defender_idx,
                
                  J_defteam = max(shots$defteam_idx, na.rm = TRUE),
                  defteam   = shots$defteam_idx,
                
                  grainsize = 250)

cat("\nStan data prepared: N =", stan_data$N, "\n")





## ═════════════════════════════════════════════════════════════════════════════
## 04 — COMPILE & SAMPLE =======================================================
## ═════════════════════════════════════════════════════════════════════════════

m_v3 <- cmdstan_model("stan/nba_xfg_v3_twostage.stan",
                      cpp_options = list(stan_threads = TRUE))

fit_v3 <- m_v3$sample(data             = stan_data,
                      chains           = 4,
                      parallel_chains  = 4,
                      threads_per_chain = 4,
                      iter_warmup      = 600,
                      iter_sampling    = 600,
                      refresh          = 100,
                      adapt_delta      = 0.90,
                      max_treedepth    = 10)





## ═════════════════════════════════════════════════════════════════════════════
## 05 — DIAGNOSTICS ============================================================
## ═════════════════════════════════════════════════════════════════════════════

fit_v3$cmdstan_diagnose()

# Calibration parameters: how well does XGBoost's logit transfer?
cal_params <- c(paste0("cal_intercept[", 1:3, "]"),
                paste0("cal_slope[", 1:3, "]"))

cat("\n═══ CALIBRATION PARAMETERS ═══\n")
cat("  (intercept ≈ 0, slope ≈ 1 means XGBoost is well-calibrated)\n\n")
print(fit_v3$summary(variables = cal_params))

# Hierarchical SDs
sigma_params <- c(paste0("sigma_player[", 1:3, "]"),
                  paste0("sigma_defender[", 1:3, "]"),
                  paste0("sigma_defteam[", 1:3, "]"))

cat("\n═══ HIERARCHICAL SDs ═══\n")
print(fit_v3$summary(variables = sigma_params))





## ═════════════════════════════════════════════════════════════════════════════
## 06 — EXTRACT POSTERIOR DRAWS ================================================
## ═════════════════════════════════════════════════════════════════════════════

draws_mat <- fit_v3$draws(format = "draws_matrix")
cat("\nDraws matrix:", nrow(draws_mat), "draws ×", ncol(draws_mat), "parameters\n")





## ═════════════════════════════════════════════════════════════════════════════
## 07 — EXTRACT RANDOM EFFECTS (posterior mean + 90% CI) =======================
## ═════════════════════════════════════════════════════════════════════════════

extract_effects <- function(draws_mat, prefix, J, type_labels = c("rim", "j2", "j3")) {
  # prefix: "a_player", "a_defender", or "a_defteam"
  bind_rows(lapply(1:3, function(t) {
    cols <- paste0(prefix, "[", 1:J, ",", t, "]")
    mat  <- draws_mat[, cols, drop = FALSE]

    tibble(idx         = 1:J,
           shot_type   = t,
           shot_family = type_labels[t],
           effect_mean = colMeans(mat),
           effect_sd   = apply(mat, 2, sd),
           effect_q05  = apply(mat, 2, quantile, 0.05),
           effect_q25  = apply(mat, 2, quantile, 0.25),
           effect_q75  = apply(mat, 2, quantile, 0.75),
           effect_q95  = apply(mat, 2, quantile, 0.95),
           # Probability of being above/below average
           prob_above  = colMeans(mat > 0),
           prob_below  = colMeans(mat < 0))
  }))
}

## ─── Player effects ───
player_effects <- extract_effects(draws_mat, "a_player", stan_data$J_player) %>%
  left_join(player_map, by = c("idx" = "player_idx")) %>% 
  select(-shot_type) %>% 
  relocate(player1_id, .after = idx) %>% 
  relocate(player_name, .after = idx) %>% 
  mutate(across(where(is.numeric), round, 3)) %>% 
  as.data.frame()

## ─── Defender effects ───
defender_effects <- extract_effects(draws_mat, "a_defender", stan_data$J_defender) %>%
  left_join(defender_map, by = c("idx" = "defender_idx")) %>% 
  select(-shot_type) %>% 
  relocate(defender_id, .after = idx) %>% 
  relocate(defender_name, .after = idx) %>% 
  mutate(across(where(is.numeric), round, 3)) %>% 
  as.data.frame()

## ─── Defensive team effects ───
defteam_effects <- extract_effects(draws_mat, "a_defteam", stan_data$J_defteam) %>%
  left_join(defteam_map, by = c("idx" = "defteam_idx")) %>% 
  select(-shot_type) %>% 
  relocate(defender_team, .after = idx) %>% 
  mutate(across(where(is.numeric), round, 3)) %>% 
  as.data.frame()





## ═════════════════════════════════════════════════════════════════════════════
## 08 — PLAYER SHOOTING SKILL TABLES ===========================================
## ═════════════════════════════════════════════════════════════════════════════

# Shot volume per player per family (for context)
player_volume <- shots %>%
  group_by(player_idx, shot_family) %>%
  summarise(fga = n(), fg_pct = mean(fgm), mean_xfg = mean(xfg), .groups = "drop")

player_tbl <- player_effects %>%
  left_join(player_volume, by = c("idx" = "player_idx", "shot_family")) %>%
  # Convert logit-scale effect to approximate % point impact
  # At league average FG%: rim ~60%, j2 ~42%, j3 ~36%
  # d(inv_logit)/dx at these baselines:
  mutate(baseline_p = case_when(shot_family == "rim" ~ 0.65,
                                shot_family == "j2"  ~ 0.42,
                                shot_family == "j3"  ~ 0.36),
         effect_pp = effect_mean * baseline_p * (1 - baseline_p) * 100) %>% # approx pp
  select(-baseline_p)

# Top shooters by family
for (fam in c("rim", "j2", "j3")) {
  cat("\n═══ TOP 15 PLAYERS:", toupper(fam), "(min 100 FGA, by posterior mean) ═══\n")
  player_tbl %>%
    filter(shot_family == fam, fga >= 100) %>%
    arrange(desc(effect_mean)) %>%
    select(player_name, fga, fg_pct, mean_xfg, effect_mean, effect_pp,
           effect_q05, effect_q95, prob_above) %>%
    head(15) %>%
    mutate(across(where(is.numeric), ~ round(., 3))) %>%
    print(n = 15)
}





## ═════════════════════════════════════════════════════════════════════════════
## 09 — EXPECTED POINTS ABOVE AVERAGE (EPAA) ===================================
## ═════════════════════════════════════════════════════════════════════════════
##
## For each shot: EPAA = (p_with_skill - p_context_only) × point_value
##
## p_context_only:  inv_logit(cal_intercept + cal_slope × xfg_logit)
## p_with_skill:    inv_logit(cal_intercept + cal_slope × xfg_logit + a_player)
##
## Using posterior means for point estimates (could also do full posterior).

cal_hat <- colMeans(draws_mat[, cal_params, drop = FALSE])

# Player effect posterior means: matrix [J_player × 3]
a_player_hat <- matrix(0, nrow = stan_data$J_player, ncol = 3)
for (t in 1:3) {
  cols <- paste0("a_player[", 1:stan_data$J_player, ",", t, "]")
  a_player_hat[, t] <- colMeans(draws_mat[, cols, drop = FALSE])
}

shots <- shots %>%
  mutate(# Calibrated baseline (average player at this shot difficulty)
         eta_context = cal_hat[paste0("cal_intercept[", shot_type, "]")] +
           cal_hat[paste0("cal_slope[", shot_type, "]")] * xfg_logit,
         p_context = inv_logit(eta_context),
         
         # With player skill
         a_player_val = a_player_hat[cbind(player_idx, shot_type)],
         p_skill = inv_logit(eta_context + a_player_val),
         
         # EPAA per shot
         point_value = if_else(is_three == 1, 3, 2),
         epaa = (p_skill - p_context) * point_value)

# Aggregate EPAA
epaa_tbl <- shots %>%
  group_by(player1_id, player_name, shot_family) %>%
  summarise(fga           = n(),
            fg_pct        = mean(fgm),
            xfg_pct       = mean(xfg),
            cal_xfg       = mean(p_context),    # Stan-recalibrated
            skill_fg      = mean(p_skill),      # with player effect
            epaa_total    = sum(epaa),
            epaa_per100   = 100 * epaa_total / fga,
            .groups = "drop") %>% 
  as.data.frame() %>% 
  mutate(across(where(is.numeric), ~ round(., 3)))

# Overall EPAA (across all shot types)
epaa_overall <- shots %>%
  group_by(player1_id, player_name) %>%
  summarise(fga           = n(),
            fg_pct        = mean(fgm),
            xfg_pct       = mean(xfg),
            epaa_total    = sum(epaa),
            epaa_per100   = 100 * epaa_total / fga,
            .groups = "drop")

cat("\n═══ TOP 20 OVERALL EPAA (min 300 FGA) ═══\n")
epaa_overall %>%
  #filter(fga >= 300) %>%
  arrange(desc(epaa_total)) %>%
  head(20) %>%
  mutate(across(where(is.numeric), ~ round(., 3))) %>%
  print(n = 20)

cat("\n═══ TOP 15 THREE-POINT SHOOTERS (min 150 3PA) ═══\n")
epaa_tbl %>%
  filter(shot_family == "j3", fga >= 150) %>%
  arrange(desc(epaa_total)) %>%
  head(15) %>%
  mutate(across(where(is.numeric), ~ round(., 3))) %>%
  print(n = 15)





## ═════════════════════════════════════════════════════════════════════════════
## 10 — DEFENDER IMPACT TABLES =================================================
## ═════════════════════════════════════════════════════════════════════════════

defender_volume <- shots %>%
  group_by(defender_idx, shot_family) %>%
  summarise(contests = n(), opp_fg = mean(fgm), .groups = "drop")

defender_tbl <- defender_effects %>%
  left_join(defender_volume, by = c("idx" = "defender_idx", "shot_family")) %>%
  filter(!is.na(contests))

for (fam in c("rim", "j3")) {
  cat("\n═══ TOP 15 DEFENDERS:", toupper(fam),
      "(min 100 contested, negative = better) ═══\n")
  defender_tbl %>%
    filter(shot_family == fam, contests >= 100) %>%
    arrange(effect_mean) %>%
    select(defender_name, contests, opp_fg, effect_mean,
           effect_q05, effect_q95, prob_below) %>%
    head(15) %>%
    mutate(across(where(is.numeric), ~ round(., 3))) %>%
    print(n = 15)
}





## ═════════════════════════════════════════════════════════════════════════════
## 11 — DEFENSIVE TEAM EFFECTS =================================================
## ═════════════════════════════════════════════════════════════════════════════

defteam_volume <- shots %>%
  group_by(defteam_idx, shot_family) %>%
  summarise(fga_against = n(), opp_fg = mean(fgm), .groups = "drop")

defteam_tbl <- defteam_effects %>%
  left_join(defteam_volume, by = c("idx" = "defteam_idx", "shot_family")) %>%
  filter(!is.na(fga_against))

for (fam in c("rim", "j2", "j3")) {
  cat("\n═══ DEFENSIVE TEAM EFFECTS:", toupper(fam),
      "(negative = better defense) ═══\n")
  defteam_tbl %>%
    filter(shot_family == fam) %>%
    arrange(effect_mean) %>%
    select(defender_team, fga_against, opp_fg, effect_mean,
           effect_q05, effect_q95) %>%
    mutate(across(where(is.numeric), ~ round(., 3))) %>%
    print(n = 30)
}





## ═════════════════════════════════════════════════════════════════════════════
## 12 — CALIBRATION: STAN-RECALIBRATED vs OBSERVED =============================
## ═════════════════════════════════════════════════════════════════════════════

# Does the two-stage model (XGB + Stan) produce well-calibrated probabilities?
# Including player effects, the calibrated prediction is p_skill.

cal_check <- shots %>%
  mutate(p_bin = cut(p_skill, breaks = seq(0, 1, by = 0.05))) %>%
  group_by(p_bin) %>%
  summarise(n         = n(),
            predicted = mean(p_skill),
            observed  = mean(fgm),
            .groups   = "drop") %>%
  filter(n >= 50)

p_cal_stan <- ggplot(cal_check, aes(x = predicted, y = observed)) +
  geom_point(aes(size = n), alpha = 0.7, color = "steelblue") +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
  scale_size_continuous(range = c(1, 8)) +
  labs(title = "Two-Stage Calibration (XGB + Stan player effects)",
       x = "Predicted FG%", y = "Observed FG%") +
  theme_minimal()

ggsave("twostage_calibration.png", p_cal_stan, width = 7, height = 6, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 13 — PLAYER EFFECT CATERPILLAR PLOTS ========================================
## ═════════════════════════════════════════════════════════════════════════════

caterpillar_plot <- function(effects_df, family, entity_name_col,
                             min_n = 100, top_n = 30, title_suffix = "") {
  effects_df %>%
    filter(shot_family == family, fga >= min_n) %>%
    arrange(desc(effect_mean)) %>%
    head(top_n) %>%
    ggplot(aes(x = reorder(!!sym(entity_name_col), effect_mean),
               y = effect_mean)) +
    geom_point(size = 2, color = "steelblue") +
    geom_errorbar(aes(ymin = effect_q05, ymax = effect_q95),
                  width = 0.3, alpha = 0.5) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    coord_flip() +
    labs(title = paste0(toupper(family), " ", title_suffix),
         x = NULL, y = "Logit-scale effect (90% CI)") +
    theme_minimal()
}

p_cat_rim <- caterpillar_plot(player_tbl, "rim", "player_name",
                              title_suffix = "player shooting effects")
p_cat_j3  <- caterpillar_plot(player_tbl, "j3", "player_name",
                              title_suffix = "player shooting effects")

p_cat <- p_cat_rim + p_cat_j3 +
  plot_annotation(title = "Player Shooting Skill (Two-Stage Model)")

ggsave("player_caterpillar.png", p_cat, width = 14, height = 8, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 14 — SIGMA COMPARISON PLOT ==================================================
## ═════════════════════════════════════════════════════════════════════════════
## Shows relative magnitude of player vs defender vs team effects

sigma_draws <- fit_v3$draws(variables = sigma_params, format = "draws_df") %>%
  pivot_longer(cols = -c(.chain, .iteration, .draw),
               names_to = "parameter", values_to = "value") %>%
  mutate(entity = case_when(str_detect(parameter, "player")   ~ "Player",
                            str_detect(parameter, "defender")  ~ "Defender",
                            str_detect(parameter, "defteam")  ~ "Def Team"),
         shot_family = case_when(str_detect(parameter, "\\[1\\]") ~ "rim",
                                 str_detect(parameter, "\\[2\\]") ~ "j2",
                                 str_detect(parameter, "\\[3\\]") ~ "j3"))

p_sigma <- ggplot(sigma_draws, aes(x = value, fill = entity)) +
  geom_density(alpha = 0.5) +
  facet_wrap(~shot_family, ncol = 3) +
  labs(title = "Posterior Distributions of Hierarchical SDs",
       subtitle = "Player skill >> Defender impact >> Team scheme",
       x = "σ (logit scale)", fill = NULL) +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave("sigma_comparison.png", p_sigma, width = 12, height = 5, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 15 — xFG RECALIBRATION DIAGNOSTIC ===========================================
## ═════════════════════════════════════════════════════════════════════════════
## Plot: XGBoost xFG vs Stan-recalibrated xFG (should be near-identity)

recal <- shots %>%
  sample_n(min(5000, n())) %>%
  mutate(cal_xfg = inv_logit(cal_hat[paste0("cal_intercept[", shot_type, "]")] +
                              cal_hat[paste0("cal_slope[", shot_type, "]")] * xfg_logit))

p_recal <- ggplot(recal, aes(x = xfg, y = cal_xfg, color = shot_family)) +
  geom_point(alpha = 0.1, size = 0.5) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  labs(title = "XGBoost xFG vs Stan-Recalibrated xFG",
       subtitle = "Close to diagonal = XGBoost was well-calibrated",
       x = "XGBoost xFG", y = "Stan-recalibrated xFG") +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave("xfg_recalibration.png", p_recal, width = 7, height = 6, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 16 — SAVE ALL OUTPUTS =======================================================
## ═════════════════════════════════════════════════════════════════════════════

write_csv(shots %>%
            select(game_id, event_num, player1_id, player_name,
                   shot_family, shot_type, fgm, xfg, xfg_logit,
                   p_context, p_skill, epaa, point_value,
                   player_idx, defender_idx, defteam_idx),
          "shots_twostage_final.csv")

write_csv(epaa_tbl,           "epaa_by_family.csv")
write_csv(epaa_overall,       "epaa_overall.csv")
write_csv(player_tbl %>%
            select(player_name, shot_family, fga, fg_pct, mean_xfg,
                   effect_mean, effect_pp, effect_q05, effect_q95, prob_above),
          "player_effects.csv")
write_csv(defender_tbl %>%
            select(imputed_def_name, shot_family, contests, opp_fg,
                   effect_mean, effect_q05, effect_q95, prob_below),
          "defender_effects.csv")
write_csv(defteam_tbl %>%
            select(defender_team, shot_family, fga_against, opp_fg,
                   effect_mean, effect_q05, effect_q95),
          "defteam_effects.csv")

# Save Stan draws for reuse
saveRDS(fit_v3, "stan_fit.rds")
saveRDS(draws_mat, "draws_v3_twostage.rds")

# Save mapping tables
write_csv(player_map,   "player_map_v3.csv")
write_csv(defender_map,  "defender_map_v3.csv")
write_csv(defteam_map,  "defteam_map_v3.csv")

message("\n─── Two-stage fitting complete ───")
message("  shots_twostage_final.csv  — per-shot predictions")
message("  epaa_overall.csv          — player EPAA rankings")
message("  player_effects.csv        — player random effects")
message("  defender_effects.csv      — defender random effects")
message("  defteam_effects.csv       — team defense effects")
