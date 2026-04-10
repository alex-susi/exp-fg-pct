## ═════════════════════════════════════════════════════════════════════════════
## 03_shootingTalent.R
## EPAA Stage 2: Hierarchical random effects via Stan
##
## OBJECTIVE:
##   Estimate how much each player, defender, and defensive team deviates from
##   the XGBoost shot quality baseline. Stage 1 answered
##   "how hard was this shot for a league-average player?". This script answers
##   "who is above or below average, and by how much?"
##
## TWO-STAGE DECOMPOSITION:
##   Stage 1 (XGBoost) → xfg_logit: the expected log-odds of making the shot
##                                   based purely on shot context
##   Stage 2 (Stan)    → a_player, a_defender, a_defteam: player/defender/team
##                                   deviations from the XGBoost baseline, on
##                                   the logit scale, estimated via partial pooling
##
## MODEL STRUCTURE (per shot i):
##   logit(p_make[i]) = cal_intercept[type] + cal_slope[type] * xfg_logit[i]
##                      + a_player[shooter, type]
##                      + a_defender[defender, type]
##                      + a_defteam[def_team, type]
##
##   The cal_intercept / cal_slope terms allow Stan to recalibrate the XGBoost
##   output before adding random effects (e.g. if XGBoost systematically
##   compresses the logit scale for three-pointers, cal_slope absorbs that).
##
## EPAA (Expected Points Above Average):
##   For each shot: EPAA = (p_with_skill − p_context_only) × point_value
##   Summed or averaged across shots to produce season-level ratings.
##
## Inputs:  shots_with_xfg.csv (from 02_shotQuality.R)
## Outputs: player/defender/team effect tables, EPAA rankings, diagnostic plots
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





## ═════════════════════════════════════════════════════════════════════════════
## 02 — BUILD STAN INDICES =====================================================
## ═════════════════════════════════════════════════════════════════════════════
## Re-index player, defender, defteam as contiguous 1..J integers.

shots <- shots %>%
  mutate(player_idx   = as.integer(factor(player1_id)),
         defender_idx = as.integer(factor(defender_id)),
         defteam_idx  = as.integer(factor(defender_team)),
         shot_type    = case_when(shot_family == "rim" ~ 1L,
                                  shot_family == "j2"  ~ 2L,
                                  shot_family == "j3"  ~ 3L)) %>% 
  filter(!is.na(xfg_logit))

# Mapping tables
player_map <- shots %>%
  distinct(player_idx, player1_id, player_name_full) %>%
  arrange(player_idx)

defender_map <- shots %>%
  filter(!is.na(defender_idx)) %>%
  distinct(defender_idx, defender_id, defender_name) %>%
  arrange(defender_idx)

defteam_map <- shots %>%
  filter(!is.na(defteam_idx)) %>%
  distinct(defteam_idx, defender_team) %>%
  arrange(defteam_idx)


# Compute volume rates per player × shot type 
# Build a volume-rate matrix passed to Stan as a volume-ability covariate.
# For each player × shot_family cell, compute the player's share of their
# own FGA coming from that family, then log-transform and standardize.
# The Stan model uses this as an additive term in transformed parameters:
#   a_player[j, t] = mu_volume[t] * log_fga_rate[j, t] + sigma_player[t] * z_player[j, t]
# This separates volume-driven mean shifts (specialists taking more rim
# attempts are on average better at the rim) from residual skill variation.

# Count FGA per player per shot family
player_vol <- expand_grid(player1_id = unique(shots$player1_id), 
                          shot_family = c("rim", "j2", "j3")) %>% 
  left_join(shots %>% 
              count(player1_id, shot_family, name = "fga"), 
            by = c("player1_id", "shot_family")) %>% 
  replace_na(list(fga = 0)) %>% 
  group_by(player1_id) %>% 
  mutate(fga_rate = (fga / sum(fga)) * 100, 
         log_vol = log1p(fga_rate)) %>%
  ungroup() %>% 
  group_by(shot_family) %>% 
  mutate(log_vol_z = (log_vol - mean(log_vol)) / sd(log_vol)) %>% 
  ungroup() 

# Reshape into a J_player × 3 matrix matching Stan indices 
vol_matrix <- player_vol %>% 
  left_join(player_map, by = "player1_id") %>% 
  mutate(t = case_when(shot_family == "rim" ~ 1L, 
                       shot_family == "j2" ~ 2L, 
                       shot_family == "j3" ~ 3L )) %>% 
  select(player_idx, t, log_vol_z) %>% 
  pivot_wider(names_from = t, values_from = log_vol_z) %>% 
  arrange(player_idx) %>% 
  select(-player_idx) %>% 
  as.matrix() 





## ═════════════════════════════════════════════════════════════════════════════
## 03 — PREPARE STAN DATA ======================================================
## ═════════════════════════════════════════════════════════════════════════════

stan_data <- list(N         = nrow(shots),
                  y         = as.integer(shots$fgm),
                  xfg_logit = shots$xfg_logit,
                  shot_type = shots$shot_type,
                  log_fga_rate = vol_matrix,
                
                  J_player  = max(shots$player_idx),
                  player    = shots$player_idx,
                
                  J_defender = max(shots$defender_idx, na.rm = TRUE),
                  defender   = shots$defender_idx,
                
                  J_defteam = max(shots$defteam_idx, na.rm = TRUE),
                  defteam   = shots$defteam_idx,
                
                  grainsize = 250)





## ═════════════════════════════════════════════════════════════════════════════
## 04 — COMPILE & SAMPLE =======================================================
## ═════════════════════════════════════════════════════════════════════════════

mod <- cmdstan_model("02_models/02_shootingTalent/shootingTalent.stan",
                     cpp_options = list(stan_threads = TRUE))

fit_nba <- mod$sample(data              = stan_data,
                      chains            = 4,
                      parallel_chains   = 4,
                      threads_per_chain = 4,
                      iter_warmup       = 600,
                      iter_sampling     = 600,
                      refresh           = 100,
                      adapt_delta       = 0.90,
                      max_treedepth     = 10)




## ═════════════════════════════════════════════════════════════════════════════
## 05 — DIAGNOSTICS ============================================================
## ═════════════════════════════════════════════════════════════════════════════

fit_nba$cmdstan_diagnose()

## Calibration parameter summaries: if XGBoost is well-calibrated for a given
## shot family, we expect cal_intercept ≈ 0 and cal_slope ≈ 1.
## Notably, j3 slope is > 1 (~2.0) because XGBoost compresses the
## logit range for three-pointers, so XGBoost underestimates how hard 
## an open vs. contested three is.
## The Stan calibration slope rescales this back to a plausible range.

cal_params <- c(paste0("cal_intercept[", 1:3, "]"),
                paste0("cal_slope[", 1:3, "]"))

print(fit_nba$summary(variables = cal_params))


## Hierarchical SD - how much skill variation exists within each shot type
sigma_params <- c(paste0("sigma_player[", 1:3, "]"),
                  paste0("sigma_defender[", 1:3, "]"),
                  paste0("sigma_defteam[", 1:3, "]"))

print(fit_nba$summary(variables = sigma_params))


## Volume-ability coefficient
##    Positive mu_volume[t] --> players who take a higher share of their shots from 
##    shot type t are on average better at that shot.
##    Players who take very few/no 3-pointers are probably bad at those shots.

print(fit_nba$summary(variables = "mu_volume"))





## ═════════════════════════════════════════════════════════════════════════════
## 06 — POSTERIOR DRAWS ========================================================
## ═════════════════════════════════════════════════════════════════════════════

draws_mat <- fit_nba$draws(format = "draws_matrix")
cat("\nDraws matrix:", nrow(draws_mat), "draws ×", ncol(draws_mat), "parameters\n")





## ═════════════════════════════════════════════════════════════════════════════
## 07 — RANDOM EFFECTS =========================================================
## ═════════════════════════════════════════════════════════════════════════════

## For each entity j and shot type, get the posterior mean, SD, and quantiles
extract_effects <- function(draws_mat, prefix, J, 
                            type_labels = c("rim", "j2", "j3")) {
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


## Player effects
player_effects <- extract_effects(draws_mat, "a_player", stan_data$J_player) %>%
  left_join(player_map, by = c("idx" = "player_idx")) %>% 
  select(-shot_type) %>% 
  relocate(player1_id, .after = idx) %>% 
  relocate(player_name_full, .after = idx) %>% 
  mutate(across(where(is.numeric), round, 3)) %>% 
  as.data.frame()


## Defender effects
defender_effects <- extract_effects(draws_mat, "a_defender", 
                                    stan_data$J_defender) %>%
  left_join(defender_map, by = c("idx" = "defender_idx")) %>% 
  select(-shot_type) %>% 
  relocate(defender_id, .after = idx) %>% 
  relocate(defender_name, .after = idx) %>% 
  mutate(across(where(is.numeric), round, 3)) %>% 
  as.data.frame()


## Defensive team effects
defteam_effects <- extract_effects(draws_mat, "a_defteam", stan_data$J_defteam) %>%
  left_join(defteam_map, by = c("idx" = "defteam_idx")) %>% 
  select(-shot_type) %>% 
  relocate(defender_team, .after = idx) %>% 
  mutate(across(where(is.numeric), round, 3)) %>% 
  as.data.frame()





## ═════════════════════════════════════════════════════════════════════════════
## 08 — PLAYER SHOOTING SKILL TABLES ===========================================
## ═════════════════════════════════════════════════════════════════════════════

# Shot volume per player per family
player_volume <- shots %>%
  group_by(player_idx, shot_family) %>%
  summarise(fga = n(), fg_pct = mean(fgm), mean_xfg = mean(xfg), .groups = "drop")


## Convert logit-scale effect to percentage-point impact using derivative
## of inv_logit at the shot type's league-average FG%:
##   d(inv_logit)/dx ≈ p * (1 - p)
player_tbl <- player_effects %>%
  left_join(player_volume, by = c("idx" = "player_idx", "shot_family")) %>%
  mutate(baseline_p = case_when(shot_family == "rim" ~ 0.65,
                                shot_family == "j2"  ~ 0.43,
                                shot_family == "j3"  ~ 0.36),
         effect_pp = effect_mean * baseline_p * (1 - baseline_p) * 100) %>%
  select(-baseline_p)


# Top shooters by family
for (fam in c("rim", "j2", "j3")) {
  cat("\n═══ Top 15 PLAYERS:", toupper(fam), "(by posterior mean) ═══\n")
  player_tbl %>%
    filter(shot_family == fam) %>%
    arrange(desc(effect_mean)) %>%
    select(player_name_full, fga, fg_pct, mean_xfg, effect_mean, effect_pp,
           effect_q05, effect_q95, prob_above) %>%
    head(n = 15) %>%
    mutate(across(where(is.numeric), ~ round(., 3))) %>%
    print()
}





## ═════════════════════════════════════════════════════════════════════════════
## 09 — EXPECTED POINTS ABOVE AVERAGE (EPAA) ===================================
## ═════════════════════════════════════════════════════════════════════════════
##
## For each shot: EPAA = (p_with_skill - p_context_only) × point_value
##
## p_context_only:  inv_logit(cal_intercept + cal_slope × xfg_logit)
##   This is the probability an average player (zero random effects) would
##   make this exact shot given its context.
##
## p_with_skill:    inv_logit(cal_intercept + cal_slope × xfg_logit + a_player)
##   This is the probability this specific player makes this shot.
##
## The difference × point_value (2 or 3) gives EPAA per shot.
## Summing across all of a player's attempts gives total season EPAA.
## Using posterior means provides point estimates.

cal_hat <- colMeans(draws_mat[, cal_params, drop = FALSE])

# Player effect posterior means: matrix [J_player × 3]
a_player_hat <- matrix(0, nrow = stan_data$J_player, ncol = 3)
for (t in 1:3) {
  cols <- paste0("a_player[", 1:stan_data$J_player, ",", t, "]")
  a_player_hat[, t] <- colMeans(draws_mat[, cols, drop = FALSE])
}


shots <- shots %>%
  # average player at this shot difficulty
  mutate(eta_context = cal_hat[paste0("cal_intercept[", shot_type, "]")] +
           cal_hat[paste0("cal_slope[", shot_type, "]")] * xfg_logit,
         p_context = inv_logit(eta_context),
         
         # With player skill
         a_player_val = a_player_hat[cbind(player_idx, shot_type)],
         p_skill = inv_logit(eta_context + a_player_val),
         
         # EPAA per shot
         point_value = if_else(is_three == 1, 3, 2),
         epaa = (p_skill - p_context) * point_value)


# Aggregate EPAA by player × shot family
epaa_tbl <- shots %>%
  group_by(player1_id, player_name_full, shot_family) %>%
  summarise(fga           = n(),
            fg_pct        = mean(fgm),
            xfg_pct       = mean(xfg),
            cal_xfg       = mean(p_context),
            skill_fg      = mean(p_skill),
            epaa_total    = sum(epaa),
            epaa_per100   = 100 * epaa_total / fga,
            .groups = "drop") %>% 
  as.data.frame() %>% 
  mutate(across(where(is.numeric), ~ round(., 3)))


# Overall EPAA collapsed across all shot families
epaa_overall <- shots %>%
  group_by(player1_id, player_name_full) %>%
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

cat("\n═══ TOP 15 THREE-POINT SHOOTERS ═══\n")
epaa_tbl %>%
  filter(shot_family == "j3") %>%
  arrange(desc(epaa_per100)) %>%
  head(15) %>%
  mutate(across(where(is.numeric), ~ round(., 3))) 

cat("\n═══ TOP 15 RIM FINISHERS ═══\n")
epaa_tbl %>%
  filter(shot_family == "rim") %>%
  #filter(player_name_full == "Zion Williamson") %>%
  arrange(desc(epaa_per100)) %>%
  head(15) %>%
  mutate(across(where(is.numeric), ~ round(., 3))) 


## EPAA 90% credible intervals
epaa_per100_ci  <- bind_rows(lapply(1:3, function(t) {
  pb   <- shots %>%
    group_by(player_idx, shot_type, shot_family) %>%
    summarise(mean_eta = mean(eta_context),
              pt_val   = first(point_value), .groups = "drop") %>% 
    filter(shot_type == t)
  cols <- paste0("a_player[", pb$player_idx, ",", t, "]")
  a_draws <- draws_mat[, cols, drop = FALSE]
  
  base_p    <- inv_logit(pb$mean_eta)
  skilled_p <- inv_logit(sweep(a_draws, 2, pb$mean_eta, "+"))
  epaa_draws <- sweep(skilled_p - rep(base_p, each = nrow(a_draws)),
                      2, pb$pt_val * 100, "*")
  
  tibble(idx         = pb$player_idx,
         shot_family = pb$shot_family,
         epaa100_mean = colMeans(epaa_draws),
         epaa100_q05  = apply(epaa_draws, 2, quantile, 0.05),
         epaa100_q95  = apply(epaa_draws, 2, quantile, 0.95))
}))

player_tbl <- player_tbl %>%
  left_join(epaa_per100_ci , by = c("idx", "shot_family"))





## ═════════════════════════════════════════════════════════════════════════════
## 10 — DEFENDER IMPACT TABLES =================================================
## ═════════════════════════════════════════════════════════════════════════════

defender_volume <- shots %>%
  group_by(defender_idx, shot_family) %>%
  summarise(contests = n(), 
            opp_fg = mean(fgm), 
            opp_xfg = mean(xfg), 
            .groups = "drop")

defender_tbl <- defender_effects %>%
  left_join(defender_volume, by = c("idx" = "defender_idx", "shot_family")) %>%
  filter(!is.na(contests))

## a_defender[j, t] < 0 --> opposing shooters make fewer shots than expected when 
## guarded by defender j (good defense).
for (fam in c("rim", "j2", "j3")) {
  cat("\n═══ TOP 15 DEFENDERS:", toupper(fam),
      "(min 100 contested, negative = better) ═══\n")
  defender_tbl %>%
    filter(shot_family == fam) %>%
    arrange(effect_mean) %>%
    select(defender_name, contests, opp_fg, opp_xfg, effect_mean,
           effect_q05, effect_q95, prob_below) %>%
    head(15) %>%
    mutate(across(where(is.numeric), ~ round(., 3))) %>%
    print()
}

defender_tbl %>%
  filter(shot_family == "rim", contests >= 100) %>%
  #arrange(effect_mean) %>%
  arrange((opp_xfg)) %>%
  select(defender_name, contests, opp_fg, opp_xfg, effect_mean,
         effect_q05, effect_q95, prob_below) %>%
  head(15) %>%
  mutate(across(where(is.numeric), ~ round(., 3))) %>%
  print()


## Points Saved Above Average per contest credible intervals
psaa_per100_ci <- bind_rows(lapply(1:3, function(t) {
  db   <- shots %>%
    filter(!is.na(defender_idx)) %>%
    group_by(defender_idx, shot_type, shot_family) %>%
    summarise(mean_eta = mean(eta_context),
              pt_val   = first(point_value), .groups = "drop") %>% 
    filter(shot_type == t)
  cols <- paste0("a_defender[", db$defender_idx, ",", t, "]")
  d_draws <- draws_mat[, cols, drop = FALSE]
  
  base_p    <- inv_logit(db$mean_eta)
  opp_p     <- inv_logit(sweep(d_draws, 2, db$mean_eta, "+"))
  # positive = points saved
  psaa_draws <- sweep(rep(base_p, each = nrow(d_draws)) - opp_p,
                      2, db$pt_val * 100, "*")
  
  tibble(idx         = db$defender_idx,
         shot_family = db$shot_family,
         psaa100_mean = colMeans(psaa_draws),
         psaa100_q05  = apply(psaa_draws, 2, quantile, 0.05),
         psaa100_q95  = apply(psaa_draws, 2, quantile, 0.95))
}))

defender_tbl <- defender_tbl %>%
  left_join(psaa_per100_ci, by = c("idx", "shot_family"))





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
    print()
}





## ═════════════════════════════════════════════════════════════════════════════
## 12 — CALIBRATION: STAN-RECALIBRATED vs OBSERVED =============================
## ═════════════════════════════════════════════════════════════════════════════

## Calibration plot for the full two-stage model: bins shots by p_skill
## (the Stan-recalibrated prediction including the player random effect) and
## overlays observed FG%. A well-calibrated model tracks the diagonal closely.

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

ggsave("02_models/02_shootingTalent/graphs/twostage_calibration.png", 
       p_cal_stan, width = 7, height = 6, dpi = 300)





## ═════════════════════════════════════════════════════════════════════════════
## 13 — PLAYER EFFECT CATERPILLAR PLOTS ========================================
## ═════════════════════════════════════════════════════════════════════════════

caterpillar_plot <- function(df, family, name_col, n_col,
                             mean_col, q05_col, q95_col,
                             min_n = 100, top_n = 5, bottom_n = 5,
                             title_suffix = "", y_lab = "") {
  
  family_label <- switch(family, rim = "Rim", j2 = "Midrange", j3 = "3PT", family)
  
  eligible <- df %>%
    { if (min_n == 0) filter(., shot_family == family)
      else filter(., shot_family == family, .data[[n_col]] >= min_n) }
  
  top <- eligible %>% 
    arrange(desc(.data[[mean_col]])) %>% 
    head(top_n)
  bot <- eligible %>% 
    arrange(.data[[mean_col]]) %>% 
    head(bottom_n)

  
  df_sub <- bind_rows(top, bot) %>%
    distinct(idx, .keep_all = TRUE) %>%
    arrange(desc(.data[[mean_col]])) %>%
    #mutate(label = paste0(.data[[name_col]], " (", .data[[n_col]], ")"),
    mutate(label = paste0(.data[[name_col]]),
           label = factor(label, levels = rev(label)))
  
  ggplot(df_sub, aes(x = label, y = .data[[mean_col]])) +
    geom_point(size = 4, color = "steelblue") +
    geom_errorbar(aes(ymin = .data[[q05_col]], ymax = .data[[q95_col]]),
                  width = 0.3, alpha = 0.5) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    coord_flip() +
    labs(title = paste(family_label, title_suffix),
         x = NULL, y = y_lab) +
    theme_minimal() +
    theme(plot.title   = element_text(hjust = 0.5, size = 20, face = "bold"),
          axis.title.x = element_text(size = 15),
          axis.title.y = element_text(size = 15),
          axis.text.x  = element_text(size = 15),
          axis.text.y  = element_text(size = 15))
  
}


## Player EPAA per 100 shots
p_cat_rim <- caterpillar_plot(player_tbl, "rim", "player_name_full", "fga",
                              "epaa100_mean", "epaa100_q05", "epaa100_q95",
                              title_suffix = "",
                              y_lab = "EPAA per 100 shots (90% CI)")
p_cat_j2  <- caterpillar_plot(player_tbl, "j2", "player_name_full", "fga",
                              "epaa100_mean", "epaa100_q05", "epaa100_q95",
                              title_suffix = "",
                              y_lab = "EPAA per 100 shots (90% CI)")
p_cat_j3  <- caterpillar_plot(player_tbl, "j3", "player_name_full", "fga",
                              "epaa100_mean", "epaa100_q05", "epaa100_q95",
                              title_suffix = "",
                              y_lab = "EPAA per 100 shots (90% CI)")

p_cat <- p_cat_rim + p_cat_j2 + p_cat_j3 +
  plot_annotation(title = "EPAA — Player Shooting Talent",
                  theme = theme(plot.title = element_text(hjust = 0.5, 
                                                          size = 25, 
                                                          face = "bold")))

ggsave("02_models/02_shootingTalent/graphs/player_caterpillar.png", 
       p_cat, width = 14, height = 8, dpi = 300)
ggsave("poster_CSAS/player_caterpillar.png", 
       p_cat, width = 18, height = 5, dpi = 300)


## Defender PSAA per 100 contests
def_cat_rim <- caterpillar_plot(defender_tbl, "rim", "defender_name", "contests",
                                "psaa100_mean", "psaa100_q05", "psaa100_q95",
                                title_suffix = "",
                                y_lab = "Points saved per 100 contests (90% CI)")
def_cat_j2  <- caterpillar_plot(defender_tbl, "j2", "defender_name", "contests",
                                "psaa100_mean", "psaa100_q05", "psaa100_q95",
                                title_suffix = "",
                                y_lab = "Points saved per 100 contests (90% CI)")
def_cat_j3  <- caterpillar_plot(defender_tbl, "j3", "defender_name", "contests",
                                "psaa100_mean", "psaa100_q05", "psaa100_q95",
                                title_suffix = "",
                                y_lab = "Points saved per 100 contests (90% CI)")

def_cat <- def_cat_rim + def_cat_j2 + def_cat_j3 +
  plot_annotation(title = "Defensive EPAA — Defender Talent",
                  theme = theme(plot.title = element_text(hjust = 0.5, 
                                                          size = 25, 
                                                          face = "bold")))

ggsave("02_models/02_shootingTalent/graphs/defender_caterpillar.png", 
       def_cat, width = 14, height = 3, dpi = 150)
ggsave("poster_CSAS/defender_caterpillar.png", 
       def_cat, width = 18, height = 5, dpi = 300)





## ═════════════════════════════════════════════════════════════════════════════
## 14 — SIGMA COMPARISON PLOT ==================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Shows the posterior distributions of the three hierarchical SDs side by side.
## The relative magnitudes tell us how much of the total shot-outcome variation
## is attributable to shooter skill vs. defender impact vs. team scheme.

sigma_draws <- fit_nba$draws(variables = sigma_params, format = "draws_df") %>%
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

ggsave("02_models/02_shootingTalent/graphs/sigma_comparison.png", 
       p_sigma, width = 12, height = 5, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 15 — xFG RECALIBRATION DIAGNOSTIC ===========================================
## ═════════════════════════════════════════════════════════════════════════════
## 
## Scatterplot of raw XGBoost xFG vs. Stan-recalibrated xFG (using posterior
## mean cal_intercept and cal_slope). If XGBoost were perfectly calibrated
## all points would lie on the diagonal.

recal <- shots %>%
  sample_n(min(5000, n())) %>%
  mutate(cal_xfg = inv_logit(cal_hat[paste0("cal_intercept[", shot_type, "]")] +
                              cal_hat[paste0("cal_slope[", shot_type, "]")] * 
                               xfg_logit))

p_recal <- ggplot(recal, aes(x = xfg, y = cal_xfg, color = shot_family)) +
  geom_point(alpha = 0.1, size = 0.5) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed") +
  labs(title = "XGBoost xFG vs Stan-Recalibrated xFG",
       subtitle = "Close to diagonal = XGBoost was well-calibrated",
       x = "XGBoost xFG", y = "Stan-recalibrated xFG") +
  theme_minimal() +
  theme(legend.position = "bottom")

ggsave("02_models/02_shootingTalent/graphs/xfg_recalibration.png", 
       p_recal, width = 7, height = 6, dpi = 150)





## ═════════════════════════════════════════════════════════════════════════════
## 16 — EPAA vs Next year performance ==========================================
## ═════════════════════════════════════════════════════════════════════════════
## Plot: XGBoost xFG vs Stan-recalibrated xFG (should be near-identity)

yoy <- read.csv("epaa_fg_yoy.csv")

mod <- lm(fgoe_26 ~ epaa_per100, data = yoy)
r2 <- summary(mod)$r.squared

p_yoy <- ggplot(yoy, aes(x = epaa_per100, y = fgoe_26)) +
  geom_hline(yintercept = 0, color = "gray70", linetype = "dotted") +
  geom_vline(xintercept = 0, color = "gray70", linetype = "dotted") +
  geom_point(aes(size = fga), alpha = 0.5, color = "#1d428a") +
  geom_smooth(method = "lm", se = FALSE, color = "red", linetype = "dashed") +
  annotate("text", x = max(df$epaa_per100) * 0.7, y = max(df$fgoe_26) * 0.9,
           label = paste0("R² = ", round(r2, 3)),
           size = 6, fontface = "bold") +
  scale_size_continuous(range = c(1.5, 6), name = "FGA") +
  labs(x = "EPAA per 100 Shots (2024-25)",
       y = "FG% Over Expected (2025-26)",
       title = "EPAA Predicting Next-Year Shooting Performance") +
  theme_minimal(base_size = 14) +
  theme(plot.title = element_text(face = "bold", hjust = 0.5))

ggsave("02_models/02_shootingTalent/graphs/yoy.png", 
       p_yoy, width = 7, height = 6, dpi = 150)
ggsave("poster_CSAS/yoy.png", p_yoy, width = 10, height = 10, dpi = 300)





## ═════════════════════════════════════════════════════════════════════════════
## 16 — SAVE ALL OUTPUTS =======================================================
## ═════════════════════════════════════════════════════════════════════════════

## Per-shot predictions: includes both p_context (context-only baseline) and
## p_skill (with player random effect) for every FGA. Primary analysis file.
write_csv(shots %>%
            select(game_id, event_num, player1_id, player_name,
                   shot_family, shot_type, fgm, xfg, xfg_logit,
                   p_context, p_skill, epaa, point_value,
                   player_idx, defender_idx, defteam_idx),
          "shots_twostage_final.csv")

write_csv(epaa_tbl,     "epaa_by_family.csv")
write_csv(epaa_overall, "epaa_overall.csv")
write_csv(player_tbl %>%
            select(player_name_full, shot_family, fga, fg_pct, mean_xfg,
                   effect_mean, effect_pp, effect_q05, effect_q95, prob_above, 
                   epaa100_mean, epaa100_q05, epaa100_q95),
          "player_effects.csv")
write_csv(defender_tbl %>%
            select(imputed_def_name, shot_family, contests, opp_fg,
                   effect_mean, effect_q05, effect_q95, prob_below),
          "defender_effects.csv")
write_csv(defteam_tbl %>%
            select(defender_team, shot_family, fga_against, opp_fg,
                   effect_mean, effect_q05, effect_q95),
          "defteam_effects.csv")

## Save Stan fit object and draws matrix.
saveRDS(fit_nba, "stan_fit_nba.rds")
saveRDS(draws_mat, "draws_v3_twostage.rds")

# Save mapping tables
write_csv(player_map,   "player_map_v3.csv")
write_csv(defender_map, "defender_map_v3.csv")
write_csv(defteam_map,  "defteam_map_v3.csv")
