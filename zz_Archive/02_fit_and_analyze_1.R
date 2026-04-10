## =============================================================================
## 02_fit_and_analyze.R
## NBA xFG v2 — Model fitting, diagnostics, xFG extraction, player evaluation
##
## Assumes 01_data_pipeline.R has been run:
##   build_season_pbp(2024)  → pbp_2024.csv, scaling_params_2024.csv, etc.
##
## Sections:
##   1-3.   Load data → build Stan list → sample
##   4-5.   Diagnostics → extract draws
##   6.     xFG% per shot (context-only, no player effects)
##   7.     Player summary table
##   8.     Foul-drawing value integration
##   9.     Defensive team effects
##   10-11. Surface plots, context comparisons
##   12.    Calibration / posterior predictive check
##   13.    Feature importance
##   14.    Save
## =============================================================================

library(dplyr)
library(readr)
library(stringr)
library(tidyr)
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

SEASON_TAG <- "2024"   # change to match build_season_pbp() call


## =============================================================================
## 1 ─ LOAD DATA
## =============================================================================

pbp_full       <- read_csv(paste0("pbp_", SEASON_TAG, ".csv"),
                           show_col_types = FALSE)
scaling_params <- read_csv(paste0("scaling_params_", SEASON_TAG, ".csv"),
                           show_col_types = FALSE)
player_map     <- read_csv(paste0("player_map_", SEASON_TAG, ".csv"),
                           show_col_types = FALSE)
defteam_map    <- read_csv(paste0("defteam_map_", SEASON_TAG, ".csv"),
                           show_col_types = FALSE)

# Non-FGA foul trips (for foul-drawing evaluation)
foul_path <- paste0("non_fga_fouls_", SEASON_TAG, ".csv")
non_fga_fouls <- if (file.exists(foul_path)) {
  read_csv(foul_path, show_col_types = FALSE)
} else NULL

# Filter to FGA rows with valid features
shots <- pbp_full %>%
  filter(fga == 1, !is.na(shot_family), !is.na(player_idx),
         !is.na(defteam_idx))

cat("Loaded ", nrow(shots), " shots from pbp_", SEASON_TAG, ".csv\n")


## =============================================================================
## 2 ─ BUILD STAN DATA
## =============================================================================

# Replace any residual NAs in scaled features with 0 (scaled mean)
shots <- shots %>%
  mutate(across(c(D, A, T_scaled, def_dist_scaled, ht_match_scaled),
                ~ if_else(is.na(.x), 0, .x)))

stan_data <- list(
  N  = nrow(shots),
  y  = as.integer(shots$y),

  D        = shots$D,
  A        = shots$A,
  T        = shots$T_scaled,
  def_dist = shots$def_dist_scaled,
  ht_match = shots$ht_match_scaled,

  is_dunk = as.integer(shots$is_dunk),
  is_2nd  = as.integer(shots$is_2ndchance),
  is_fb   = as.integer(shots$is_fastbreak),
  is_ft   = as.integer(shots$is_fromturnover),
  is_cs   = as.integer(coalesce(shots$is_catch_shoot, 0L)),

  shot_type = shots$shot_type,
  J_player  = max(shots$player_idx),
  player    = shots$player_idx,
  J_defteam = max(shots$defteam_idx),
  defteam   = shots$defteam_idx,

  grainsize = 200
)

cat("Stan data: N=", stan_data$N, " J_player=", stan_data$J_player,
    " J_defteam=", stan_data$J_defteam, "\n")


## =============================================================================
## 3 ─ COMPILE & SAMPLE
## =============================================================================

m_v2 <- cmdstan_model("nba_xfg_v2.stan",
                      cpp_options = list(stan_threads = TRUE))

fit_v2 <- m_v2$sample(
  data             = stan_data,
  chains           = 4,
  parallel_chains  = 4,
  threads_per_chain = 4,
  iter_warmup      = 800,
  iter_sampling    = 800,
  refresh          = 200,
  adapt_delta      = 0.90,
  max_treedepth    = 12
)


## =============================================================================
## 4 ─ DIAGNOSTICS
## =============================================================================

fit_v2$cmdstan_diagnose()

# Fixed effect names (must match Stan parameter block exactly)
ctx_pars <- c(
  "a_rim", "bD_rim", "bA_rim", "bDk_rim",
  "bT_rim", "b2_rim", "bFB_rim", "bFT_rim", "bDef_rim", "bH_rim",
  "a_j2", "bD_j2", "bA_j2",
  "bT_j2", "b2_j2", "bFB_j2", "bFT_j2", "bDef_j2", "bCS_j2", "bH_j2",
  "a_j3", "bD_j3", "bA_j3",
  "bT_j3", "b2_j3", "bFB_j3", "bFT_j3", "bDef_j3", "bCS_j3", "bH_j3"
)

cat("\n═══ FIXED EFFECTS ═══\n")
print(fit_v2$summary(variables = ctx_pars))

cat("\n═══ HIERARCHICAL SDs ═══\n")
print(fit_v2$summary(variables = c("sigma_player", "sigma_defteam")))


## =============================================================================
## 5 ─ EXTRACT POSTERIOR DRAWS
## =============================================================================

all_params <- fit_v2$metadata()$model_params
draw_vars  <- c(ctx_pars,
                "sigma_player", "sigma_defteam",
                grep("^z_player\\[",  all_params, value = TRUE),
                grep("^z_defteam\\[", all_params, value = TRUE))

draws_mat <- fit_v2$draws(variables = draw_vars) %>%
  posterior::as_draws_matrix()

cat("Draws: ", nrow(draws_mat), " × ", ncol(draws_mat), "\n")


## =============================================================================
## 6 ─ xFG% PER SHOT (context-only, average player)
## =============================================================================

# Build design matrices per shot family and multiply by posterior draws.
# This gives the "what would an average player shoot here?" probability.

compute_xfg <- function(shots_df, dm, S = 200, seed = 1) {
  set.seed(seed)
  dm <- dm[sample.int(nrow(dm), min(S, nrow(dm))), , drop = FALSE]
  N  <- nrow(shots_df)
  xfg <- numeric(N)

  # ── rim: 10 features (intercept + 9) ──
  rim_cols <- c("a_rim", "bD_rim", "bA_rim", "bDk_rim",
                "bT_rim", "b2_rim", "bFB_rim", "bFT_rim",
                "bDef_rim", "bH_rim")
  # ── j2: 10 features ──
  j2_cols <- c("a_j2", "bD_j2", "bA_j2",
               "bT_j2", "b2_j2", "bFB_j2", "bFT_j2",
               "bDef_j2", "bCS_j2", "bH_j2")
  # ── j3: 10 features ──
  j3_cols <- c("a_j3", "bD_j3", "bA_j3",
               "bT_j3", "b2_j3", "bFB_j3", "bFT_j3",
               "bDef_j3", "bCS_j3", "bH_j3")

  fam <- shots_df$shot_family

  # rim
  idx <- which(fam == "rim")
  if (length(idx) > 0) {
    X <- rbind(1,
               shots_df$D[idx], shots_df$A[idx], shots_df$is_dunk[idx],
               shots_df$T_scaled[idx], shots_df$is_2ndchance[idx],
               shots_df$is_fastbreak[idx], shots_df$is_fromturnover[idx],
               shots_df$def_dist_scaled[idx], shots_df$ht_match_scaled[idx])
    xfg[idx] <- colMeans(plogis(dm[, rim_cols, drop = FALSE] %*% X))
  }

  # j2
  idx <- which(fam == "j2")
  if (length(idx) > 0) {
    X <- rbind(1,
               shots_df$D[idx], shots_df$A[idx],
               shots_df$T_scaled[idx], shots_df$is_2ndchance[idx],
               shots_df$is_fastbreak[idx], shots_df$is_fromturnover[idx],
               shots_df$def_dist_scaled[idx],
               coalesce(shots_df$is_catch_shoot[idx], 0L),
               shots_df$ht_match_scaled[idx])
    xfg[idx] <- colMeans(plogis(dm[, j2_cols, drop = FALSE] %*% X))
  }

  # j3
  idx <- which(fam == "j3")
  if (length(idx) > 0) {
    X <- rbind(1,
               shots_df$D[idx], shots_df$A[idx],
               shots_df$T_scaled[idx], shots_df$is_2ndchance[idx],
               shots_df$is_fastbreak[idx], shots_df$is_fromturnover[idx],
               shots_df$def_dist_scaled[idx],
               coalesce(shots_df$is_catch_shoot[idx], 0L),
               shots_df$ht_match_scaled[idx])
    xfg[idx] <- colMeans(plogis(dm[, j3_cols, drop = FALSE] %*% X))
  }

  xfg
}


shots <- shots %>%
  mutate(
    xfg       = compute_xfg(shots, draws_mat, S = 200),
    point_val = if_else(is_three == 1, 3L, 2L),
    xpoints   = xfg * point_val,
    points    = fgm * point_val
  )


## =============================================================================
## 7 ─ PLAYER SUMMARY TABLE (shot-making only)
## =============================================================================

player_tbl <- shots %>%
  group_by(player1_id, player_name) %>%
  summarise(
    fga          = n(),
    fg_pct       = mean(fgm),
    xfg_pct      = mean(xfg),
    fg_over_exp  = fg_pct - xfg_pct,
    pts_per_shot = mean(points),
    xpts_per_shot = mean(xpoints),
    pps_over_exp = pts_per_shot - xpts_per_shot,
    total_pts    = sum(points),
    total_xpts   = sum(xpoints),
    pts_above_exp = total_pts - total_xpts,
    .groups = "drop"
  ) %>%
  arrange(desc(fg_over_exp))

cat("\n═══ TOP PLAYERS BY FG% OVER EXPECTED (min 500 FGA) ═══\n")
player_tbl %>% filter(fga >= 500) %>% head(20) %>% print(n = 20)


## =============================================================================
## 8 ─ FOUL-DRAWING VALUE
## =============================================================================
#
# The Stan model predicts P(make | FGA). Foul-drawing value is computed
# separately and ADDED to the player evaluation.
#
# Two sources of foul-drawing points:
#   A) And-1: made FGA + 1 bonus FT → points already counted in FGA model,
#      bonus FT is EXTRA value (0 or 1 point per and-1)
#   B) Non-FGA foul trip: fouled before shot → 2 or 3 FTs, no FGA.
#      These are "hidden" shot opportunities not in the FGA model.
#
# Total expected points per "true shot attempt" (TSA):
#   E[pts | TSA] = P(FGA) × E[pts | FGA]           ← from Stan model
#                + P(and-1) × E[and-1 FT pts]       ← bonus
#                + P(non-FGA foul) × E[foul trip pts] ← hidden value
#
# For player evaluation, we compute:
#   foul_value_total = and1_bonus_pts + non_fga_foul_pts

# A) And-1 bonus points (already attached to FGA rows)
and1_value <- shots %>%
  group_by(player1_id, player_name) %>%
  summarise(
    n_and1         = sum(drew_and1, na.rm = TRUE),
    and1_bonus_pts = sum(and1_ft_made, na.rm = TRUE),
    .groups = "drop"
  )

# B) Non-FGA foul trip points
if (!is.null(non_fga_fouls) && nrow(non_fga_fouls) > 0) {
  nfga_value <- non_fga_fouls %>%
    group_by(foul_drawn_by) %>%
    summarise(
      n_foul_trips    = n(),
      non_fga_ft_pts  = sum(n_ft_made, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    rename(player1_id = foul_drawn_by)
} else {
  nfga_value <- tibble(player1_id = character(),
                       n_foul_trips = integer(),
                       non_fga_ft_pts = integer())
}

# C) Combined player evaluation
player_full <- player_tbl %>%
  left_join(and1_value %>% select(player1_id, n_and1, and1_bonus_pts),
            by = "player1_id") %>%
  left_join(nfga_value, by = "player1_id") %>%
  mutate(
    across(c(n_and1, and1_bonus_pts, n_foul_trips, non_fga_ft_pts),
           ~ coalesce(.x, 0L)),

    # True shot attempts (FGA + non-FGA foul trips)
    tsa = fga + n_foul_trips,

    # Foul-drawing value = and-1 bonus + non-FGA FT points
    foul_pts      = and1_bonus_pts + non_fga_ft_pts,
    foul_pts_per_tsa = foul_pts / tsa,

    # Total value = shot-making + foul-drawing
    total_pts_incl_fouls = total_pts + foul_pts,
    total_xpts_tsa       = total_xpts,  # xFG model doesn't include fouls
    total_above_exp      = pts_above_exp + foul_pts,

    # Points per TSA (true efficiency)
    pts_per_tsa = total_pts_incl_fouls / tsa
  ) %>%
  arrange(desc(total_above_exp))

cat("\n═══ TOP PLAYERS — TOTAL VALUE (shot-making + foul-drawing, min 500 TSA) ═══\n")
player_full %>%
  filter(tsa >= 500) %>%
  select(player_name, tsa, fg_pct, xfg_pct, fg_over_exp,
         foul_pts, pts_per_tsa, total_above_exp) %>%
  head(20) %>%
  print(n = 20)


## =============================================================================
## 9 ─ DEFENSIVE TEAM EFFECTS
## =============================================================================

J_defteam      <- stan_data$J_defteam
sigma_def_cols <- paste0("sigma_defteam[", 1:3, "]")

get_z_mat <- function(dm, prefix, t, J) {
  pat  <- paste0("^", prefix, "\\[(\\d+),\\s*", t, "\\]$")
  cols <- grep(pat, colnames(dm), value = TRUE)
  idx  <- as.integer(sub(pat, "\\1", cols))
  dm[, cols[order(idx)], drop = FALSE]
}

defteam_effects <- bind_rows(lapply(1:3, function(t) {
  z <- get_z_mat(draws_mat, "z_defteam", t, J_defteam)
  s <- draws_mat[, sigma_def_cols[t], drop = TRUE]
  a <- sweep(z, 1, s, "*")
  tibble(
    defteam_idx = 1:J_defteam,
    shot_type   = t,
    shot_family = c("rim", "j2", "j3")[t],
    effect_mean = colMeans(a),
    effect_q05  = apply(a, 2, quantile, 0.05),
    effect_q95  = apply(a, 2, quantile, 0.95)
  )
})) %>%
  left_join(defteam_map, by = "defteam_idx")

cat("\n═══ BEST DEFENSES BY SHOT TYPE (lower = better) ═══\n")
defteam_effects %>%
  arrange(shot_family, effect_mean) %>%
  group_by(shot_family) %>%
  slice_head(n = 5) %>%
  print(n = 15)


## =============================================================================
## 10 ─ xFG SURFACE PLOT (average context)
## =============================================================================

par_hat <- colMeans(draws_mat[, ctx_pars, drop = FALSE])

x_seq <- seq(-250, 250, by = 2.5)
y_seq <- seq(-47.5, 422.5, by = 2.5)

grid <- expand_grid(loc_x = x_seq, loc_y = y_seq) %>%
  mutate(
    shot_dist = sqrt(loc_x^2 + loc_y^2) / 10,
    angle_deg = atan2(abs(loc_x), pmax(loc_y, 1e-1)) * (180 / pi),
    is_three  = as.integer(if_else(loc_y <= 92.5,
                                   abs(loc_x) >= 220,
                                   sqrt(loc_x^2 + loc_y^2) >= 237.5)),
    is_jump   = as.integer(shot_dist > 4),
    shot_family = case_when(is_jump == 0 ~ "rim",
                            is_jump == 1 & is_three == 0 ~ "j2",
                            TRUE ~ "j3")
  ) %>%
  left_join(scaling_params, by = "shot_family") %>%
  mutate(
    D = (shot_dist - mu_dist) / sd_dist,
    A = (angle_deg - mu_ang) / sd_ang,

    # All context features at neutral (scaled mean = 0)
    eta = case_when(
      shot_family == "rim" ~
        par_hat["a_rim"] + par_hat["bD_rim"]*D + par_hat["bA_rim"]*A,
      shot_family == "j2" ~
        par_hat["a_j2"]  + par_hat["bD_j2"]*D  + par_hat["bA_j2"]*A,
      TRUE ~
        par_hat["a_j3"]  + par_hat["bD_j3"]*D  + par_hat["bA_j3"]*A
    ),
    xfg     = inv_logit(eta),
    xpoints = xfg * (2 + is_three)
  )

# Court drawing helper — source your existing one, or use this minimal version
# source("court_helpers.R")

p_xfg_surface <- ggplot(grid, aes(loc_x, loc_y, fill = xfg)) +
  geom_raster(interpolate = TRUE) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(labels = percent_format(1), name = "xFG%") +
  labs(title = "v2 xFG% surface (average context)") +
  theme_void() + theme(legend.position = "right")

p_xpts_surface <- ggplot(grid, aes(loc_x, loc_y, fill = xpoints)) +
  geom_raster(interpolate = TRUE) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(labels = number_format(0.01), name = "xPts") +
  labs(title = "v2 xPoints surface (average context)") +
  theme_void() + theme(legend.position = "right")


## =============================================================================
## 11 ─ CONTEXT COMPARISON: fastbreak vs late-clock half-court
## =============================================================================

grid_fb <- grid %>% mutate(
  context = "Fastbreak",
  eta_adj = eta + case_when(
    shot_family == "rim" ~ par_hat["bFB_rim"] + par_hat["bT_rim"]*(-1.5) +
                           par_hat["bDef_rim"]*1,
    shot_family == "j2"  ~ par_hat["bFB_j2"]  + par_hat["bT_j2"]*(-1.5) +
                           par_hat["bDef_j2"]*1 + par_hat["bCS_j2"]*1,
    TRUE                 ~ par_hat["bFB_j3"]  + par_hat["bT_j3"]*(-1.5) +
                           par_hat["bDef_j3"]*1 + par_hat["bCS_j3"]*1),
  xfg_adj  = inv_logit(eta_adj),
  xpts_adj = xfg_adj * (2 + is_three))

grid_hc <- grid %>% mutate(
  context = "Half-court (late clock)",
  eta_adj = eta + case_when(
    shot_family == "rim" ~ par_hat["bT_rim"]*1.5 + par_hat["bDef_rim"]*(-0.5),
    shot_family == "j2"  ~ par_hat["bT_j2"]*1.5  + par_hat["bDef_j2"]*(-0.5),
    TRUE                 ~ par_hat["bT_j3"]*1.5  + par_hat["bDef_j3"]*(-0.5)),
  xfg_adj  = inv_logit(eta_adj),
  xpts_adj = xfg_adj * (2 + is_three))

p_ctx <- ggplot(bind_rows(grid_fb, grid_hc),
                aes(loc_x, loc_y, fill = xpts_adj)) +
  geom_raster(interpolate = TRUE) +
  facet_wrap(~context) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(limits = c(0.4, 1.5), name = "xPts") +
  labs(title = "Context-adjusted xPoints: fastbreak vs half-court") +
  theme_void()


## =============================================================================
## 12 ─ CALIBRATION / POSTERIOR PREDICTIVE CHECK
## =============================================================================

p_calibration <- shots %>%
  mutate(xfg_bin = cut(xfg, breaks = seq(0, 1, by = 0.05))) %>%
  group_by(xfg_bin) %>%
  summarise(n = n(), predicted = mean(xfg), observed = mean(fgm),
            .groups = "drop") %>%
  filter(n >= 50) %>%
  ggplot(aes(predicted, observed)) +
  geom_point(aes(size = n), alpha = 0.7) +
  geom_abline(slope = 1, intercept = 0, lty = 2, color = "red") +
  scale_size_continuous(range = c(1, 8)) +
  labs(title = "Posterior Predictive Calibration",
       subtitle = "Predicted xFG% vs observed FG% (5pp bins)",
       x = "Predicted xFG%", y = "Observed FG%") +
  theme_minimal()


## =============================================================================
## 13 ─ FEATURE IMPORTANCE
## =============================================================================

coef_summary <- fit_v2$summary(variables = ctx_pars) %>%
  mutate(
    shot_family = case_when(str_detect(variable, "rim") ~ "rim",
                            str_detect(variable, "j2")  ~ "j2",
                            str_detect(variable, "j3")  ~ "j3"),
    feature = variable %>%
      str_remove("^(a|bD|bA|bDk|bT|b2|bFB|bFT|bDef|bCS|bH)_") %>%
      str_replace(".*", function(x) str_extract(variable, "^[^_]+"))
  )

p_coefs <- ggplot(coef_summary %>% filter(!str_detect(variable, "^a_")),
                  aes(reorder(variable, abs(mean)), mean)) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = q5, ymax = q95), width = 0.2) +
  geom_hline(yintercept = 0, lty = 2) +
  coord_flip() +
  facet_wrap(~shot_family, scales = "free_y") +
  labs(title = "Fixed effects (90% CIs)", x = NULL,
       y = "Logit-scale coefficient") +
  theme_minimal()


## =============================================================================
## 14 ─ SAVE
## =============================================================================

write_csv(shots %>% select(game_id, event_num, player1_id, player_name,
                           shot_family, shot_type, fgm, xfg, xpoints, points,
                           drew_and1, and1_ft_made,
                           is_2ndchance, is_fastbreak, is_fromturnover,
                           is_catch_shoot, sec_since_play_start,
                           expected_def_dist, height_matchup,
                           def_team_abbr, x_legacy, y_legacy,
                           shot_distance, angle),
          paste0("shots_with_xfg_", SEASON_TAG, ".csv"))

write_csv(player_full, paste0("player_summary_", SEASON_TAG, ".csv"))
write_csv(defteam_effects, paste0("defteam_effects_", SEASON_TAG, ".csv"))
saveRDS(draws_mat, paste0("draws_mat_", SEASON_TAG, ".rds"))

message("\n─── Fitting and analysis complete ───")
