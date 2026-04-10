## =============================================================================
## 02_fit_and_analyze.R
## NBA xFG v2 — Model compilation, fitting, diagnostics, xFG extraction
##
## Assumes 01_data_pipeline.R has been run and produced:
##   - shots_v2_enriched.csv
##   - scaling_params_v2.csv
##   - player_map_v2.csv, defteam_map_v2.csv
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


## =============================================================================
## 1 ─ LOAD DATA
## =============================================================================

shots <- read_csv("shots_v2_enriched.csv", show_col_types = FALSE)
scaling_params <- read_csv("scaling_params_v2.csv", show_col_types = FALSE)
player_map  <- read_csv("player_map_v2.csv", show_col_types = FALSE)
defteam_map <- read_csv("defteam_map_v2.csv", show_col_types = FALSE)


## =============================================================================
## 2 ─ PREPARE STAN DATA
## =============================================================================

stan_data <- list(
  N  = nrow(shots),
  y  = shots$y,

  # continuous features (scaled)
  D        = shots$D,
  A        = shots$A,
  T        = shots$T,
  def_dist = shots$def_dist_scaled,
  ht_match = shots$ht_match_scaled,

  # binary features
  is_dunk   = as.integer(shots$is_dunk),
  is_second = as.integer(shots$is_second_chance),
  is_trans  = as.integer(shots$is_transition),
  is_cs     = as.integer(shots$is_catch_shoot),

  # indices
  shot_type = shots$shot_type,
  J_player  = n_distinct(shots$player_idx),
  player    = shots$player_idx,
  J_defteam = n_distinct(shots$defteam_idx),
  defteam   = shots$defteam_idx,

  grainsize = 200
)

cat("Stan data prepared:\n")
cat("  N =", stan_data$N, "\n")
cat("  J_player =", stan_data$J_player, "\n")
cat("  J_defteam =", stan_data$J_defteam, "\n")


## =============================================================================
## 3 ─ COMPILE & SAMPLE
## =============================================================================

m_v2 <- cmdstan_model("nba_xfg_v2.stan",
                      cpp_options = list(stan_threads = TRUE))

fit_v2 <- m_v2$sample(
  data             = stan_data,
  chains           = 4,
  parallel_chains  = 4,
  threads_per_chain = 4,     # 16 total cores
  iter_warmup      = 800,
  iter_sampling    = 800,
  refresh          = 200,
  adapt_delta      = 0.90,   # slightly tighter for the richer model
  max_treedepth    = 12
)


## =============================================================================
## 4 ─ DIAGNOSTICS
## =============================================================================

fit_v2$cmdstan_diagnose()

# Fixed effect summaries
context_params <- c(
  "alpha_rim", "bD_rim", "bA_rim", "bDunk_rim",
  "bT_rim", "b2nd_rim", "bTrans_rim", "bDef_rim", "bH_rim",
  "alpha_j2", "bD_j2", "bA_j2",
  "bT_j2", "b2nd_j2", "bTrans_j2", "bDef_j2", "bCS_j2", "bH_j2",
  "alpha_j3", "bD_j3", "bA_j3",
  "bT_j3", "b2nd_j3", "bTrans_j3", "bDef_j3", "bCS_j3", "bH_j3"
)

cat("\n═══ FIXED EFFECTS ═══\n")
print(fit_v2$summary(variables = context_params))

cat("\n═══ HIERARCHICAL SDs ═══\n")
print(fit_v2$summary(variables = c("sigma_player_type", "sigma_defteam_type")))


## =============================================================================
## 5 ─ EXTRACT POSTERIOR DRAWS
## =============================================================================

# All fixed effects + hierarchical parameters
draw_vars <- c(
  context_params,
  "sigma_player_type", "sigma_defteam_type",
  grep("^z_player\\[",  fit_v2$metadata()$model_params, value = TRUE),
  grep("^z_defteam\\[", fit_v2$metadata()$model_params, value = TRUE)
)

draws_mat <- fit_v2$draws(variables = draw_vars) %>%
  posterior::as_draws_matrix()

cat("Draws matrix: ", nrow(draws_mat), " draws × ",
    ncol(draws_mat), " parameters\n")


## =============================================================================
## 6 ─ COMPUTE xFG% PER SHOT (average-player, no random effects)
## =============================================================================

# This gives the "context-only" expected FG% — what an average player
# would shoot from this spot, in this game situation, against this defense quality.

get_xfg_v2 <- function(shots_df, S = 200, seed = 1,
                        draws_mat = draws_mat) {

  set.seed(seed)
  dm <- draws_mat[sample.int(nrow(draws_mat), min(S, nrow(draws_mat))), , drop = FALSE]

  N   <- nrow(shots_df)
  out <- numeric(N)

  fam  <- as.character(shots_df$shot_family)

  # Pre-extract parameter columns
  rim_cols <- c("alpha_rim", "bD_rim", "bA_rim", "bDunk_rim",
                "bT_rim", "b2nd_rim", "bTrans_rim", "bDef_rim", "bH_rim")
  j2_cols  <- c("alpha_j2", "bD_j2", "bA_j2",
                "bT_j2", "b2nd_j2", "bTrans_j2", "bDef_j2", "bCS_j2", "bH_j2")
  j3_cols  <- c("alpha_j3", "bD_j3", "bA_j3",
                "bT_j3", "b2nd_j3", "bTrans_j3", "bDef_j3", "bCS_j3", "bH_j3")

  B_rim <- dm[, rim_cols, drop = FALSE]
  B_j2  <- dm[, j2_cols,  drop = FALSE]
  B_j3  <- dm[, j3_cols,  drop = FALSE]

  # Split by family and vectorize
  idx_rim <- which(fam == "rim")
  idx_j2  <- which(fam == "j2")
  idx_j3  <- which(fam == "j3")

  if (length(idx_rim) > 0) {
    # Design matrix: intercept, D, A, is_dunk, T, is_second, is_trans, def_dist, ht_match
    X_rim <- rbind(1,
                   shots_df$D[idx_rim],
                   shots_df$A[idx_rim],
                   shots_df$is_dunk[idx_rim],
                   shots_df$T[idx_rim],
                   shots_df$is_second_chance[idx_rim],
                   shots_df$is_transition[idx_rim],
                   shots_df$def_dist_scaled[idx_rim],
                   shots_df$ht_match_scaled[idx_rim])
    out[idx_rim] <- colMeans(plogis(B_rim %*% X_rim))
  }

  if (length(idx_j2) > 0) {
    X_j2 <- rbind(1,
                  shots_df$D[idx_j2],
                  shots_df$A[idx_j2],
                  shots_df$T[idx_j2],
                  shots_df$is_second_chance[idx_j2],
                  shots_df$is_transition[idx_j2],
                  shots_df$def_dist_scaled[idx_j2],
                  shots_df$is_catch_shoot[idx_j2],
                  shots_df$ht_match_scaled[idx_j2])
    out[idx_j2] <- colMeans(plogis(B_j2 %*% X_j2))
  }

  if (length(idx_j3) > 0) {
    X_j3 <- rbind(1,
                  shots_df$D[idx_j3],
                  shots_df$A[idx_j3],
                  shots_df$T[idx_j3],
                  shots_df$is_second_chance[idx_j3],
                  shots_df$is_transition[idx_j3],
                  shots_df$def_dist_scaled[idx_j3],
                  shots_df$is_catch_shoot[idx_j3],
                  shots_df$ht_match_scaled[idx_j3])
    out[idx_j3] <- colMeans(plogis(B_j3 %*% X_j3))
  }

  out
}


shots <- shots %>%
  mutate(
    xfg       = get_xfg_v2(shots, S = 200, draws_mat = draws_mat),
    xpoints   = xfg * (2 + is_three),
    points    = SHOT_MADE_FLAG * (2 + is_three)
  )


## =============================================================================
## 7 ─ PLAYER-LEVEL SUMMARY TABLE
## =============================================================================

player_tbl <- shots %>%
  group_by(PLAYER_ID, PLAYER_NAME) %>%
  summarise(
    fga           = n(),
    fg_pct        = mean(SHOT_MADE_FLAG),
    xfg_pct       = mean(xfg),
    fg_over_exp   = fg_pct - xfg_pct,
    xpoints_ps    = mean(xpoints),
    points_ps     = mean(points),
    pps_over_exp  = points_ps - xpoints_ps,
    xpoints       = sum(xpoints),
    points        = sum(points),
    points_over_exp = points - xpoints,
    .groups = "drop"
  ) %>%
  arrange(desc(fg_over_exp)) %>%
  select(-PLAYER_ID)

cat("\n═══ TOP PLAYERS BY FG% OVER EXPECTED (min 500 FGA) ═══\n")
player_tbl %>%
  filter(fga > 500) %>%
  head(20) %>%
  print(n = 20)


## =============================================================================
## 8 ─ DEFENSIVE TEAM EFFECTS
## =============================================================================

# Extract defensive team random effects on probability scale
J_defteam <- stan_data$J_defteam
sigma_def_cols <- paste0("sigma_defteam_type[", 1:3, "]")
alpha_names    <- c("alpha_rim", "alpha_j2", "alpha_j3")

get_z_defteam <- function(dm, t, J) {
  pat  <- paste0("^z_defteam\\[(\\d+),\\s*", t, "\\]$")
  cols <- grep(pat, colnames(dm), value = TRUE)
  idx  <- as.integer(sub(pat, "\\1", cols))
  cols <- cols[order(idx)]
  dm[, cols, drop = FALSE]
}

defteam_effects <- bind_rows(lapply(1:3, function(t) {
  z_mat <- get_z_defteam(draws_mat, t, J_defteam)
  s_vec <- draws_mat[, sigma_def_cols[t], drop = TRUE]
  a_mat <- sweep(z_mat, 1, s_vec, "*")  # S × J_defteam

  tibble(
    defteam_idx  = 1:J_defteam,
    shot_type    = t,
    shot_family  = c("rim", "j2", "j3")[t],
    effect_mean  = colMeans(a_mat),
    effect_q05   = apply(a_mat, 2, quantile, 0.05),
    effect_q95   = apply(a_mat, 2, quantile, 0.95)
  )
})) %>%
  left_join(defteam_map, by = "defteam_idx")

cat("\n═══ DEFENSIVE TEAM EFFECTS (logit scale) ═══\n")
defteam_effects %>%
  filter(shot_family == "overall" | shot_family == "j3") %>%
  arrange(shot_family, effect_mean) %>%
  print(n = 30)


## =============================================================================
## 9 ─ xFG SURFACE PLOTS (updated with new features at "average" context)
## =============================================================================

par_hat <- colMeans(draws_mat[, context_params, drop = FALSE])

# For the surface, we set the new features at their "average" values:
# seconds since play start = 0 (scaled mean), second chance = 0,
# transition = 0, defender distance = 0 (scaled mean),
# catch-and-shoot = 0 (pullup), height matchup = 0 (even matchup).

x_seq <- seq(-250, 250, by = 2.5)
y_seq <- seq(-47.5, 422.5, by = 2.5)

grid <- tidyr::expand_grid(loc_x = x_seq, loc_y = y_seq) %>%
  mutate(
    shot_dist = sqrt(loc_x^2 + loc_y^2) / 10,
    angle     = atan2(abs(loc_x), pmax(loc_y, 1e-6)),
    is_three  = as.integer(if_else(loc_y <= 92.5,
                                   abs(loc_x) >= 220,
                                   sqrt(loc_x^2 + loc_y^2) >= 237.5)),
    is_jump   = as.integer(shot_dist > 4),
    is_dunk   = 0L,
    shot_family = case_when(is_jump == 0 ~ "rim",
                            is_jump == 1 & is_three == 0 ~ "j2",
                            TRUE ~ "j3")
  ) %>%
  left_join(scaling_params, by = "shot_family") %>%
  mutate(
    D = (shot_dist - mu_dist) / sd_dist,
    A = (angle    - mu_ang)  / sd_ang,

    # All new context features at their "neutral" scaled values
    T_sc       = 0,   # average seconds since play start
    def_dist_sc = 0,  # average defender distance
    ht_match_sc = 0,  # even height matchup

    eta = case_when(
      shot_family == "rim" ~
        par_hat["alpha_rim"]  + par_hat["bD_rim"]*D + par_hat["bA_rim"]*A +
        par_hat["bDunk_rim"]*0 + par_hat["bT_rim"]*0 + par_hat["b2nd_rim"]*0 +
        par_hat["bTrans_rim"]*0 + par_hat["bDef_rim"]*0 + par_hat["bH_rim"]*0,

      shot_family == "j2" ~
        par_hat["alpha_j2"] + par_hat["bD_j2"]*D + par_hat["bA_j2"]*A +
        par_hat["bT_j2"]*0 + par_hat["b2nd_j2"]*0 + par_hat["bTrans_j2"]*0 +
        par_hat["bDef_j2"]*0 + par_hat["bCS_j2"]*0 + par_hat["bH_j2"]*0,

      TRUE ~
        par_hat["alpha_j3"] + par_hat["bD_j3"]*D + par_hat["bA_j3"]*A +
        par_hat["bT_j3"]*0 + par_hat["b2nd_j3"]*0 + par_hat["bTrans_j3"]*0 +
        par_hat["bDef_j3"]*0 + par_hat["bCS_j3"]*0 + par_hat["bH_j3"]*0
    ),

    xfg     = inv_logit(eta),
    xpoints = xfg * (2 + is_three)
  )


## Court drawing (same helper as your v1)
source("court_helpers.R")  # or paste your nba_halfcourt_layers() here

p_surface_xfg <- ggplot(grid, aes(x = loc_x, y = loc_y, fill = xfg)) +
  geom_raster(interpolate = TRUE) +
  nba_halfcourt_layers(line_size = 0.35) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(labels = percent_format(accuracy = 1), name = "xFG%") +
  labs(title = "Model v2 — xFG% surface (average context, no player effects)") +
  theme_void() +
  theme(legend.position = "right", plot.title = element_text(hjust = 0.5))

p_surface_xpts <- ggplot(grid, aes(x = loc_x, y = loc_y, fill = xpoints)) +
  geom_raster(interpolate = TRUE) +
  nba_halfcourt_layers(line_size = 0.35) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(labels = number_format(accuracy = 0.01), name = "xPts") +
  labs(title = "Model v2 — xPoints surface (average context)") +
  theme_void() +
  theme(legend.position = "right", plot.title = element_text(hjust = 0.5))


## =============================================================================
## 10 ─ CONTEXT-ADJUSTED SURFACE: transition vs half-court
## =============================================================================

# Show how the surface shifts for transition vs half-court shots.
# Transition: is_transition = 1, T_scaled ~ -1.5 (fast), more open defense
# Half-court: is_transition = 0, T_scaled ~ 0.5 (longer possession)

grid_transition <- grid %>%
  mutate(
    context = "Transition",
    eta_adj = eta +
      case_when(
        shot_family == "rim" ~ par_hat["bTrans_rim"]*1 + par_hat["bT_rim"]*(-1.5) +
                               par_hat["bDef_rim"]*1,     # more open
        shot_family == "j2"  ~ par_hat["bTrans_j2"]*1 + par_hat["bT_j2"]*(-1.5) +
                               par_hat["bDef_j2"]*1 + par_hat["bCS_j2"]*1,
        TRUE                 ~ par_hat["bTrans_j3"]*1 + par_hat["bT_j3"]*(-1.5) +
                               par_hat["bDef_j3"]*1 + par_hat["bCS_j3"]*1
      ),
    xfg_adj = inv_logit(eta_adj),
    xpts_adj = xfg_adj * (2 + is_three)
  )

grid_halfcourt <- grid %>%
  mutate(
    context = "Half-court (late clock)",
    eta_adj = eta +
      case_when(
        shot_family == "rim" ~ par_hat["bT_rim"]*1.5 + par_hat["bDef_rim"]*(-0.5),
        shot_family == "j2"  ~ par_hat["bT_j2"]*1.5 + par_hat["bDef_j2"]*(-0.5),
        TRUE                 ~ par_hat["bT_j3"]*1.5 + par_hat["bDef_j3"]*(-0.5)
      ),
    xfg_adj = inv_logit(eta_adj),
    xpts_adj = xfg_adj * (2 + is_three)
  )

# Side-by-side comparison
p_trans <- ggplot(grid_transition, aes(x = loc_x, y = loc_y, fill = xpts_adj)) +
  geom_raster(interpolate = TRUE) +
  nba_halfcourt_layers(line_size = 0.3) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(limits = c(0.4, 1.5), name = "xPts") +
  labs(title = "Transition") + theme_void()

p_hc <- ggplot(grid_halfcourt, aes(x = loc_x, y = loc_y, fill = xpts_adj)) +
  geom_raster(interpolate = TRUE) +
  nba_halfcourt_layers(line_size = 0.3) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(limits = c(0.4, 1.5), name = "xPts") +
  labs(title = "Half-court (late clock)") + theme_void()

p_context_compare <- p_trans + p_hc +
  plot_annotation(title = "Context-adjusted xPoints surfaces",
                  subtitle = "Same location, different game situation")


## =============================================================================
## 11 ─ UPDATED EPAA COMPUTATION (with defensive team effects)
## =============================================================================

# Points Above Average per shot, now incorporating defensive team effects.
# EPAA = (p_skill - p_context) × point_value
#
# p_context: average player, this shot's context + defensive team effect
# p_skill:   THIS player, this shot's context + defensive team effect

J_players <- stan_data$J_player
sigma_player_cols <- paste0("sigma_player_type[", 1:3, "]")

get_z_player <- function(dm, t, J) {
  pat  <- paste0("^z_player\\[(\\d+),\\s*", t, "\\]$")
  cols <- grep(pat, colnames(dm), value = TRUE)
  idx  <- as.integer(sub(pat, "\\1", cols))
  cols <- cols[order(idx)]
  dm[, cols, drop = FALSE]
}

player_pts_by_family <- shots %>%
  mutate(
    # Context-only linear predictor (no player effects, yes defensive team)
    eta_context = case_when(
      shot_type == 1L ~
        par_hat["alpha_rim"] + par_hat["bD_rim"]*D + par_hat["bA_rim"]*A +
        par_hat["bDunk_rim"]*is_dunk + par_hat["bT_rim"]*T +
        par_hat["b2nd_rim"]*is_second_chance + par_hat["bTrans_rim"]*is_transition +
        par_hat["bDef_rim"]*def_dist_scaled + par_hat["bH_rim"]*ht_match_scaled,
      shot_type == 2L ~
        par_hat["alpha_j2"] + par_hat["bD_j2"]*D + par_hat["bA_j2"]*A +
        par_hat["bT_j2"]*T + par_hat["b2nd_j2"]*is_second_chance +
        par_hat["bTrans_j2"]*is_transition + par_hat["bDef_j2"]*def_dist_scaled +
        par_hat["bCS_j2"]*is_catch_shoot + par_hat["bH_j2"]*ht_match_scaled,
      TRUE ~
        par_hat["alpha_j3"] + par_hat["bD_j3"]*D + par_hat["bA_j3"]*A +
        par_hat["bT_j3"]*T + par_hat["b2nd_j3"]*is_second_chance +
        par_hat["bTrans_j3"]*is_transition + par_hat["bDef_j3"]*def_dist_scaled +
        par_hat["bCS_j3"]*is_catch_shoot + par_hat["bH_j3"]*ht_match_scaled
    ),

    p_context = plogis(eta_context),

    # Add player skill (posterior mean)
    p_skill = {
      # Pre-compute player effects for each shot type
      a_hat <- matrix(0, nrow = J_players, ncol = 3)
      for (t in 1:3) {
        z_mat <- get_z_player(draws_mat, t, J_players)
        s_vec <- draws_mat[, sigma_player_cols[t], drop = TRUE]
        a_hat[, t] <- colMeans(sweep(z_mat, 1, s_vec, "*"))
      }
      # Look up per shot
      plogis(eta_context + a_hat[cbind(player_idx, shot_type)])
    },

    point_value = if_else(shot_type == 3L, 3, 2),
    pts_above_avg = (p_skill - p_context) * point_value
  ) %>%
  group_by(player_idx, PLAYER_ID, PLAYER_NAME, shot_family) %>%
  summarise(
    fga              = n(),
    pts_aa_total     = sum(pts_above_avg),
    pts_aa_per100    = 100 * pts_aa_total / fga,
    .groups = "drop"
  ) %>%
  arrange(desc(pts_aa_total))

cat("\n═══ POINTS ABOVE AVERAGE — TOP PLAYERS (j3, min 200 FGA) ═══\n")
player_pts_by_family %>%
  filter(shot_family == "j3", fga >= 200) %>%
  head(15) %>%
  print()


## =============================================================================
## 12 ─ POSTERIOR PREDICTIVE CHECK
## =============================================================================

# Calibration: bin shots by predicted xFG% and compare to observed FG%
shots %>%
  mutate(xfg_bin = cut(xfg, breaks = seq(0, 1, by = 0.05))) %>%
  group_by(xfg_bin) %>%
  summarise(n = n(),
            predicted = mean(xfg),
            observed  = mean(SHOT_MADE_FLAG),
            .groups   = "drop") %>%
  filter(n >= 50) %>%
  ggplot(aes(x = predicted, y = observed)) +
  geom_point(aes(size = n), alpha = 0.7) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
  scale_size_continuous(range = c(1, 8)) +
  labs(title = "Posterior Predictive Calibration",
       subtitle = "Predicted xFG% vs observed FG% (binned)",
       x = "Mean predicted xFG%", y = "Observed FG%") +
  theme_minimal()


## =============================================================================
## 13 ─ FEATURE IMPORTANCE: compare coefficient magnitudes
## =============================================================================

# Extract posterior summaries for all fixed effects to show
# which features matter most (on standardized scale)
coef_summary <- fit_v2$summary(variables = context_params) %>%
  mutate(
    shot_family = case_when(str_detect(variable, "rim") ~ "rim",
                            str_detect(variable, "j2")  ~ "j2",
                            str_detect(variable, "j3")  ~ "j3"),
    feature = str_replace(variable, "_(rim|j2|j3)$", "") %>%
      str_replace("^b", "")
  )

ggplot(coef_summary %>% filter(!str_detect(variable, "^alpha")),
       aes(x = reorder(variable, abs(mean)), y = mean)) +
  geom_point(size = 2) +
  geom_errorbar(aes(ymin = q5, ymax = q95), width = 0.2) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  coord_flip() +
  facet_wrap(~shot_family, scales = "free_y") +
  labs(title = "Fixed Effect Estimates (90% CIs)",
       x = NULL, y = "Logit-scale coefficient") +
  theme_minimal()


## =============================================================================
## 14 ─ SAVE OUTPUTS
## =============================================================================

write_csv(shots %>% select(-on_court_defenders),
          "shots_v2_with_xfg.csv")
write_csv(player_tbl, "player_summary_v2.csv")
write_csv(player_pts_by_family %>% select(-player_idx, -PLAYER_ID),
          "player_epaa_v2.csv")
write_csv(defteam_effects, "defteam_effects_v2.csv")

# Save draws for downstream use
saveRDS(draws_mat, "draws_mat_v2.rds")

message("\n─── Fitting and analysis complete ───")
