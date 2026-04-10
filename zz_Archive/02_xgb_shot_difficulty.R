## =============================================================================
## 02_xgb_shot_difficulty.R
## NBA xFG — Stage 1: XGBoost context-only shot difficulty model
##
## Fits 3 separate XGBoost models (rim / j2 / j3) using ONLY context features.
## Player/defender identity is intentionally EXCLUDED — we want the model to
## answer "how hard was this shot for a league-average player?", not "how good
## is this player?". Player skill is estimated in Stage 2 (Stan).
##
## Inputs:  shots_enriched.csv (from 01_data_pipeline.R)
## Outputs: shots_with_xfg.csv, xgb models (.rds), SHAP plots, calibration
## =============================================================================

library(dplyr)
library(readr)
library(xgboost)
library(ggplot2)
library(patchwork)
library(scales)

inv_logit <- function(x) 1 / (1 + exp(-x))
set.seed(42)


## =============================================================================
## 1 — LOAD DATA
## =============================================================================

shots <- pbp_all %>% 
  filter(fga == 1) %>% 
  mutate(height_diff = player_height - defender_height,
         wingspan_diff = player_wingspan - defender_wingspan)

cat("Loaded", nrow(shots), "FGA rows\n")
cat("  rim:", sum(shots$shot_family == "rim"),
    "  j2:", sum(shots$shot_family == "j2"),
    "  j3:", sum(shots$shot_family == "j3"), "\n")


## =============================================================================
## 2 — DEFINE FEATURES PER SHOT FAMILY
## =============================================================================
##
## Key principle: NO player or defender identity features. Only context.
## The features should capture everything about the shot SITUATION that
## affects difficulty: where, when, how, and what kind of defense.

# Shared continuous features (used on raw scale — XGBoost handles non-linearity)
shared_features <- c("shot_distance",     # feet from basket
                     "shot_angle",        # degrees from center
                     "height_diff",       # shooter_ht - defender_ht (inches)
                     "wingspan_diff",     # shooter_ht - defender_ht (inches)
                     "player_min_game",   # game minutes played
                     "player_min_stint",  # stint minutes played
                     "shot_clock")

shared_features_factors <- c("defender_dist_range",
                             "dribble_range")

# Shared binary features
shared_binary <- c("is_2ndchance",       # offensive rebound putback
                   "is_fastbreak",       # transition opportunity
                   "is_fromturnover")    # live-ball turnover

# Family-specific feature lists
features_rim <- c(shared_features, shared_features_factors, shared_binary, "is_dunk")
features_j2  <- c(shared_features, shared_features_factors, shared_binary)
features_j3  <- c(shared_features, shared_features_factors, shared_binary)


## =============================================================================
## 3 — PREPARE DATA SPLITS
## =============================================================================
##
## Time-based holdout: last 15% of games by date for validation.
## This mimics how the model would be used in practice (predict future games).

game_dates <- shots %>%
  distinct(game_id, game_date) %>%
  arrange(game_date)

cutoff_idx <- floor(0.85 * nrow(game_dates))
cutoff_date <- game_dates$game_date[cutoff_idx]

shots <- shots %>%
  mutate(split = if_else(game_date <= cutoff_date, "train", "test"))

cat("\nTrain:", sum(shots$split == "train"), "shots (",
    round(100 * mean(shots$split == "train"), 1), "%)\n")
cat("Test: ", sum(shots$split == "test"), "shots (",
    round(100 * mean(shots$split == "test"), 1), "%)\n")

## Guard against missing features: fill NAs with sensible defaults
shots <- shots %>%
  mutate(
    height_matchup       = coalesce(height_matchup, 0),
    sec_since_play_start = coalesce(sec_since_play_start, 14),
    expected_def_dist    = coalesce(expected_def_dist, 4.0),
    is_catch_shoot       = coalesce(is_catch_shoot, 0L),
    is_2ndchance         = coalesce(as.integer(is_2ndchance), 0L),
    is_fastbreak         = coalesce(as.integer(is_fastbreak), 0L),
    is_fromturnover      = coalesce(as.integer(is_fromturnover), 0L)
  )


## =============================================================================
## 4 — FIT XGBoost PER SHOT FAMILY
## =============================================================================

fit_xgb_family <- function(shots_df, features, family_name, nrounds = 500) {

  train <- shots_df %>% filter(split == "train", shot_family == family_name)
  test  <- shots_df %>% filter(split == "test",  shot_family == family_name)

  X_train <- as.matrix(train[, features])
  y_train <- train$fgm
  X_test  <- as.matrix(test[, features])
  y_test  <- test$fgm

  dtrain <- xgb.DMatrix(data = X_train, label = y_train)
  dtest  <- xgb.DMatrix(data = X_test,  label = y_test)

  # Hyperparameters tuned for ~200K shot classification
  # Conservative depth to prevent overfitting on player-like patterns
  params <- list(
    objective        = "binary:logistic",
    eval_metric      = "logloss",
    max_depth        = 5,
    eta              = 0.05,
    subsample        = 0.8,
    colsample_bytree = 0.8,
    min_child_weight = 50,      # conservative: prevents fitting to rare combos
    gamma            = 0.1
  )

  # Train with early stopping on holdout
  model <- xgb.train(
    params    = params,
    data      = dtrain,
    nrounds   = nrounds,
    watchlist = list(train = dtrain, test = dtest),
    early_stopping_rounds = 30,
    verbose   = 1,
    print_every_n = 50
  )

  cat("\n", family_name, ": best iteration =", model$best_iteration,
      "  test logloss =", round(model$best_score, 5), "\n")

  # Feature importance
  imp <- xgb.importance(model = model)
  cat("  Top features:\n")
  print(head(imp, 10))

  list(
    model   = model,
    imp     = imp,
    dtrain  = dtrain,
    dtest   = dtest,
    X_train = X_train,
    X_test  = X_test,
    y_train = y_train,
    y_test  = y_test
  )
}

cat("\n══════════════════════════════════════════\n")
cat("  Fitting XGBoost: RIM\n")
cat("══════════════════════════════════════════\n")
fit_rim <- fit_xgb_family(shots, features_rim, "rim")

cat("\n══════════════════════════════════════════\n")
cat("  Fitting XGBoost: J2\n")
cat("══════════════════════════════════════════\n")
fit_j2 <- fit_xgb_family(shots, features_j2, "j2")

cat("\n══════════════════════════════════════════\n")
cat("  Fitting XGBoost: J3\n")
cat("══════════════════════════════════════════\n")
fit_j3 <- fit_xgb_family(shots, features_j3, "j3")


## =============================================================================
## 5 — PREDICT xFG FOR ALL SHOTS
## =============================================================================

shots$xfg <- NA_real_

for (fam_info in list(
  list(name = "rim", fit = fit_rim, feats = features_rim),
  list(name = "j2",  fit = fit_j2,  feats = features_j2),
  list(name = "j3",  fit = fit_j3,  feats = features_j3)
)) {
  idx <- which(shots$shot_family == fam_info$name)
  X   <- as.matrix(shots[idx, fam_info$feats])
  d   <- xgb.DMatrix(data = X)
  shots$xfg[idx] <- predict(fam_info$fit$model, d)
}

shots <- shots %>%
  mutate(
    xfg_logit = qlogis(pmax(pmin(xfg, 0.999), 0.001)),  # for Stan Stage 2
    xpoints   = xfg * if_else(is_three == 1, 3, 2),
    points    = fgm * if_else(is_three == 1, 3, 2)
  )


## =============================================================================
## 6 — CALIBRATION CHECK (per family, train vs test)
## =============================================================================

calibration_plot <- function(shots_df, family_name) {

  cal <- shots_df %>%
    filter(shot_family == family_name) %>%
    mutate(xfg_bin = cut(xfg, breaks = seq(0, 1, by = 0.05))) %>%
    group_by(split, xfg_bin) %>%
    summarise(
      n         = n(),
      predicted = mean(xfg),
      observed  = mean(fgm),
      .groups   = "drop"
    ) %>%
    filter(n >= 30)

  ggplot(cal, aes(x = predicted, y = observed, color = split)) +
    geom_point(aes(size = n), alpha = 0.7) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey40") +
    scale_size_continuous(range = c(1, 6)) +
    labs(title = paste0(family_name, " calibration"),
         x = "Predicted xFG%", y = "Observed FG%") +
    theme_minimal() +
    theme(legend.position = "bottom")
}

p_cal <- calibration_plot(shots, "rim") +
  calibration_plot(shots, "j2") +
  calibration_plot(shots, "j3") +
  plot_annotation(title = "XGBoost Calibration: Predicted vs Observed FG%",
                  subtitle = "Train (blue) vs holdout (red) — 5% bins, min 30 shots")

ggsave("xgb_calibration.png", p_cal, width = 14, height = 5, dpi = 150)
cat("\nCalibration plot saved: xgb_calibration.png\n")


## =============================================================================
## 7 — FEATURE IMPORTANCE PLOTS (per family)
## =============================================================================

plot_importance <- function(imp_df, family_name, top_n = 10) {
  imp_df %>%
    head(top_n) %>%
    ggplot(aes(x = reorder(Feature, Gain), y = Gain)) +
    geom_col(fill = "steelblue") +
    coord_flip() +
    labs(title = paste0(family_name, " feature importance"),
         x = NULL, y = "Gain") +
    theme_minimal()
}

p_imp <- plot_importance(fit_rim$imp, "rim") +
  plot_importance(fit_j2$imp, "j2") +
  plot_importance(fit_j3$imp, "j3") +
  plot_annotation(title = "XGBoost Feature Importance by Shot Family")

ggsave("xgb_feature_importance.png", p_imp, width = 14, height = 5, dpi = 150)
cat("Feature importance plot saved: xgb_feature_importance.png\n")


## =============================================================================
## 8 — SHAP VALUES (optional, requires shapviz)
## =============================================================================

tryCatch({
  library(shapviz)

  for (fam_info in list(
    list(name = "rim", fit = fit_rim, feats = features_rim),
    list(name = "j2",  fit = fit_j2,  feats = features_j2),
    list(name = "j3",  fit = fit_j3,  feats = features_j3)
  )) {
    # Sample 5000 shots for SHAP (computational cost)
    idx <- which(shots$shot_family == fam_info$name)
    samp <- sample(idx, min(5000, length(idx)))
    X_samp <- as.matrix(shots[samp, fam_info$feats])

    shp <- shapviz(fam_info$fit$model, X_pred = X_samp, X = X_samp)

    p <- sv_importance(shp, kind = "beeswarm") +
      labs(title = paste0(fam_info$name, " SHAP values")) +
      theme_minimal()

    ggsave(paste0("shap_", fam_info$name, ".png"), p, width = 8, height = 6, dpi = 150)
    cat("SHAP plot saved: shap_", fam_info$name, ".png\n")
  }
}, error = function(e) {
  message("SHAP plots skipped (install shapviz): ", e$message)
})


## =============================================================================
## 9 — HOLDOUT PERFORMANCE SUMMARY
## =============================================================================

test_perf <- shots %>%
  filter(split == "test") %>%
  group_by(shot_family) %>%
  summarise(
    n         = n(),
    obs_fg    = mean(fgm),
    pred_xfg  = mean(xfg),
    logloss   = -mean(fgm * log(pmax(xfg, 1e-7)) +
                      (1 - fgm) * log(pmax(1 - xfg, 1e-7))),
    brier     = mean((fgm - xfg)^2),
    .groups   = "drop"
  )

cat("\n══════════════════════════════════════════\n")
cat("  HOLDOUT PERFORMANCE\n")
cat("══════════════════════════════════════════\n")
print(test_perf)

# Overall
overall <- shots %>%
  filter(split == "test") %>%
  summarise(
    n = n(),
    logloss = -mean(fgm * log(pmax(xfg, 1e-7)) +
                    (1 - fgm) * log(pmax(1 - xfg, 1e-7))),
    brier   = mean((fgm - xfg)^2)
  )
cat("\nOverall holdout:  logloss =", round(overall$logloss, 5),
    "  brier =", round(overall$brier, 5), "\n")


## =============================================================================
## 10 — SAVE OUTPUTS
## =============================================================================

# Shots with xFG predictions (input to Stage 2 Stan model)
write_csv(shots, "shots_with_xfg.csv")

# Save models for reuse
saveRDS(fit_rim$model, "xgb_rim.rds")
saveRDS(fit_j2$model,  "xgb_j2.rds")
saveRDS(fit_j3$model,  "xgb_j3.rds")

# Save feature lists for prediction on new data
saveRDS(list(rim = features_rim, j2 = features_j2, j3 = features_j3),
        "xgb_features.rds")

cat("\n── Stage 1 complete ──\n")
cat("  shots_with_xfg.csv  (", nrow(shots), " rows, includes xfg + xfg_logit)\n")
cat("  xgb_rim/j2/j3.rds  (saved models)\n")
cat("  → Ready for 03_stan_random_effects.R (Stage 2)\n")
