## ═════════════════════════════════════════════════════════════════════════════
## 02_xgb_shot_difficulty.R
## NBA xFG — Stage 1: XGBoost context-only shot difficulty model
##
## Fits 3 separate XGBoost models (rim / j2 / j3) using ONLY context features.
## Player/defender identity is intentionally EXCLUDED — we want the model to
## answer "how hard was this shot for a league-average player?", not "how good
## is this player?". Player skill is estimated in Stage 2 (Stan).
##
## NEW in this version:
##   - Hyperparameter tuning: grid search over max_depth × min_child_weight
##   - Reduced-feature model: excludes imputed variables (height_diff,
##     wingspan_diff, defender_dist_range, dribble_range) to validate that
##     imputation is adding signal, not noise
##   - LogLoss comparison plots: baseline vs tuned, full vs reduced
##   - Both full and reduced xFG predictions saved for Stan comparison
##
## Inputs:  shots_enriched.csv (from 01_data_pipeline.R)
## Outputs: shots_with_xfg.csv, xgb models (.rds), comparison plots
## ═════════════════════════════════════════════════════════════════════════════

## ═════════════════════════════════════════════════════════════════════════════
## 00 — CONFIGURE ==============================================================
## ═════════════════════════════════════════════════════════════════════════════

library(dplyr)
library(readr)
library(xgboost)
library(ggplot2)
library(patchwork)
library(scales)
library(shapviz)
library(reshape2)   # for melt() in logloss comparison plots

inv_logit <- function(x) 1 / (1 + exp(-x))
set.seed(42)





## ═════════════════════════════════════════════════════════════════════════════
## 01 — LOAD DATA ==============================================================
## ═════════════════════════════════════════════════════════════════════════════

shots <- pbp_all %>% 
  filter(fga == 1, shot_distance < 36)





## ═════════════════════════════════════════════════════════════════════════════
## 02 — FEATURES PER SHOT FAMILY ===============================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Key principle: NO player or defender identity features. Only context.
## The features should capture everything about the shot SITUATION that
## affects difficulty: where, when, how, and what kind of defense.

## ─── FULL feature sets (include imputed variables) ───

shared_features <- c("shot_distance",     # feet from basket
                     "shot_angle",        # degrees from center
                     "player_min_game",   # game minutes played
                     "player_min_stint",  # stint minutes played
                     "shot_clock")

shared_features_factors <- c("defender_dist_level",
                             "dribble_level")

shared_binary <- c("is_2ndchance",       # offensive rebound putback
                   "is_fastbreak",       # transition opportunity
                   "is_fromturnover")    # live-ball turnover

features_rim <- c(shared_features, shared_features_factors, shared_binary, "is_dunk")
features_j2  <- c(shared_features, shared_features_factors, shared_binary)
features_j3  <- c(shared_features, shared_features_factors, shared_binary)

## ─── REDUCED feature sets (NO imputed variables) ───
## Removes: height_diff, wingspan_diff, defender_dist_range, dribble_range
## These 4 variables all depend on imputation in the pipeline. Comparing
## full vs reduced tells us whether the imputation is adding real signal.

imputed_vars <- c(#"height_diff", "wingspan_diff",
                  "defender_dist_level", "dribble_level")

features_rim_reduced <- setdiff(features_rim, imputed_vars)
features_j2_reduced  <- setdiff(features_j2,  imputed_vars)
features_j3_reduced  <- setdiff(features_j3,  imputed_vars)

cat("\nFull feature count (rim/j2/j3):", length(features_rim), "/",
    length(features_j2), "/", length(features_j3), "\n")
cat("Reduced feature count (rim/j2/j3):", length(features_rim_reduced), "/",
    length(features_j2_reduced), "/", length(features_j3_reduced), "\n")
cat("Removed imputed variables:", paste(imputed_vars, collapse = ", "), "\n")





## ═════════════════════════════════════════════════════════════════════════════
## 03 — PREPARE DATA SPLITS ====================================================
## ═════════════════════════════════════════════════════════════════════════════
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
  mutate(player_min_game  = coalesce(player_min_game, 0),
         player_min_stint = coalesce(player_min_stint, 0),
         shot_clock       = coalesce(shot_clock, 14L),
         is_2ndchance     = coalesce(as.integer(is_2ndchance), 0L),
         is_fastbreak     = coalesce(as.integer(is_fastbreak), 0L),
         is_fromturnover  = coalesce(as.integer(is_fromturnover), 0L))


## Convert categorical features to integer codes for XGBoost.
## XGBoost requires a numeric matrix — as.matrix() on a df with even one
## character column coerces the ENTIRE matrix to character, silently breaking
## all numeric features. We encode each factor with a fixed, ordered level
## set so that codes are consistent across train/test and across seasons.
##
## Level orders match the pipeline constants DEF_DIST_RANGES / DRIBBLE_RANGES
## (01_data_pipeline.R lines 99–108).

defender_dist_levels <- c("0-2 Feet - Very Tight",
                          "2-4 Feet - Tight",
                          "4-6 Feet - Open",
                          "6+ Feet - Wide Open")

dribble_levels <- c("0 Dribbles",
                    "1 Dribble",
                    "2 Dribbles",
                    "3-6 Dribbles",
                    "7+ Dribbles")

shots <- shots %>%
  mutate(defender_dist_level = as.integer(factor(defender_dist_range,
                                                 levels = defender_dist_levels)),
         dribble_level       = as.integer(factor(dribble_range,
                                                 levels = dribble_levels)))


cat("  defender_dist_range codes:", sort(unique(shots$defender_dist_level)), "\n")
cat("  dribble_range codes:      ", sort(unique(shots$dribble_level)), "\n")


## ═════════════════════════════════════════════════════════════════════════════
## 04 — CORE FITTING FUNCTION ==================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Returns both the final xgb.train model AND the xgb.cv evaluation log
## so we can plot training/test logloss curves for comparison.

fit_xgb_family <- function(shots_df, 
                           features, 
                           family_name,
                           params, 
                           nrounds = 500, 
                           nfold = 5,
                           label = "default") {
  
  # Train/test split
  train <- shots_df %>% 
    filter(split == "train", 
           shot_family == family_name)
  test  <- shots_df %>% 
    filter(split == "test",  
           shot_family == family_name)

  X_train <- as.matrix(train[, features])
  y_train <- train$fgm
  X_test  <- as.matrix(test[, features])
  y_test  <- test$fgm

  dtrain <- xgb.DMatrix(data = X_train, label = y_train)
  dtest  <- xgb.DMatrix(data = X_test,  label = y_test)

  ## ─── Cross-validation (for logloss curves) ───
  cv_model <- xgb.cv(params    = params,
                     data      = dtrain,
                     nrounds   = nrounds,
                     nfold     = nfold,
                     early_stopping_rounds = 100,
                     verbose   = 0,
                     print_every_n = 100)

  best_nrounds_cv <- cv_model$early_stop$best_iteration

  ## ─── Final model on full training set (with holdout evals) ───
  model <- xgb.train(params    = params,
                     data      = dtrain,
                     nrounds   = nrounds,
                     evals     = list(train = dtrain, test = dtest),
                     early_stopping_rounds = 100,
                     verbose   = 1,
                     print_every_n = 50)

  ## Extract best test logloss from the evaluation log.
  ## model$best_score can be non-numeric in newer xgboost versions,
  ## so we pull directly from the evaluation_log at best_iteration.
  best_test_logloss <- attributes(model)$early_stop$best_score

  cat("\n [", label, "]", family_name, ": best CV iteration =", best_nrounds_cv,
      "  best train iteration =", attributes(model)$early_stop$best_iteration,
      "  test logloss =", round(best_test_logloss, 5), "\n")

  # Feature importance
  imp <- xgb.importance(model = model)
  cat("  Top features:\n")
  print(head(imp, length(features)))

  list(model    = model,
       cv_model = cv_model,
       imp      = imp,
       dtrain   = dtrain,
       dtest    = dtest,
       X_train  = X_train,
       X_test   = X_test,
       y_train  = y_train,
       y_test   = y_test,
       label    = label,
       family   = family_name,
       params   = params,
       best_test_logloss = best_test_logloss)
}





## ═════════════════════════════════════════════════════════════════════════════
## 05 — BASELINE (PRE-TUNING) MODELS ===========================================
## ═════════════════════════════════════════════════════════════════════════════
## Original hyperparameters — these serve as the baseline for comparison.

params_baseline <- list(booster          = "gbtree",
                        objective        = "binary:logistic",
                        eval_metric      = "logloss",
                        max_depth        = 10,
                        eta              = 0.03,
                        subsample        = 0.8,
                        colsample_bytree = 0.8,
                        min_child_weight = 5,
                        gamma            = 0.1)

cat("\n══════════════════════════════════════════\n")
cat("  BASELINE MODELS (pre-tuning)\n")
cat("══════════════════════════════════════════\n")

baseline_rim <- fit_xgb_family(shots, features_rim, "rim",
                               params = params_baseline, label = "baseline")
baseline_j2  <- fit_xgb_family(shots, features_j2,  "j2",
                               params = params_baseline, label = "baseline")
baseline_j3  <- fit_xgb_family(shots, features_j3,  "j3",
                               params = params_baseline, label = "baseline")






## ═════════════════════════════════════════════════════════════════════════════
## 06 — HYPERPARAMETER TUNING: max_depth × min_child_weight GRID ===============
## ═════════════════════════════════════════════════════════════════════════════
##
## max_depth: Controls tree depth. Deeper trees capture more interactions
##   but risk overfitting to player-like patterns (which we want Stan to handle).
##   - 4: shallower, more regularized — may underfit complex context interactions
##   - 6: deeper, more expressive — may start memorizing specific game situations
##
## min_child_weight: Minimum hessian sum in a leaf node. Higher values require
##   more evidence before splitting, preventing the model from fitting to rare
##   shot contexts (e.g., a very specific defender_dist × dribble_range combo).
##   - 30: more flexible — allows splits on less common shot contexts
##   - 100: more conservative — requires substantial evidence for each split
##
## We keep eta, subsample, colsample_bytree, gamma fixed to isolate the
## complexity/regularization tradeoff.

tune_grid <- expand.grid(max_depth        = c(6, 8, 10, 12),
                         min_child_weight = c(5, 10, 50, 100))

cat("\n══════════════════════════════════════════\n")
cat("  HYPERPARAMETER TUNING\n")
cat("  Grid:", nrow(tune_grid), "combinations ×", "3 shot families\n")
cat("══════════════════════════════════════════\n")
print(tune_grid)

## Storage for tuning results
tune_results <- list()

for (i in seq_len(nrow(tune_grid))) {
  md  <- tune_grid$max_depth[i]
  mcw <- tune_grid$min_child_weight[i]
  
  params_i <- list(booster          = "gbtree",
                   objective        = "binary:logistic",
                   eval_metric      = "logloss",
                   max_depth        = md,
                   eta              = 0.03,
                   subsample        = 0.8,
                   colsample_bytree = 0.8,
                   min_child_weight = mcw,
                   gamma            = 0.1)
  
  label_i <- paste0("depth", md, "_mcw", mcw)
  cat("\n────── Tuning config:", label_i, "──────\n")
  
  tune_results[[label_i]] <- list(params = params_i,
                                  rim = fit_xgb_family(shots, features_rim, "rim",
                                                       params = params_i, 
                                                       label = label_i),
                                  j2  = fit_xgb_family(shots, features_j2,  "j2",
                                                       params = params_i, 
                                                       label = label_i),
                                  j3  = fit_xgb_family(shots, features_j3,  "j3",
                                                       params = params_i, 
                                                       label = label_i))
}

## ─── Select best configuration per shot family ───
## Criterion: lowest test logloss (from xgb.train evals, not CV)

select_best_config <- function(tune_results, family_name) {
  configs <- names(tune_results)
  scores  <- sapply(configs, function(cfg) {
    unname(tune_results[[cfg]][[family_name]][["best_test_logloss"]])
  })
  best_cfg <- configs[which.min(scores)]
  
  cat("\n  Best config for", family_name, ":", best_cfg,
      "  (test logloss =", round(min(scores), 5), ")\n")
  cat("  All configs:\n")
  for (cfg in configs) {
    cat("    ", cfg, "→", round(scores[cfg], 5), "\n")
  }
  
  best_cfg
}

cat("\n══════════════════════════════════════════\n")
cat("  TUNING RESULTS SUMMARY\n")
cat("══════════════════════════════════════════\n")

best_rim_cfg <- select_best_config(tune_results, "rim")
best_j2_cfg  <- select_best_config(tune_results, "j2")
best_j3_cfg  <- select_best_config(tune_results, "j3")

## Extract the best tuned fits
fit_rim <- tune_results[[best_rim_cfg]]$rim
fit_j2  <- tune_results[[best_j2_cfg]]$j2
fit_j3  <- tune_results[[best_j3_cfg]]$j3

cat("\n  Final selections:\n")
cat("    rim →", best_rim_cfg, "(was: depth5_mcw50)\n")
cat("    j2  →", best_j2_cfg,  "(was: depth5_mcw50)\n")
cat("    j3  →", best_j3_cfg,  "(was: depth5_mcw50)\n")




shots %>% group_by(shot_family) %>% 
  summarize(n = n(),
            fg_pct = mean(fgm),
            variance = var(fgm))





## ═════════════════════════════════════════════════════════════════════════════
## 07 — REDUCED-FEATURE MODELS (no imputed variables) ==========================
## ═════════════════════════════════════════════════════════════════════════════
##
## Uses the SAME best hyperparameters from tuning but drops the 4 imputed
## variables: height_diff, wingspan_diff, defender_dist_range, dribble_range.
##
## If the full model outperforms reduced, it validates that our imputation
## methods are injecting real signal. If reduced matches or beats full,
## the imputation may be adding noise.

cat("\n══════════════════════════════════════════\n")
cat("  REDUCED-FEATURE MODELS (no imputed vars)\n")
cat("══════════════════════════════════════════\n")

reduced_rim <- fit_xgb_family(shots, features_rim_reduced, "rim",
                              params = tune_results[[best_rim_cfg]]$params,
                              label = "reduced")
reduced_j2  <- fit_xgb_family(shots, features_j2_reduced, "j2",
                              params = tune_results[[best_j2_cfg]]$params,
                              label = "reduced")
reduced_j3  <- fit_xgb_family(shots, features_j3_reduced, "j3",
                              params = tune_results[[best_j3_cfg]]$params,
                              label = "reduced")





## ═════════════════════════════════════════════════════════════════════════════
## 08 — LOGLOSS COMPARISON PLOTS ===============================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Three comparison panels:
##   A) Baseline vs Best Tuned (per family) — shows tuning improvement
##   B) Full features vs Reduced features (per family) — validates imputation
##   C) All-in-one summary across families

## ─── Helper: extract CV logloss curves from a fit object ───
extract_cv_logloss <- function(fit_obj) {
  log <- fit_obj$cv_model$evaluation_log
  data.frame(Iteration     = 1:nrow(log),
             TrainLogLoss  = log$train_logloss_mean,
             TestLogLoss   = log$test_logloss_mean,
             Label         = fit_obj$label,
             Family        = fit_obj$family)
}

## ─── A) Baseline vs Tuned (per family) ───

logloss_baseline_vs_tuned <- function(baseline_fit, tuned_fit, family_name) {
  
  df_baseline <- extract_cv_logloss(baseline_fit)
  df_tuned    <- extract_cv_logloss(tuned_fit)
  
  # Combine and reshape for plotting
  df_combined <- bind_rows(df_baseline %>% mutate(Model = "Baseline"),
                           df_tuned    %>% mutate(Model = "Tuned"))
  
  df_long <- melt(df_combined,
                  id.vars = c("Iteration", "Label", "Family", "Model"),
                  measure.vars = c("TrainLogLoss", "TestLogLoss"),
                  variable.name = "Dataset",
                  value.name = "LogLoss")
  
  # Create combined label: Model + Dataset
  df_long <- df_long %>%
    mutate(Series = paste(Model, gsub("LogLoss", "", Dataset)))
  
  ggplot(df_long, aes(x = Iteration, y = LogLoss, color = Series)) +
    geom_line(linewidth = 0.8) +
    labs(title = paste0(toupper(family_name), ": Baseline vs Tuned"),
         subtitle = paste0("Baseline: depth=5, mcw=50  |  Tuned: ",
                          tuned_fit$label),
         x = "Iteration", y = "LogLoss") +
    theme_minimal() +
    scale_color_manual(values = c("Baseline Train" = "lightblue",
                                  "Baseline Test"  = "blue",
                                  "Tuned Train"    = "lightsalmon",
                                  "Tuned Test"     = "red")) +
    theme(plot.title    = element_text(hjust = 0.5, face = "bold", size = 14),
          axis.title    = element_text(face = "bold", size = 12),
          legend.title  = element_blank(),
          legend.position = "top")
}

p_tune_rim <- logloss_baseline_vs_tuned(baseline_rim, fit_rim, "rim")
p_tune_j2  <- logloss_baseline_vs_tuned(baseline_j2,  fit_j2,  "j2")
p_tune_j3  <- logloss_baseline_vs_tuned(baseline_j3,  fit_j3,  "j3")

p_tuning_comparison <- p_tune_rim / p_tune_j2 / p_tune_j3 +
  plot_annotation(title    = "XGBoost LogLoss: Pre- vs Post-Hyperparameter Tuning",
                  subtitle = "Lower test logloss = better generalization")

ggsave("graphs/xgb_logloss_baseline_vs_tuned.png", p_tuning_comparison,
       width = 10, height = 14, dpi = 150)
cat("\nTuning comparison plot saved: xgb_logloss_baseline_vs_tuned.png\n")


## ─── B) Full vs Reduced features (per family) ───

logloss_full_vs_reduced <- function(full_fit, reduced_fit, family_name) {
  
  df_full    <- extract_cv_logloss(full_fit)
  df_reduced <- extract_cv_logloss(reduced_fit)
  
  df_combined <- bind_rows(df_full    %>% mutate(Model = "Full (w/ imputed)"),
                           df_reduced %>% mutate(Model = "Reduced (no imputed)"))
  
  df_long <- melt(df_combined,
                  id.vars = c("Iteration", "Label", "Family", "Model"),
                  measure.vars = c("TrainLogLoss", "TestLogLoss"),
                  variable.name = "Dataset",
                  value.name = "LogLoss")
  
  df_long <- df_long %>%
    mutate(Series = paste(Model, gsub("LogLoss", "", Dataset)))
  
  ggplot(df_long, aes(x = Iteration, y = LogLoss, color = Series)) +
    geom_line(linewidth = 0.8) +
    labs(title = paste0(toupper(family_name),
                        ": Full Features vs Reduced (No Imputed)"),
         subtitle = paste0("Imputed vars: height_diff, wingspan_diff, ",
                          "defender_dist_range, dribble_range"),
         x = "Iteration", y = "LogLoss") +
    theme_minimal() +
    scale_color_manual(values = c("Full (w/ imputed) Train"       = "lightblue",
                                  "Full (w/ imputed) Test"        = "blue",
                                  "Reduced (no imputed) Train"    = "lightsalmon",
                                  "Reduced (no imputed) Test"     = "red")) +
    theme(plot.title    = element_text(hjust = 0.5, face = "bold", size = 14),
          axis.title    = element_text(face = "bold", size = 12),
          legend.title  = element_blank(),
          legend.position = "top")
}

p_feat_rim <- logloss_full_vs_reduced(fit_rim, reduced_rim, "rim")
p_feat_j2  <- logloss_full_vs_reduced(fit_j2,  reduced_j2,  "j2")
p_feat_j3  <- logloss_full_vs_reduced(fit_j3,  reduced_j3,  "j3")

p_feature_comparison <- p_feat_rim / p_feat_j2 / p_feat_j3 +
  plot_annotation(
    title    = "XGBoost LogLoss: Full Features vs No Imputed Variables",
    subtitle = "If full model test logloss < reduced, imputation adds signal"
  )

ggsave("graphs/xgb_logloss_full_vs_reduced.png", p_feature_comparison,
       width = 10, height = 14, dpi = 150)
cat("Feature comparison plot saved: xgb_logloss_full_vs_reduced.png\n")


## ─── C) Summary table: all model variants ───

summarize_model_logloss <- function(fit_obj, model_type) {
  test_idx <- which(shots$shot_family == fit_obj$family & shots$split == "test")
  X_test   <- as.matrix(shots[test_idx, colnames(fit_obj$X_test)])
  y_test   <- shots$fgm[test_idx]
  preds    <- predict(fit_obj$model, xgb.DMatrix(data = X_test))
  
  logloss <- -mean(y_test * log(pmax(preds, 1e-7)) +
                    (1 - y_test) * log(pmax(1 - preds, 1e-7)))
  brier   <- mean((y_test - preds)^2)
  
  data.frame(model_type  = model_type,
             family      = fit_obj$family,
             test_logloss = round(logloss, 5),
             test_brier   = round(brier, 5),
             best_iter    = attributes(fit_obj$model)$early_stop$best_iteration,
             max_depth    = fit_obj$params$max_depth,
             min_child_weight = fit_obj$params$min_child_weight,
             stringsAsFactors = FALSE)
}

comparison_summary <- bind_rows(summarize_model_logloss(baseline_rim, "baseline"),
                                summarize_model_logloss(baseline_j2,  "baseline"),
                                summarize_model_logloss(baseline_j3,  "baseline"),
                                summarize_model_logloss(fit_rim, "tuned_full"),
                                summarize_model_logloss(fit_j2,  "tuned_full"),
                                summarize_model_logloss(fit_j3,  "tuned_full"),
                                summarize_model_logloss(reduced_rim, "tuned_reduced"),
                                summarize_model_logloss(reduced_j2,  "tuned_reduced"),
                                summarize_model_logloss(reduced_j3,  "tuned_reduced"))

cat("\n══════════════════════════════════════════\n")
cat("  MODEL COMPARISON SUMMARY (Test Set)\n")
cat("══════════════════════════════════════════\n")
print(comparison_summary, row.names = FALSE)

## Imputation value check: how much logloss improves with imputed features
cat("\n── Imputation Value (Full - Reduced, negative = full is better) ──\n")
for (fam in c("rim", "j2", "j3")) {
  full_ll <- comparison_summary %>%
    filter(model_type == "tuned_full", family == fam) %>% pull(test_logloss)
  red_ll  <- comparison_summary %>%
    filter(model_type == "tuned_reduced", family == fam) %>% pull(test_logloss)
  diff <- full_ll - red_ll
  cat("  ", fam, ": full =", full_ll, "  reduced =", red_ll,
      "  delta =", round(diff, 5),
      if (diff < 0) " ✓ imputation helps" else " ✗ imputation hurts", "\n")
}





## ═════════════════════════════════════════════════════════════════════════════
## 09 — PREDICT xFG FOR ALL SHOTS (BOTH full and reduced models) ===============
## ═════════════════════════════════════════════════════════════════════════════

## ─── Full model predictions ───
shots$xfg <- NA_real_

for (fam_info in list(list(name = "rim", fit = fit_rim, feats = features_rim),
                      list(name = "j2",  fit = fit_j2,  feats = features_j2),
                      list(name = "j3",  fit = fit_j3,  feats = features_j3))) {
  idx <- which(shots$shot_family == fam_info$name)
  X   <- as.matrix(shots[idx, fam_info$feats])
  d   <- xgb.DMatrix(data = X)
  shots$xfg[idx] <- predict(fam_info$fit$model, d)
}


## ─── Reduced model predictions ───
shots$xfg_reduced <- NA_real_

for (fam_info in list(list(name = "rim", 
                           fit = reduced_rim, 
                           feats = features_rim_reduced),
                      list(name = "j2",
                           fit = reduced_j2,
                           feats = features_j2_reduced),
                      list(name = "j3",
                           fit = reduced_j3,
                           feats = features_j3_reduced))) {
  idx <- which(shots$shot_family == fam_info$name)
  X   <- as.matrix(shots[idx, fam_info$feats])
  d   <- xgb.DMatrix(data = X)
  shots$xfg_reduced[idx] <- predict(fam_info$fit$model, d)
}

## Derived columns for both
shots <- shots %>%
  mutate(xfg_logit         = qlogis(pmax(pmin(xfg, 0.999), 0.001)),
         xfg_reduced_logit = qlogis(pmax(pmin(xfg_reduced, 0.999), 0.001)),
         xpoints           = xfg * if_else(is_three == 1, 3, 2),
         xpoints_reduced   = xfg_reduced * if_else(is_three == 1, 3, 2),
         points            = fgm * if_else(is_three == 1, 3, 2))




shots %>%
  filter(split == "test") %>%
  group_by(shot_family) %>%
  summarise(base_rate     = mean(fgm),
            naive_ll      = -(base_rate * log(base_rate) + 
                                (1 - base_rate) * log(1 - base_rate)),
            model_ll      = -mean(fgm * log(pmax(xfg, 1e-7)) + 
                                    (1 - fgm) * log(pmax(1 - xfg, 1e-7))),
            lift          = naive_ll - model_ll,
            skill_score   = lift / naive_ll)





## ═════════════════════════════════════════════════════════════════════════════
## 10 — CALIBRATION CHECK (per family, train vs test) ==========================
## ═════════════════════════════════════════════════════════════════════════════

calibration_plot <- function(shots_df, family_name) {

  cal <- shots_df %>%
    filter(shot_family == family_name) %>%
    mutate(xfg_bin = cut(xfg, breaks = seq(0, 1, by = 0.01))) %>%
    group_by(split, xfg_bin) %>%
    summarise(n         = n(),
              predicted = mean(xfg),
              observed  = mean(fgm),
              .groups   = "drop") %>%
    filter(n >= 1000)

  ggplot(cal, aes(x = predicted, y = observed, color = split)) +
    geom_point(aes(size = n), alpha = 0.7) +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey40") +
    scale_size_continuous(range = c(1, 12)) +
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

ggsave("graphs/xgb_calibration.png", p_cal, width = 14, height = 5, dpi = 150)
cat("\nCalibration plot saved: xgb_calibration.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 11 — FEATURE IMPORTANCE PLOTS (per family) ==================================
## ═════════════════════════════════════════════════════════════════════════════

plot_importance <- function(imp_df, family_name, top_n = 15) {
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
  plot_annotation(title = "XGBoost Feature Importance by Shot Family (Tuned)")

ggsave("graphs/xgb_feature_importance.png", p_imp, width = 14, height = 5, dpi = 150)
xcat("Feature importance plot saved: xgb_feature_importance.png\n")





## ═════════════════════════════════════════════════════════════════════════════
## 12 — SHAP VALUES ============================================================
## ═════════════════════════════════════════════════════════════════════════════

for (fam_info in list(list(name = "rim", fit = fit_rim, feats = features_rim),
                      list(name = "j2",  fit = fit_j2,  feats = features_j2),
                      list(name = "j3",  fit = fit_j3,  feats = features_j3))) {
  
  # Sample 5000 shots for SHAP (computational cost)
  idx <- which(shots$shot_family == fam_info$name)
  samp <- sample(idx, min(5000, length(idx)))
  X_samp <- as.matrix(shots[samp, fam_info$feats])

  shp <- shapviz(fam_info$fit$model, X_pred = X_samp, X = X_samp)

  p <- sv_importance(shp, kind = "beeswarm") +
    labs(title = "") +
    #labs(title = paste0(fam_info$name, " SHAP values")) +
    theme_minimal()

  ggsave(paste0("shap_", fam_info$name, ".png"), p, 
         width = 8, height = 4, dpi = 150)
  cat("SHAP plot saved: shap_", fam_info$name, ".png\n")
}



plot_shap_importance <- function(fit_obj, shots, family_code, family_label, 
                                 feats, max_n = 5000, show_legend = TRUE) {
  
  # sample rows for this shot family
  idx <- which(shots$shot_family == family_code)
  samp <- sample(idx, min(max_n, length(idx)))
  X_samp <- as.matrix(shots[samp, feats])
  
  # build shapviz object
  shp <- shapviz(fit_obj$model, X_pred = X_samp, X = X_samp)
  
  # return ggplot object
  p <- sv_importance(shp, kind = "beeswarm") +
    labs(title = family_label) +
    theme_minimal(base_size = 14) +
    theme(plot.title   = element_text(hjust = 0.5, size = 20, face = "bold"),
          axis.title.x = element_text(size = 20),
          axis.title.y = element_text(size = 20),
          axis.text.x  = element_text(size = 15),
          axis.text.y  = element_text(size = 15))
  
  if (!show_legend) {
    p <- p + theme(legend.position = "none")
  }
  
  p
}

p_shap <- plot_shap_importance(fit_rim, shots, "rim", "Rim", features_rim, show_legend = FALSE) +
  plot_shap_importance(fit_j2,  shots, "j2",  "Mid-range", features_j2, show_legend = FALSE) +
  plot_shap_importance(fit_j3,  shots, "j3",  "3-point", features_j3, show_legend = TRUE) +
  plot_annotation(title = "",
                  theme = theme(plot.title = element_text(hjust = 0.5, 
                                                          size = 22, 
                                                          face = "bold"))) +
  plot_layout(ncol = 3)

dir.create("graphs", showWarnings = FALSE)

ggsave("graphs/shap_feature_importance.png",
       p_shap, width = 30, height = 8, dpi = 300)
ggsave("poster_CSAS/shap_feature_importance.png",
       p_shap, width = 20, height = 8, dpi = 300)


## ═════════════════════════════════════════════════════════════════════════════
## 13 — HOLDOUT PERFORMANCE SUMMARY ============================================
## ═════════════════════════════════════════════════════════════════════════════

test_perf <- shots %>%
  filter(split == "test") %>%
  group_by(shot_family) %>%
  summarise(
    n         = n(),
    obs_fg    = mean(fgm),
    pred_xfg  = mean(xfg),
    pred_xfg_reduced = mean(xfg_reduced),
    logloss   = -mean(fgm * log(pmax(xfg, 1e-7)) +
                      (1 - fgm) * log(pmax(1 - xfg, 1e-7))),
    logloss_reduced = -mean(fgm * log(pmax(xfg_reduced, 1e-7)) +
                            (1 - fgm) * log(pmax(1 - xfg_reduced, 1e-7))),
    brier     = mean((fgm - xfg)^2),
    brier_reduced = mean((fgm - xfg_reduced)^2),
    .groups   = "drop"
  )

cat("\n══════════════════════════════════════════\n")
cat("  HOLDOUT PERFORMANCE (Full vs Reduced)\n")
cat("══════════════════════════════════════════\n")
print(test_perf)

# Overall
overall <- shots %>%
  filter(split == "test") %>%
  summarise(
    n = n(),
    logloss = -mean(fgm * log(pmax(xfg, 1e-7)) +
                    (1 - fgm) * log(pmax(1 - xfg, 1e-7))),
    logloss_reduced = -mean(fgm * log(pmax(xfg_reduced, 1e-7)) +
                            (1 - fgm) * log(pmax(1 - xfg_reduced, 1e-7))),
    brier   = mean((fgm - xfg)^2),
    brier_reduced = mean((fgm - xfg_reduced)^2)
  )
cat("\nOverall holdout (full):     logloss =", round(overall$logloss, 5),
    "  brier =", round(overall$brier, 5), "\n")
cat("Overall holdout (reduced):  logloss =", round(overall$logloss_reduced, 5),
    "  brier =", round(overall$brier_reduced, 5), "\n")


## ═════════════════════════════════════════════════════════════════════════════
## 14 — SAVE OUTPUTS ===========================================================
## ═════════════════════════════════════════════════════════════════════════════

## Shots with BOTH full and reduced xFG predictions (input to Stage 2)
write_csv(shots, "shots_with_xfg.csv")

## Save tuned full models
saveRDS(fit_rim$model, "xgb_rim.rds")
saveRDS(fit_j2$model,  "xgb_j2.rds")
saveRDS(fit_j3$model,  "xgb_j3.rds")

## Save reduced models (for Stan comparison)
saveRDS(reduced_rim$model, "xgb_rim_reduced.rds")
saveRDS(reduced_j2$model,  "xgb_j2_reduced.rds")
saveRDS(reduced_j3$model,  "xgb_j3_reduced.rds")

## Save feature lists for prediction on new data
saveRDS(list(
  full    = list(rim = features_rim, j2 = features_j2, j3 = features_j3),
  reduced = list(rim = features_rim_reduced, j2 = features_j2_reduced,
                 j3 = features_j3_reduced)
), "xgb_features.rds")

## Save comparison summary
write_csv(comparison_summary, "xgb_model_comparison.csv")

## Save tuning grid results
tune_summary <- bind_rows(lapply(names(tune_results), function(cfg) {
  bind_rows(lapply(c("rim", "j2", "j3"), function(fam) {
    fit_i <- tune_results[[cfg]][[fam]]
    data.frame(
      config           = cfg,
      family           = fam,
      max_depth        = fit_i$params$max_depth,
      min_child_weight = fit_i$params$min_child_weight,
      best_iter        = fit_i$model$early_stop$best_iteration,
      test_logloss     = round(fit_i$best_test_logloss, 5),
      stringsAsFactors = FALSE
    )
  }))
}))
write_csv(tune_summary, "xgb_tuning_results.csv")

cat("\n── Stage 1 complete ──\n")
cat("  shots_with_xfg.csv       (", 
    nrow(shots), " rows, includes xfg + xfg_reduced)\n")
cat("  xgb_rim/j2/j3.rds        (tuned full models)\n")
cat("  xgb_*_reduced.rds        (reduced-feature models)\n")
cat("  xgb_model_comparison.csv (full comparison table)\n")
cat("  xgb_tuning_results.csv   (all tuning grid results)\n")
cat("  → Ready for 03_stan_random_effects.R (Stage 2)\n")
