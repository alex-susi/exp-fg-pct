## ═══════════════════════════════════════════════════════════════════════════════
## 04_web_export.R — data hand-off from the Shiny app to the static site in web/
##
## Loads 03_app/app.R without launching it and writes the files the browser
## reads to web/public/data/. Every table, lookup and image path comes from the
## app's own objects and functions, so the site cannot drift from the app.
##
## Run from the repo root:   Rscript 04_web_export.R
## Then rerun the parity reference (the web tests check the stamp):
##                           Rscript web/scripts/parity_reference.R
##
## Output (web/public/data/):
##   meta.json                 players, defenders, teams, feature levels, model index
##   cal.bin                   calibration draws (float32)
##   draws/<kind>/<idx>.bin    random-effect draws for one shooter/defender/team
##   xgb/<family>.bin          XGBoost trees for one shot family
##   xpts_ecdf.json            expected-points percentile lookup (shot grade)
##   lb/<role>__<shot>.json    leaderboard / scatterplot summary table
## and web/public/epaa_img_cache/ (headshots and team logos copied from 03_app/www).
## ═══════════════════════════════════════════════════════════════════════════════

## jsonlite is called with its prefix and never attached: attaching it would mask
## shiny::validate(), which the render functions in app.R call.
suppressMessages({
  library(shiny)
  library(xgboost)
})

repo_root <- normalizePath(getwd(), winslash = "/")
stopifnot("Run from the repo root" = file.exists(file.path(repo_root, "03_app", "app.R")))
web_public <- file.path(repo_root, "web", "public")
out_dir <- file.path(web_public, "data")

## ── Load app.R without launching it ──────────────────────────────────────────
setwd(file.path(repo_root, "03_app"))
app_env <- new.env()
suppressMessages(sys.source("app.R", envir = app_env))
a <- app_env

pd <- a$load_posterior_draws()
n_draws <- as.integer(pd$n_draws)
families <- c("rim", "j2", "j3")   # shot_type 1, 2, 3

unlink(out_dir, recursive = TRUE)
for (d in c("", "draws/player", "draws/defender", "draws/team", "xgb", "lb")) {
  dir.create(file.path(out_dir, d), recursive = TRUE, showWarnings = FALSE)
}

write_json_file <- function(x, path) {
  jsonlite::write_json(x, file.path(out_dir, path), auto_unbox = TRUE,
                       digits = NA, na = "null", null = "null")
}

write_f32 <- function(values, path) {
  con <- file(file.path(out_dir, path), "wb")
  on.exit(close(con))
  writeBin(as.double(values), con, size = 4, endian = "little")
}

na_to_empty <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  unname(x)
}

## ── Calibration draws: 6 blocks of n_draws (intercept 1-3, slope 1-3) ────────
cal <- unclass(pd$cal_draws)
stopifnot(ncol(cal) == 6, nrow(cal) == n_draws)
write_f32(as.vector(cal), "cal.bin")

## ── Random-effect draws: one file per actor, 3 blocks of n_draws ─────────────
## Blocks stay in posterior draw order so effects can be summed draw by draw.
write_actor_draws <- function(draws_by_family, kind) {
  J <- ncol(draws_by_family[[1]])
  for (j in seq_len(J)) {
    v <- unlist(lapply(1:3, function(t) as.vector(unclass(draws_by_family[[t]])[, j])))
    stopifnot(length(v) == 3 * n_draws)
    write_f32(v, sprintf("draws/%s/%d.bin", kind, j))
  }
  J
}
J_player   <- write_actor_draws(pd$player_draws,   "player")
J_defender <- write_actor_draws(pd$defender_draws, "defender")
J_defteam  <- write_actor_draws(pd$defteam_draws,  "team")

## ── XGBoost models: flatten the trees predict() actually uses ────────────────
## Layout per family: int32 tree_offset, left, right; float32 value; then uint8
## feature, default_left. `value` is the split threshold on internal nodes and
## the leaf weight on leaves (left == -1).
flatten_trees <- function(trees) {
  n_nodes <- vapply(trees, function(t) length(t$left_children), integer(1))
  stopifnot(all(unlist(lapply(trees, function(t) unlist(t$split_type))) == 0))
  list(
    tree_offset  = as.integer(c(0, cumsum(n_nodes))),
    left         = as.integer(unlist(lapply(trees, `[[`, "left_children"))),
    right        = as.integer(unlist(lapply(trees, `[[`, "right_children"))),
    value        = as.numeric(unlist(lapply(trees, `[[`, "split_conditions"))),
    feature      = as.integer(unlist(lapply(trees, `[[`, "split_indices"))),
    default_left = as.integer(unlist(lapply(trees, `[[`, "default_left")))
  )
}

## Reference predictor over the flattened trees. Features are compared as
## float32, like xgboost does.
predict_flat <- function(flat, base_margin, X) {
  to_f32 <- function(v) readBin(writeBin(as.double(v), raw(), size = 4), "double", n = length(v), size = 4)
  X <- matrix(to_f32(X), nrow = nrow(X))
  n_trees <- length(flat$tree_offset) - 1L
  vapply(seq_len(nrow(X)), function(i) {
    margin <- base_margin
    for (t in seq_len(n_trees)) {
      off <- flat$tree_offset[t]
      node <- 0L
      while (flat$left[off + node + 1L] != -1L) {
        k <- off + node + 1L
        x <- X[i, flat$feature[k] + 1L]
        go_left <- if (is.na(x)) flat$default_left[k] == 1L else x < flat$value[k]
        node <- if (go_left) flat$left[k] else flat$right[k]
      }
      margin <- margin + flat$value[off + node + 1L]
    }
    1 / (1 + exp(-margin))
  }, numeric(1))
}

set.seed(7406)
export_model <- function(fam) {
  model <- a$get_xgb_model(fam)
  js <- jsonlite::fromJSON(rawToChar(xgb.save.raw(model, raw_format = "json")), simplifyVector = FALSE)
  learner <- js$learner
  stopifnot(identical(learner$objective$name, "binary:logistic"),
            identical(learner$gradient_booster$name, "gbtree"))
  feature_names <- unlist(learner$feature_names)
  base_score <- as.numeric(gsub("\\[|\\]", "", learner$learner_model_param$base_score))
  base_margin <- qlogis(base_score)
  trees <- learner$gradient_booster$model$trees

  ## predict() may stop at the early-stopping iteration. Find the tree count
  ## that reproduces it rather than assuming one.
  n_test <- 40L
  X <- cbind(
    shot_distance = runif(n_test, 0, 30), shot_angle = runif(n_test, 0, 90),
    player_min_game = runif(n_test, 0, 48), player_min_stint = runif(n_test, 0, 20),
    shot_clock = sample(0:24, n_test, TRUE), defender_dist_level = sample(1:4, n_test, TRUE),
    dribble_level = sample(1:5, n_test, TRUE), is_2ndchance = sample(0:1, n_test, TRUE),
    is_fastbreak = sample(0:1, n_test, TRUE), is_fromturnover = sample(0:1, n_test, TRUE),
    is_dunk = sample(0:1, n_test, TRUE)
  )[, feature_names, drop = FALSE]
  target <- predict(model, xgb.DMatrix(data = X))
  best <- suppressWarnings(as.integer(learner$attributes$best_iteration))
  candidates <- unique(c(length(trees), best + 1L, best))
  candidates <- candidates[!is.na(candidates) & candidates >= 1L & candidates <= length(trees)]
  flat <- NULL
  for (n_use in candidates) {
    cand <- flatten_trees(trees[seq_len(n_use)])
    err <- max(abs(predict_flat(cand, base_margin, X) - target))
    if (err < 1e-6) { flat <- cand; break }
  }
  if (is.null(flat)) stop("Could not reproduce predict() for the ", fam, " model from its trees.")
  cat(sprintf("  xgb %-3s: %d of %d trees, %d nodes, max |diff| vs predict() = %.2e\n",
              fam, n_use, length(trees), length(flat$left), err))

  n_nodes <- length(flat$left)
  con <- file(file.path(out_dir, sprintf("xgb/%s.bin", fam)), "wb")
  writeBin(flat$tree_offset, con, size = 4, endian = "little")
  writeBin(flat$left,        con, size = 4, endian = "little")
  writeBin(flat$right,       con, size = 4, endian = "little")
  writeBin(flat$value,       con, size = 4, endian = "little")
  writeBin(flat$feature,     con, size = 1)
  writeBin(flat$default_left, con, size = 1)
  close(con)

  list(file = sprintf("xgb/%s.bin", fam), n_trees = n_use, n_nodes = n_nodes,
       base_margin = base_margin, feature_names = I(feature_names))
}
cat("Exporting XGBoost models...\n")
models <- setNames(lapply(families, export_model), families)

## ── Shot Explorer lookups, from the app's own maps and helpers ───────────────
player_idx <- as.integer(a$player_map$player_idx)
defender_idx <- as.integer(a$defender_map$defender_idx)
defteam_idx <- as.integer(a$defteam_map$defteam_idx)
stopifnot(identical(player_idx, seq_len(J_player)),
          identical(defender_idx, seq_len(J_defender)),
          identical(defteam_idx, seq_len(J_defteam)))

players <- list(
  name = as.character(a$player_map$player_name_full),
  img  = na_to_empty(vapply(a$player_id_lookup[as.character(player_idx)],
                            a$get_headshot_url, character(1)))
)
defenders <- list(
  name = as.character(a$defender_map$defender_name),
  img  = na_to_empty(vapply(a$defender_id_lookup[as.character(defender_idx)],
                            a$get_headshot_url, character(1))),
  ## Same lookup the server uses to auto-set the defensive team (NA -> null).
  team = unname(as.integer(a$def_team_link[as.character(defender_idx)]))
)
teams <- list(
  abbr = as.character(a$defteam_map$defender_team),
  logo = na_to_empty(vapply(as.character(a$defteam_map$defender_team),
                            a$get_team_logo_url, character(1)))
)

levels_list <- function(x) lapply(seq_along(x), function(i) list(label = names(x)[i], level = unname(x[i])))

## ── Shot grade lookup ────────────────────────────────────────────────────────
stopifnot(!is.null(a$shots_summary$xpts_ecdf))
ecdf_tbl <- a$shots_summary$xpts_ecdf
write_json_file(list(x = ecdf_tbl$x, p = ecdf_tbl$p), "xpts_ecdf.json")

## ── Leaderboard / scatterplot tables: every role x shot the app can show ─────
roles <- c("offense", "defense", "team")
shots <- c("all", "j3", "j2", "rim")
num_cols <- c("shot_quality",
              paste0("total_", c("mid", "q10", "q90", "q25", "q75", "q40", "q60")),
              paste0("per100_", c("mid", "q10", "q90", "q25", "q75", "q40", "q60")))
lb_rows <- list()
for (role in roles) {
  for (shot in shots) {
    df <- a$get_lb_cache(role, shot)
    stopifnot(!is.null(df), nrow(df) > 0, all(num_cols %in% names(df)))
    tbl <- c(
      list(role = role, shot = shot, obs_label = a$lb_meta[[role]]$obs_label,
           idx = as.integer(df$idx), label = as.character(df$label),
           n_obs = as.integer(df$n_obs),
           team_abbr = na_to_empty(df$team_abbr), img_url = na_to_empty(df$img_url),
           team_logo_url = na_to_empty(df$team_logo_url)),
      lapply(setNames(num_cols, num_cols), function(nm) as.numeric(df[[nm]]))
    )
    key <- paste(role, shot, sep = "__")
    write_json_file(tbl, sprintf("lb/%s.json", key))
    lb_rows[[key]] <- nrow(df)
  }
}

## ── Metadata ─────────────────────────────────────────────────────────────────
generated <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3%z")
meta <- list(
  generated = generated,
  n_draws = n_draws,
  families = I(families),
  cal = list(file = "cal.bin",
             blocks = I(c(paste0("intercept_", families), paste0("slope_", families)))),
  draws = list(player = "draws/player", defender = "draws/defender", team = "draws/team"),
  models = models,
  defender_dist_levels = levels_list(a$DEFENDER_DIST_LEVELS),
  dribble_levels = levels_list(a$DRIBBLE_LEVELS),
  players = players,
  defenders = defenders,
  teams = teams,
  lb = list(roles = I(roles), shots = I(shots), rows = lb_rows)
)
write_json_file(meta, "meta.json")

## ── Images: the app serves these from 03_app/www/epaa_img_cache ──────────────
img_src <- file.path("www", "epaa_img_cache")
if (dir.exists(img_src)) {
  dir.create(web_public, recursive = TRUE, showWarnings = FALSE)
  invisible(file.copy(img_src, web_public, recursive = TRUE, overwrite = TRUE))
}

setwd(repo_root)

files <- list.files(out_dir, recursive = TRUE, full.names = TRUE)
cat(sprintf("Wrote %d files (%.1f MB) to web/public/data, stamped %s\n",
            length(files), sum(file.size(files)) / 1e6, generated))
cat(sprintf("  draws: %d shooters, %d defenders, %d teams x 3 families x %d draws\n",
            J_player, J_defender, J_defteam, n_draws))
