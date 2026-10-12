## ═══════════════════════════════════════════════════════════════════════════════
## parity_reference.R — reference results for the web parity tests
##
## Drives the real Shiny server function in 03_app/app.R with shiny::testServer
## and writes what it returns to web/tests/parity_reference.json. The Vitest
## suite recomputes the same cases with the TypeScript port and compares.
##
## Run from the repo root, after every run of 04_web_export.R:
##   Rscript web/scripts/parity_reference.R
## ═══════════════════════════════════════════════════════════════════════════════

## jsonlite is called with its prefix and never attached: attaching it would mask
## shiny::validate(), which the render functions in app.R call.
suppressMessages(library(shiny))

repo_root <- normalizePath(getwd(), winslash = "/")
stopifnot("Run from the repo root" = file.exists(file.path(repo_root, "03_app", "app.R")))
meta_path <- file.path(repo_root, "web", "public", "data", "meta.json")
stopifnot("Run 04_web_export.R first" = file.exists(meta_path))
meta <- jsonlite::fromJSON(meta_path, simplifyVector = FALSE)

setwd(file.path(repo_root, "03_app"))
app_env <- new.env()
suppressMessages(sys.source("app.R", envir = app_env))
a <- app_env

## ── Shot Explorer cases ──────────────────────────────────────────────────────
## Chosen to reach every branch: each shot family, the corner three, a clamped
## click, every actor combination, a defender with no team link, and each toggle.
shot_defaults <- list(
  shooter = "", defender = "",
  shot_clock = 14, dribble_range = "0 Dribbles", defender_dist = "4-6 Feet - Open",
  min_game = 18, min_stint = 5,
  is_fastbreak = FALSE, is_fromturnover = FALSE, is_2ndchance = FALSE, is_dunk = FALSE
)
shot_case <- function(name, x, y, ...) {
  list(name = name, click = list(x = x, y = y), inputs = modifyList(shot_defaults, list(...)))
}

no_team_defender <- which(is.na(a$def_team_link[as.character(a$defender_map$defender_idx)]))
shot_cases <- list(
  shot_case("rim, no actors", 1.5, 2),
  shot_case("rim, dunk", 1.5, 2, is_dunk = TRUE),
  shot_case("rim, shooter + defender", -2, 1.5, shooter = "1", defender = "5", is_dunk = TRUE,
            shot_clock = 20, is_fastbreak = TRUE, defender_dist = "0-2 Feet - Very Tight"),
  shot_case("mid-range, no actors", 8, 12),
  shot_case("mid-range, shooter only", 8, 12, shooter = "9"),
  shot_case("mid-range, defender only", 8, 12, defender = "9"),
  shot_case("mid-range, 2nd chance late clock", -14, 6, shooter = "20", defender = "33",
            is_2ndchance = TRUE, shot_clock = 3, dribble_range = "7+ Dribbles",
            defender_dist = "2-4 Feet - Tight", min_game = 36, min_stint = 11),
  shot_case("mid-range, just inside the corner line", 21, 5),
  shot_case("three, above the break", 3, 25, shooter = "5", defender = "12",
            shot_clock = 19, is_fastbreak = TRUE, is_fromturnover = TRUE,
            dribble_range = "1 Dribble", defender_dist = "6+ Feet - Wide Open"),
  shot_case("three, off turnover only", 3, 25, shooter = "5", defender = "12",
            shot_clock = 19, is_fromturnover = TRUE,
            dribble_range = "1 Dribble", defender_dist = "6+ Feet - Wide Open"),
  shot_case("three, corner", 23.2, 1, shooter = "2", dribble_range = "2 Dribbles"),
  shot_case("three, click outside the court is clamped", 40, 60, defender = "3",
            dribble_range = "3-6 Dribbles", min_game = 4, min_stint = 4)
)
if (length(no_team_defender)) {
  shot_cases[[length(shot_cases) + 1]] <- shot_case(
    "mid-range, defender without a team link", -6, 15,
    defender = as.character(no_team_defender[1]))
}

## ── Leaderboard and scatterplot cases ────────────────────────────────────────
lb_case <- function(role, shot, xaxis, ci, sort, minobs = "0", team = "all") {
  list(lb_role = role, lb_shot = shot, lb_xaxis = xaxis, lb_ci = ci, lb_sort = sort,
       lb_minobs = minobs, lb_team = team)
}
lb_cases <- list(
  lb_case("offense", "all", "total", "80", "epaa_desc"),
  lb_case("offense", "all", "total", "80", "epaa_asc"),
  lb_case("offense", "j3", "per100", "50", "epaa_asc", "200"),
  lb_case("offense", "rim", "total", "80", "epaa_desc", "100", "LAL"),
  lb_case("defense", "rim", "total", "20", "sq_desc", "100", "BOS"),
  lb_case("defense", "all", "per100", "80", "sq_asc", "500"),
  lb_case("defense", "j2", "per100", "50", "epaa_desc", "50"),
  lb_case("team", "all", "total", "80", "epaa_desc", "1000", "BOS"),
  lb_case("team", "j2", "per100", "50", "sq_asc"),
  lb_case("offense", "j2", "total", "80", "epaa_desc", "1000", "UTA")
)

sc_case <- function(role, shot, xaxis, minobs = "200", team = "all") {
  list(sc_role = role, sc_shot = shot, sc_xaxis = xaxis, sc_minobs = minobs, sc_team = team)
}
sc_cases <- list(
  sc_case("offense", "all", "per100"),
  sc_case("offense", "j3", "total", "300"),
  sc_case("defense", "rim", "per100", "100", "DEN"),
  sc_case("defense", "all", "total", "500"),
  sc_case("team", "all", "per100", "1000", "DEN"),
  sc_case("team", "j3", "total")
)

num <- function(x) unname(as.numeric(x))
plot_layout <- function(json) jsonlite::fromJSON(json, simplifyVector = FALSE)$x$layout

shot_results <- list()
lb_results <- list()
sc_results <- list()

testServer(a$server, {
  ## Shot Explorer: xfg_baseline(), defteam_idx() and posterior_results() are
  ## the reactives of the app itself.
  for (case in shot_cases) {
    do.call(session$setInputs, case$inputs)
    session$setInputs(court_click = case$click)
    base <- xfg_baseline()
    res <- posterior_results()
    dt <- defteam_idx()
    expected <- lapply(res, function(v) if (is.logical(v) || is.character(v)) unname(v) else num(v))
    expected$family <- base$family
    expected$shot_type <- base$type
    expected$distance <- shot_loc$distance
    expected$angle <- shot_loc$angle
    expected$defteam_idx <- if (is.null(dt)) NA_integer_ else as.integer(dt)
    shot_results[[length(shot_results) + 1]] <<- c(case, list(expected = expected))
  }

  ## Leaderboard: lb_data() plus the axis range the rendered plot uses.
  for (case in lb_cases) {
    do.call(session$setInputs, case)
    df <- lb_data()
    expected <- if (is.null(df)) {
      list(n = 0L, idx = integer(0), mid = numeric(0), lo = numeric(0), hi = numeric(0))
    } else {
      list(n = nrow(df), idx = as.integer(df$idx), mid = num(df$mid), lo = num(df$lo),
           hi = num(df$hi), obs_label = attr(df, "obs_label"),
           x_range = num(lb_x_range_from_df(df)),
           plot_height = plot_layout(output$lb_plot)$height)
    }
    expected$title <- output$lb_title
    lb_results[[length(lb_results) + 1]] <<- list(inputs = case, expected = expected)
  }

  ## Scatterplot: scatter_data() plus the axis ranges of the rendered plot.
  for (case in sc_cases) {
    do.call(session$setInputs, case)
    df <- scatter_data()
    expected <- if (is.null(df)) {
      list(n = 0L, idx = integer(0))
    } else {
      layout <- plot_layout(output$scatter_plot)
      list(n = nrow(df), idx = as.integer(df$idx), talent = num(df$talent),
           talent_lo = num(df$talent_lo), talent_hi = num(df$talent_hi),
           shot_quality = num(df$shot_quality),
           x_range = num(unlist(layout$xaxis$range)), y_range = num(unlist(layout$yaxis$range)),
           x_title = layout$xaxis$title$text)
    }
    expected$title <- output$sc_title
    sc_results[[length(sc_results) + 1]] <<- list(inputs = case, expected = expected)
  }
})

setwd(repo_root)

## Print once so a reference where a toggle was silently ignored is visible.
cat("Shot Explorer reference (adjusted xFG%, expected points, grade):\n")
for (r in shot_results) {
  cat(sprintf("  %-44s %-3s xFG %5.1f%%  final %5.1f%%  xPts %.3f  grade %3d %s\n",
              r$name, r$expected$family, r$expected$xfg_pct, r$expected$final_median,
              r$expected$xpts_median, as.integer(r$expected$shot_grade), r$expected$letter_grade))
}
cat("Leaderboard reference (rows, first three idx):\n")
for (r in lb_results) {
  cat(sprintf("  %-44s n = %3d  %s\n", r$expected$title, r$expected$n,
              paste(head(r$expected$idx, 3), collapse = ", ")))
}
cat("Scatterplot reference (rows):\n")
for (r in sc_results) cat(sprintf("  %-60s n = %3d\n", r$expected$title, r$expected$n))

out_path <- file.path(repo_root, "web", "tests", "parity_reference.json")
jsonlite::write_json(
  list(export_generated = meta$generated, shot_cases = shot_results,
       lb_cases = lb_results, sc_cases = sc_results),
  out_path, auto_unbox = TRUE, digits = NA, na = "null", null = "null", pretty = FALSE
)
cat("Wrote", out_path, "\n")
