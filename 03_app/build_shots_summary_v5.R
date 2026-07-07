################################################################################
# build_shots_summary_v5.R
#
# Run this once before launching app_v9.R. It creates:
#   - shots_summary.rds: compact aggregated data needed by the Shiny app
#   - draws_app.rds: app-only posterior draws, stored as plain numeric matrices
#   - lb_cache.rds: precomputed leaderboard/scatterplot summaries plus one media lookup
#
# The app can still fall back to shots.csv / draws_mat.rds, but using these files
# avoids reading the shot-level CSV at runtime, avoids recomputing leaderboard
# posterior summaries when filters change, and serves image assets from www/.
################################################################################

library(dplyr)

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
inv_logit <- function(x) 1 / (1 + exp(-x))

find_first_col <- function(df, candidates) {
  hit <- intersect(candidates, names(df))
  if (length(hit)) hit[1] else NA_character_
}

mode_nonmissing <- function(x) {
  x <- x[!is.na(x) & nzchar(as.character(x))]
  if (!length(x)) return(NA_character_)
  names(which.max(table(x)))
}

TEAM_NAME_TO_ABBR <- c(
  "Atlanta Hawks" = "ATL", "Boston Celtics" = "BOS", "Brooklyn Nets" = "BKN",
  "Charlotte Hornets" = "CHA", "Chicago Bulls" = "CHI", "Cleveland Cavaliers" = "CLE",
  "Dallas Mavericks" = "DAL", "Denver Nuggets" = "DEN", "Detroit Pistons" = "DET",
  "Golden State Warriors" = "GSW", "Houston Rockets" = "HOU", "Indiana Pacers" = "IND",
  "Los Angeles Clippers" = "LAC", "LA Clippers" = "LAC", "Los Angeles Lakers" = "LAL",
  "Memphis Grizzlies" = "MEM", "Miami Heat" = "MIA", "Milwaukee Bucks" = "MIL",
  "Minnesota Timberwolves" = "MIN", "New Orleans Pelicans" = "NOP", "New York Knicks" = "NYK",
  "Oklahoma City Thunder" = "OKC", "Orlando Magic" = "ORL", "Philadelphia 76ers" = "PHI",
  "Phoenix Suns" = "PHX", "Portland Trail Blazers" = "POR", "Sacramento Kings" = "SAC",
  "San Antonio Spurs" = "SAS", "Toronto Raptors" = "TOR", "Utah Jazz" = "UTA",
  "Washington Wizards" = "WAS", "BRK" = "BKN", "BKN" = "BKN", "CHO" = "CHA",
  "PHO" = "PHX", "PHL" = "PHI", "SAN" = "SAS", "GS" = "GSW", "NY" = "NYK"
)

ESPN_TEAM_SLUG <- c(
  ATL="atl", BOS="bos", BKN="bkn", BRK="bkn", CHA="cha", CHI="chi", CLE="cle",
  DAL="dal", DEN="den", DET="det", GSW="gs", GS="gs", HOU="hou", IND="ind",
  LAC="lac", LAL="lal", MEM="mem", MIA="mia", MIL="mil", MIN="min",
  NOP="no", NO="no", NYK="ny", NY="ny", OKC="okc", ORL="orl", PHI="phi",
  PHX="phx", PHO="phx", POR="por", SAC="sac", SAS="sa", SA="sa",
  TOR="tor", UTA="utah", UTAH="utah", WAS="wsh", WSH="wsh"
)

clean_team_abbr <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  out <- x
  hit <- !is.na(out) & out %in% names(TEAM_NAME_TO_ABBR)
  out[hit] <- unname(TEAM_NAME_TO_ABBR[out[hit]])
  out <- toupper(out)
  hit <- !is.na(out) & out %in% names(TEAM_NAME_TO_ABBR)
  out[hit] <- unname(TEAM_NAME_TO_ABBR[out[hit]])
  out
}

get_team_logo_url <- function(abbr) {
  if (is.null(abbr) || length(abbr) == 0) return(NA_character_)
  abbr <- as.character(abbr[1])
  if (is.na(abbr) || abbr == "") return(NA_character_)
  slug <- ESPN_TEAM_SLUG[toupper(abbr)]
  if (is.na(slug)) slug <- tolower(abbr)
  sprintf("https://a.espncdn.com/i/teamlogos/nba/500/%s.png", slug)
}

get_headshot_url <- function(player_id) {
  if (is.null(player_id) || length(player_id) == 0) return(NA_character_)
  player_id <- as.character(player_id[1])
  if (is.na(player_id) || player_id == "") return(NA_character_)
  player_id_int <- suppressWarnings(as.integer(as.numeric(player_id)))
  if (is.na(player_id_int)) return(NA_character_)
  paste0("https://cdn.nba.com/headshots/nba/latest/260x190/", player_id_int, ".png")
}

# Image URLs are cheap as strings, but remote image downloads in the browser are
# expensive when the scatterplot contains hundreds of players. Cache image files
# under www/ once, then the app serves local static assets.
download_image_assets <- TRUE
# Existing v3 caches may contain 1040x760 headshots. Rebuild the cache so the
# app serves smaller 260x190 files that are cheaper for the browser to decode.
refresh_image_cache <- TRUE
image_cache_root <- file.path("www", "epaa_img_cache")
if (isTRUE(download_image_assets) && isTRUE(refresh_image_cache) && dir.exists(image_cache_root)) {
  unlink(image_cache_root, recursive = TRUE, force = TRUE)
}

shiny_static_path <- function(path) {
  gsub("\\\\", "/", sub("^www[\\\\/]", "", path))
}

safe_download_image <- function(url, dest) {
  if (is.na(url) || !nzchar(url)) return(FALSE)
  if (file.exists(dest) && file.info(dest)$size > 0) return(TRUE)
  dir.create(dirname(dest), recursive = TRUE, showWarnings = FALSE)
  ok <- tryCatch({
    utils::download.file(url, dest, mode = "wb", quiet = TRUE)
    file.exists(dest) && file.info(dest)$size > 0
  }, error = function(e) FALSE, warning = function(w) FALSE)
  if (!ok && file.exists(dest)) unlink(dest)
  ok
}

# Optional but useful: shrink local images after download. The browser only needs
# small markers, so keeping 500px ESPN logos or large NBA headshots makes page
# rendering slower. If magick is unavailable, the script still works and simply
# uses the downloaded source dimensions.
resize_cached_image <- function(path, geometry) {
  if (!file.exists(path) || !requireNamespace("magick", quietly = TRUE)) return(invisible(FALSE))
  ok <- tryCatch({
    img <- magick::image_read(path)
    img <- magick::image_resize(img, geometry)
    magick::image_write(img, path)
    TRUE
  }, error = function(e) FALSE, warning = function(w) FALSE)
  invisible(ok)
}

localize_media_url <- function(url, subdir, key, ext = "png") {
  if (is.na(url) || !nzchar(url)) return("")
  safe_key <- gsub("[^A-Za-z0-9_-]", "_", as.character(key))
  dest <- file.path(image_cache_root, subdir, paste0(safe_key, ".", ext))
  if (isTRUE(download_image_assets) && safe_download_image(url, dest)) {
    if (identical(subdir, "headshots")) {
      resize_cached_image(dest, "96x70>")
    } else if (identical(subdir, "team_logos")) {
      resize_cached_image(dest, "70x70>")
    }
    return(shiny_static_path(dest))
  }
  url
}

message("Reading app inputs...")
shots <- read.csv("raw_data/shots.csv")
player_map <- read.csv("player_map.csv")
defender_map <- read.csv("defender_map.csv")
defteam_map <- read.csv("defteam_map.csv")
draws_mat <- readRDS("raw_data/draws_mat.rds")

message("Extracting app-only posterior draws as plain matrices...")
cal_cols <- c(paste0("cal_intercept[", 1:3, "]"), paste0("cal_slope[", 1:3, "]"))
cal_draws <- as.matrix(draws_mat[, cal_cols, drop = FALSE])

J_player <- max(as.integer(gsub(".*\\[(\\d+),.*", "\\1",
                                grep("^a_player\\[", colnames(draws_mat), value = TRUE))))
J_defender <- max(as.integer(gsub(".*\\[(\\d+),.*", "\\1",
                                  grep("^a_defender\\[", colnames(draws_mat), value = TRUE))))
J_defteam <- max(as.integer(gsub(".*\\[(\\d+),.*", "\\1",
                                 grep("^a_defteam\\[", colnames(draws_mat), value = TRUE))))

extract_re <- function(prefix, J) {
  lapply(1:3, function(t) {
    as.matrix(draws_mat[, paste0(prefix, "[", 1:J, ",", t, "]"), drop = FALSE])
  })
}

n_draws <- nrow(cal_draws)
player_draws <- extract_re("a_player", J_player)
defender_draws <- extract_re("a_defender", J_defender)
defteam_draws <- extract_re("a_defteam", J_defteam)

draws_app <- list(
  n_draws = n_draws,
  cal_draws = cal_draws,
  player_draws = player_draws,
  defender_draws = defender_draws,
  defteam_draws = defteam_draws,
  metadata = list(
    created_at = as.character(Sys.time()),
    source = "draws_mat.rds",
    reduction = "calibration draws plus player/defender/team random-effect draws only; stored as plain matrices; saved uncompressed for faster app startup"
  )
)
saveRDS(draws_app, "draws_app.rds", compress = FALSE)

message("Building compact shot-summary object...")
cal_hat_intercept <- colMeans(cal_draws[, 1:3, drop = FALSE])
cal_hat_slope <- colMeans(cal_draws[, 4:6, drop = FALSE])

required_cols <- c(
  "player_idx", "defender_idx", "defteam_idx", "shot_type", "xfg_logit",
  "xpoints", "event_team", "offense_team", "team", "player_team"
)
keep_cols <- intersect(required_cols, names(shots))
shots_min <- shots[, keep_cols, drop = FALSE]

for (nm in c("shot_type", "xfg_logit", "xpoints")) {
  if (!nm %in% names(shots_min)) stop(sprintf("shots.csv must contain %s.", nm))
}

shots_min <- shots_min %>%
  mutate(
    shot_type = as.integer(.data$shot_type),
    point_value = if_else(.data$shot_type == 3L, 3, 2),
    eta_context = cal_hat_intercept[.data$shot_type] +
      cal_hat_slope[.data$shot_type] * .data$xfg_logit
  )

agg_actor_family <- function(df, id_col) {
  if (!id_col %in% names(df)) {
    return(data.frame(idx = integer(0), shot_type = integer(0), n_obs = integer(0),
                      mean_eta = numeric(0), pt_val = numeric(0)))
  }
  df <- df[!is.na(df[[id_col]]) & !is.na(df$shot_type),
           c(id_col, "shot_type", "eta_context", "point_value")]
  if (!nrow(df)) {
    return(data.frame(idx = integer(0), shot_type = integer(0), n_obs = integer(0),
                      mean_eta = numeric(0), pt_val = numeric(0)))
  }
  g <- interaction(df[[id_col]], df$shot_type, drop = TRUE)
  data.frame(
    idx = as.integer(tapply(df[[id_col]], g, function(x) x[1])),
    shot_type = as.integer(tapply(df$shot_type, g, function(x) x[1])),
    n_obs = as.integer(tapply(df$shot_type, g, length)),
    mean_eta = as.numeric(tapply(df$eta_context, g, mean)),
    pt_val = as.numeric(tapply(df$point_value, g, function(x) x[1])),
    row.names = NULL
  )
}

lb_agg <- list(
  offense = agg_actor_family(shots_min, "player_idx"),
  defense = agg_actor_family(shots_min, "defender_idx"),
  team = agg_actor_family(shots_min, "defteam_idx")
)

def_team_link <- if (all(c("defender_idx", "defteam_idx") %in% names(shots_min))) {
  x <- shots_min[!is.na(shots_min$defender_idx) & !is.na(shots_min$defteam_idx),
                 c("defender_idx", "defteam_idx")]
  tapply(x$defteam_idx, x$defender_idx, function(v) as.integer(names(which.max(table(v)))))
} else {
  setNames(integer(0), character(0))
}

team_col <- find_first_col(shots_min, c(
  "player_team", "player_team_abbr", "player_team_abbreviation",
  "event_team", "offense_team", "team", "team_abbr", "team_abbreviation", "team_name"
))
player_team_link <- setNames(rep(NA_character_, nrow(player_map)), as.character(player_map$player_idx))
if (!is.na(team_col) && "player_idx" %in% names(shots_min)) {
  team_df <- shots_min[!is.na(shots_min$player_idx) & !is.na(shots_min[[team_col]]),
                       c("player_idx", team_col)]
  if (nrow(team_df)) {
    team_df[[team_col]] <- clean_team_abbr(team_df[[team_col]])
    player_team_link <- tapply(team_df[[team_col]], team_df$player_idx, mode_nonmissing)
  }
}

xpoints <- shots_min$xpoints[is.finite(shots_min$xpoints)]
q_probs <- seq(0, 1, length.out = 2001)
x_grid <- as.numeric(stats::quantile(xpoints, probs = q_probs, na.rm = TRUE, names = FALSE, type = 8))
x_grid <- sort(unique(x_grid))
xpts_ecdf <- data.frame(
  x = x_grid,
  p = as.numeric(stats::ecdf(xpoints)(x_grid))
)

shots_summary <- list(
  lb_agg = lb_agg,
  def_team_link = def_team_link,
  player_team_link = player_team_link,
  xpts_ecdf = xpts_ecdf,
  metadata = list(
    created_at = as.character(Sys.time()),
    source = "shots.csv",
    rows_in_source = nrow(shots),
    rows_in_lb_agg = vapply(lb_agg, nrow, integer(1)),
    notes = paste(
      "Contains only aggregated actor/shot-family rows, defender-team links,",
      "player-team links, and an xPts percentile lookup. No shot-level rows are stored."
    )
  )
)
saveRDS(shots_summary, "shots_summary.rds", compress = FALSE)

message("Building precomputed leaderboard/scatterplot cache...")
player_choices <- setNames(player_map$player_idx, player_map$player_name_full)
player_team_logo_lookup <- setNames(vapply(player_team_link, get_team_logo_url, character(1)),
                                    names(player_team_link))
defender_team_logo_lookup <- setNames(vapply(def_team_link, function(idx) {
  abbr <- defteam_map$defender_team[defteam_map$defteam_idx == as.integer(idx)]
  if (!length(abbr)) return(NA_character_)
  get_team_logo_url(abbr[1])
}, character(1)), names(def_team_link))
defender_team_abbr_lookup <- setNames(vapply(def_team_link, function(idx) {
  abbr <- defteam_map$defender_team[defteam_map$defteam_idx == as.integer(idx)]
  if (!length(abbr)) return(NA_character_)
  as.character(abbr[1])
}, character(1)), names(def_team_link))

message("Building media lookup and local image cache...")
team_logo_by_abbr <- function(abbr) {
  remote <- get_team_logo_url(abbr)
  localize_media_url(remote, "team_logos", clean_team_abbr(abbr))
}

player_team_abbr_vec <- unname(player_team_link[as.character(player_map$player_idx)])
defender_team_abbr_vec <- unname(defender_team_abbr_lookup[as.character(defender_map$defender_idx)])
team_abbr_vec <- as.character(defteam_map$defender_team)

media_lookup <- list(
  offense = data.frame(
    idx = as.integer(player_map$player_idx),
    img_url = vapply(player_map$player1_id, function(id) {
      localize_media_url(get_headshot_url(id), "headshots", id)
    }, character(1)),
    team_abbr = ifelse(is.na(player_team_abbr_vec), "", player_team_abbr_vec),
    team_logo_url = vapply(player_team_abbr_vec, team_logo_by_abbr, character(1)),
    stringsAsFactors = FALSE
  ),
  defense = data.frame(
    idx = as.integer(defender_map$defender_idx),
    img_url = vapply(defender_map$defender_id, function(id) {
      localize_media_url(get_headshot_url(id), "headshots", id)
    }, character(1)),
    team_abbr = ifelse(is.na(defender_team_abbr_vec), "", defender_team_abbr_vec),
    team_logo_url = vapply(defender_team_abbr_vec, team_logo_by_abbr, character(1)),
    stringsAsFactors = FALSE
  ),
  team = data.frame(
    idx = as.integer(defteam_map$defteam_idx),
    img_url = vapply(team_abbr_vec, team_logo_by_abbr, character(1)),
    team_abbr = ifelse(is.na(team_abbr_vec), "", team_abbr_vec),
    team_logo_url = "",
    stringsAsFactors = FALSE
  )
)

lb_meta <- list(
  offense = list(draws = player_draws, sign = +1,
                 name_map = setNames(player_map$player_name_full, player_map$player_idx),
                 id_map = setNames(as.character(player_map$player1_id), player_map$player_idx),
                 kind = "player"),
  defense = list(draws = defender_draws, sign = -1,
                 name_map = setNames(defender_map$defender_name, defender_map$defender_idx),
                 id_map = setNames(as.character(defender_map$defender_id), defender_map$defender_idx),
                 kind = "player"),
  team = list(draws = defteam_draws, sign = -1,
              name_map = setNames(defteam_map$defender_team, defteam_map$defteam_idx),
              id_map = setNames(defteam_map$defender_team, defteam_map$defteam_idx),
              kind = "team")
)

epaa_per100_draws <- function(rows, draws_by_family, sign) {
  out <- matrix(NA_real_, nrow = n_draws, ncol = nrow(rows))
  for (t in 1:3) {
    sel <- which(rows$shot_type == t)
    if (!length(sel)) next
    a_draws <- draws_by_family[[t]][, rows$idx[sel], drop = FALSE]
    me <- rows$mean_eta[sel]
    pv <- rows$pt_val[sel]
    base_p <- inv_logit(me)
    skilled_p <- inv_logit(sweep(a_draws, 2, me, "+"))
    delta <- sweep(skilled_p, 2, base_p, "-") * sign
    out[, sel] <- sweep(delta, 2, pv * 100, "*")
  }
  out
}

summarise_draws_for_cache <- function(draw_mat, prefix) {
  stats_df <- data.frame(
    mid = colMeans(draw_mat),
    q10 = apply(draw_mat, 2, quantile, probs = 0.10, names = FALSE),
    q90 = apply(draw_mat, 2, quantile, probs = 0.90, names = FALSE),
    q25 = apply(draw_mat, 2, quantile, probs = 0.25, names = FALSE),
    q75 = apply(draw_mat, 2, quantile, probs = 0.75, names = FALSE),
    q40 = apply(draw_mat, 2, quantile, probs = 0.40, names = FALSE),
    q60 = apply(draw_mat, 2, quantile, probs = 0.60, names = FALSE),
    check.names = FALSE
  )
  names(stats_df) <- paste0(prefix, "_", names(stats_df))
  stats_df
}

build_role_shot_cache <- function(role, shot) {
  meta <- lb_meta[[role]]
  agg <- lb_agg[[role]]
  fam_t <- c(rim = 1L, j2 = 2L, j3 = 3L)
  if (is.null(agg) || !nrow(agg)) return(data.frame())

  if (shot == "all") {
    rows_all <- agg
    per100_by_row <- epaa_per100_draws(rows_all, meta$draws, meta$sign)
    total_by_row <- sweep(per100_by_row, 2, rows_all$n_obs / 100, "*")
    idx_vec <- sort(unique(rows_all$idx))
    total_draws <- vapply(idx_vec, function(i) {
      cols <- which(rows_all$idx == i)
      rowSums(total_by_row[, cols, drop = FALSE])
    }, numeric(n_draws))
    if (is.null(dim(total_draws))) total_draws <- matrix(total_draws, nrow = n_draws)
    nobs_vec <- vapply(idx_vec, function(i) sum(rows_all$n_obs[rows_all$idx == i]), integer(1))
    per100_draws <- sweep(total_draws, 2, 100 / pmax(nobs_vec, 1), "*")
    shot_quality <- vapply(idx_vec, function(i) {
      rows_i <- rows_all[rows_all$idx == i, , drop = FALSE]
      stats::weighted.mean(inv_logit(rows_i$mean_eta) * rows_i$pt_val, rows_i$n_obs)
    }, numeric(1))
  } else {
    t <- fam_t[[shot]]
    rows <- agg[agg$shot_type == t, , drop = FALSE]
    if (!nrow(rows)) return(data.frame())
    idx_vec <- rows$idx
    nobs_vec <- rows$n_obs
    per100_draws <- epaa_per100_draws(rows, meta$draws, meta$sign)
    total_draws <- sweep(per100_draws, 2, rows$n_obs / 100, "*")
    shot_quality <- inv_logit(rows$mean_eta) * rows$pt_val
  }

  out <- data.frame(
    idx = idx_vec,
    n_obs = as.integer(nobs_vec),
    shot_quality = as.numeric(shot_quality),
    stringsAsFactors = FALSE
  )
  out <- cbind(out,
               summarise_draws_for_cache(total_draws, "total"),
               summarise_draws_for_cache(per100_draws, "per100"))
  out$label <- unname(meta$name_map[as.character(out$idx)])
  out <- out[!is.na(out$label), , drop = FALSE]

  # Media fields are stored once in lb_cache$media_lookup instead of being
  # repeated in every role/shot table.
  out
}

cache_keys <- expand.grid(role = c("offense", "defense", "team"),
                          shot = c("all", "rim", "j2", "j3"),
                          stringsAsFactors = FALSE)
lb_cache <- setNames(vector("list", nrow(cache_keys)), paste(cache_keys$role, cache_keys$shot, sep = "__"))
for (i in seq_len(nrow(cache_keys))) {
  key <- names(lb_cache)[i]
  message("  ", key)
  lb_cache[[key]] <- build_role_shot_cache(cache_keys$role[i], cache_keys$shot[i])
}
lb_cache$media_lookup <- media_lookup
saveRDS(lb_cache, "lb_cache.rds", compress = FALSE)

message("Done.")
message("Wrote: shots_summary.rds")
message("Wrote: draws_app.rds")
message("Wrote: lb_cache.rds")
message("Wrote/cached resized/local image assets under: ", image_cache_root)
