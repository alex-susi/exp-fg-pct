## ═════════════════════════════════════════════════════════════════════════════
## 01_data_pipeline.R
## NBA xFG v3 — Full data pipeline (test-friendly)
##
## Entry points:
##   build_season_pbp(2024)                          → full season
##   build_season_pbp(2024, game_ids = "0022400061") → single game test
##   build_season_pbp(2024, game_ids = schedule$game_id[1:5]) → first 5 games
##   build_multi_season_pbp(c(2023, 2024))            → multi-season
##
## All intermediate results are cached to cache/ so re-runs skip API calls.
##
## v12 changes (get_game_with_lineups):
##   - Fixed lineup substitution ordering bug. The PBP feed sometimes
##     delivers sub_type="in" events BEFORE the corresponding "out" events
##     at the same game clock. The old row-by-row loop would try to insert
##     the incoming player into a full 5-man lineup (no NA slot), silently
##     dropping them. The subsequent "out" then created an unfilled NA.
##   - Fix: before the lineup tracking loop, substitution events within
##     each (period, clock) group are reordered so "out" events are always
##     processed before "in" events. Non-substitution events and the
##     between-group chronological order are preserved.
##
## v11 changes (impute_closest_defender):
##   - Period-level co-occurrence weighting for defender imputation.
##     Before Phase 2 sampling, a lookup table is built from the PBP
##     counting how many possessions each (shooter, defender) pair shared
##     the floor within each period.  Matchup FGA weights are then
##     MULTIPLIED by these period co-occurrence counts, so defenders who
##     were on the floor more with the shooter in the relevant period
##     get proportionally higher sampling weight.
##   - No new API calls required — co-occurrence is derived entirely
##     from the lineup columns (home_p1–away_p5) already in the PBP.
##   - New def_source value: "matchup_fga_period" for shots where
##     period-modulated weights were available and used.
##   - Fallback: if no co-occurrence data exists for a (game, period,
##     shooter) triple, the existing game-level-only tiers are used
##     unchanged.
##
## v9 changes:
##   - Shot clock: ORB after a blocked shot no longer resets to 14s. The ball
##     stays live; the shot clock continues from wherever it was.
##   - Shot clock error thresholds (SC_BLEND_EPS): widened from 0.5s to 2.0s
##     to account for PBP timestamp imprecision. Live-ball events (steals,
##     blocks → DRB) use an even wider 4.0s window (SC_BLEND_EPS_LIVEBALL)
##     since it may take a few seconds for the defensive team to gain
##     actual possession and the shot clock operator to reset.
##   - Tracks poss_start_is_liveball per row for live-ball possession starts.
##   - Defender distance re-sampling (Override B): after forcing contact shots
##     to "0-2 Feet", non-contact shots in the same group are re-sampled
##     from the adjusted marginal to preserve distributional fidelity.
##     Analogous to Override A's dribble re-sampling for descriptor conflicts.
##   - Season-level idx consolidation: player_idx, defender_idx, defteam_idx
##     are reassigned consistently after rbinding all chunk CSVs.  A single
##     player_map, defteam_map, and scaling_params is saved per season.
##     Per-chunk maps are no longer written.
##   - Multi-season support via build_multi_season_pbp() and
##     existing_player_map/existing_defteam_map parameters.  Player/team
##     indices are consistent across seasons; new players get fresh indices
##     appended after the max of the existing map.
##
## v3 changes (section 07 + impute_shot_context):
##   - Tracking now scraped at the game-date level (not season-level aggregates)
##   - Correct dribble_range values: "0 Dribbles" / "1 Dribble" / "2 Dribbles" /
##     "3-6 Dribbles" / "7+ Dribbles"  (v2 used general_range which returned
##     no meaningful dribble data)
##   - Full 5 × 4 joint distribution of dribble_range × def_dist_range pulled
##     per game-date, giving per-player per-game joint frequency tables
##   - New output columns per FGA:
##       imputed_dribble_range  : categorical (one of the 5 dribble bins)
##       imputed_def_dist_range : categorical (one of the 4 distance bins)
##     These replace the binary is_catch_shoot (now derived from dribble_range)
##     and complement the continuous expected_def_dist (still present, sampled
##     uniformly within the imputed bin)
##   - Descriptor-based non-C&S constraint still applied: shots with pullup /
##     driving / fadeaway / step-back descriptors have "0 Dribbles" zeroed out
##     before sampling, tightening agreement with PBP qualifiers
##   - Per-date caching in cache/trk_dates_<season>/ — scrape is fully
##     resumable; re-runs only fetch dates not yet on disk
##   - Runtime estimate: ~N_unique_dates × 20 calls × 0.7s ≈ 36 min for a
##     full 2024-25 regular season (~180 game days), one-time
## ═════════════════════════════════════════════════════════════════════════════



## ═════════════════════════════════════════════════════════════════════════════
## 00 — CONFIGURE   ============================================================
## ═════════════════════════════════════════════════════════════════════════════

library(hoopR)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(readr)
library(httr)
library(jsonlite)
library(parallel)

options(dplyr.summarise.inform = FALSE)


## ─── Tracking constants ──────────────────────────────────────────────────────
## These are the only valid values accepted by the NBA Stats API for the
## dribble_range, close_def_dist_range, and shot_clock_range parameters.
## Order matters: DRIBBLE_RANGES and DEF_DIST_RANGES are the row/column
## dimensions of the 5×4 weight matrices; SHOT_CLOCK_RANGES is ordered
## from most to least time remaining (matches API ordering).

DRIBBLE_RANGES <- c("0 Dribbles",
                    "1 Dribble",
                    "2 Dribbles",
                    "3-6 Dribbles",
                    "7+ Dribbles")

DEF_DIST_RANGES <- c("0-2 Feet - Very Tight",
                     "2-4 Feet - Tight",
                     "4-6 Feet - Open",
                     "6+ Feet - Wide Open")

SHOT_CLOCK_RANGES <- c("24-22",
                       "22-18 Very Early",
                       "18-15 Early",
                       "15-7 Average",
                       "7-4 Late",
                       "4-0 Very Late")

## Shot distance range filter (GeneralRange parameter on the API).
## "" = Overall (no filter, all distances).
## "Less Than 10 ft" = close-range shots only.
## ("Catch and Shoot" and "Pullups" are redundant with dribble_range.)
GENERAL_RANGES <- c("", "Less Than 10 ft")

## Numeric upper bounds of each sc bucket (seconds remaining on shot clock).
## SC_BOUNDARIES[k] is the threshold between SHOT_CLOCK_RANGES[k] and [k+1].
## E.g., SC_BOUNDARIES[1] = 22 → shots with sc > 22 land in "24-22",
##                                shots with 18 < sc ≤ 22 land in "22-18 Very Early".
SC_BOUNDARIES <- c(22, 18, 15, 7, 4)   # length = length(SHOT_CLOCK_RANGES) - 1

## Half-width of the ambiguity window around each boundary (seconds).
## A shot whose shot_clock falls within SC_BLEND_EPS of a boundary gets
## the two adjacent buckets' matrices SUMMED rather than choosing a side.
##
## v9: Widened from 0.5s to 2.0s to account for PBP timestamp imprecision.
## Live-ball events (steals, blocks) introduce additional delay before the
## shot clock starts ticking — the defensive team may take a few seconds to
## gain actual possession — so those get an even wider 4.0s window.
SC_BLEND_EPS          <- 2.0
SC_BLEND_EPS_LIVEBALL <- 4.0

## Maximum period to scrape (1–4 = regulation, 5–7 = OT1/OT2/OT3).
## Period 0 is a synthetic key meaning "all periods summed" and is used
## as a fallback when a specific-period lookup fails.
MAX_PERIOD <- 7L

## Number of parallel workers for the tracking scrape.  Each worker makes
## sequential API calls on its own connection, so N_WORKERS concurrent
## requests hit the NBA Stats API at once.  3–5 is safe; higher risks
## rate-limiting (HTTP 429).  Set to 1 to disable parallelism.
#N_WORKERS <- 4L
#N_WORKERS <- 8L
N_WORKERS <- 10L


## ─── Helpers ────────────────────────────────────────────────────────────────

## safe_call() wraps hoopR API calls with:
##   - Mandatory inter-call delay (rate limiting)
##   - Detection of non-retryable errors (empty-result hoopR crashes)
##   - One retry for genuine transient network/timeout errors
##
## hoopR's nba_leaguedashplayerptshot throws "object 'df_list' not found"
## when the API returns an empty result set for the requested filter combo.
## This is a hoopR bug — NOT transient.  Retrying the identical call produces
## the identical crash.  We detect it and return NULL immediately rather than
## burning the 3s retry delay 120 times per game date.

.NON_RETRYABLE_PATTERNS <- c("df_list",
                             "object .* not found",
                             "subscript out of bounds",
                             "no applicable method")

safe_call <- function(fn, ..., delay = 0.6) {
  Sys.sleep(delay)
  tryCatch(fn(...), error = function(e) {
    msg <- conditionMessage(e)
    if (any(vapply(.NON_RETRYABLE_PATTERNS, grepl, logical(1L),
                   x = msg, ignore.case = TRUE))) {
      # Empty result or hoopR internal parse failure — not transient, skip now
      return(NULL)
    }
    message("  API error: ", msg, " — retrying in 3s…")
    Sys.sleep(3)
    tryCatch(fn(...), error = function(e2) {
      message("  Retry failed: ", conditionMessage(e2))
      return(NULL)
    })
  })
}

# "PT11M43.00S" → seconds ELAPSED in the period (720 - remaining)
clock_to_seconds <- function(clock_str) {
  sapply(clock_str, function(cs) {
    if (is.na(cs) || cs == "") return(NA_real_)
    minutes   <- as.numeric(gsub("PT(\\d+)M.*", "\\1", cs))
    seconds   <- as.numeric(gsub(".*M([0-9.]+)S", "\\1", cs))
    remaining <- minutes * 60 + seconds
    720 - remaining
  }, USE.NAMES = FALSE)
}



standardize_live_pbp <- function(data) {
  if (is.null(data) || !is.data.frame(data) || nrow(data) == 0) return(data)
  
  n <- nrow(data)
  
  # ----- helpers -----
  ensure_col <- function(df, nm, value) {
    if (!nm %in% names(df)) df[[nm]] <- value
    df
  }
  ensure_list_col <- function(df, nm) {
    if (!nm %in% names(df)) df[[nm]] <- rep(list(character(0)), nrow(df))
    # if it exists but isn't list, coerce safely
    if (!is.list(df[[nm]])) df[[nm]] <- as.list(df[[nm]])
    df
  }
  
  # ----- ensure columns that hoopR's downstream mutate touches -----
  # IDs
  data <- ensure_col(data, "assist_person_id",      NA_integer_)
  data <- ensure_col(data, "block_person_id",       NA_integer_)
  data <- ensure_col(data, "steal_person_id",       NA_integer_)
  data <- ensure_col(data, "foul_drawn_person_id",  NA_integer_)
  data <- ensure_col(data, "official_id",           NA_integer_)
  
  # Names (often omitted when the stat never occurs)
  data <- ensure_col(data, "block_player_name",     NA_character_)
  data <- ensure_col(data, "steal_player_name",     NA_character_)
  data <- ensure_col(data, "foul_drawn_player_name",NA_character_)
  data <- ensure_col(data, "assist_player_name_initial", NA_character_)
  
  # list-cols used later in your pipeline
  data <- ensure_list_col(data, "qualifiers")
  data <- ensure_list_col(data, "person_ids_filter")
  
  # ----- coordinate aliases -----
  if ("x_legacy" %in% names(data) && !"xLegacy" %in% names(data)) {
    data$xLegacy <- data$x_legacy
    }
  if ("y_legacy" %in% names(data) && !"yLegacy" %in% names(data)) {
    data$yLegacy <- data$y_legacy
    }
  if ("xLegacy" %in% names(data) && !"x_legacy" %in% names(data)) {
    data$x_legacy <- data$xLegacy
    }
  if ("yLegacy" %in% names(data) && !"y_legacy" %in% names(data)) {
    data$y_legacy <- data$yLegacy
    }
  
  # ----- EXACT rename + mutate block you pasted (made robust via any_of) -----
  data <- data %>%
    dplyr::rename(dplyr::any_of(c(
      "period"          = "period",
      "event_num"       = "action_number",
      "clock"           = "clock",
      "description"     = "description",
      "locX"            = "xLegacy",
      "locY"            = "yLegacy",
      "action_type"     = "action_type",
      "sub_type"        = "sub_type",
      "descriptor"      = "descriptor",
      "shot_result"     = "shot_result",
      "shot_action_number" = "shot_action_number",
      "qualifiers"      = "qualifiers",
      "team_id"         = "team_id",
      "player1_id"      = "person_id",
      "home_score"      = "score_home",
      "away_score"      = "score_away",
      "offense_team_id" = "possession",
      "order"           = "order_number"
    ))) %>%
    dplyr::mutate(
      # make sure these are integers (case_when prefers consistent types)
      assist_person_id     = suppressWarnings(as.integer(.data$assist_person_id)),
      block_person_id      = suppressWarnings(as.integer(.data$block_person_id)),
      steal_person_id      = suppressWarnings(as.integer(.data$steal_person_id)),
      foul_drawn_person_id = suppressWarnings(as.integer(.data$foul_drawn_person_id)),
      
      player2_id = dplyr::case_when(
        !is.na(.data$assist_person_id) ~ .data$assist_person_id,
        TRUE ~ NA_integer_
      ),
      player3_id = dplyr::case_when(
        !is.na(.data$block_person_id) ~ .data$block_person_id,
        !is.na(.data$steal_person_id) ~ .data$steal_person_id,
        !is.na(.data$foul_drawn_person_id) ~ .data$foul_drawn_person_id,
        TRUE ~ NA_integer_
      )
    )
  
  # Keep legacy coords for your engineer_features() 
  if (!"x_legacy" %in% names(data) && "locX" %in% names(data)) {
    data$x_legacy <- data$locX
    }
  if (!"y_legacy" %in% names(data) && "locY" %in% names(data)) {
    data$y_legacy <- data$locY
    }
  
  # wrap in hoopR's classing if available 
  data <- tryCatch(
    hoopR:::make_hoopR_data("NBA Game Play-by-Play Information from NBA.com", 
                            Sys.time())(data),
    error = function(e) data
  )
  
  data <- data %>% 
    select(-locX, -locY)
  
  data
}


## ─── Direct NBA Stats API caller (no hoopR) ────────────────────────────────
##
## .nba_stats_ptshot_direct()
##
## Hits stats.nba.com/stats/leaguedashplayerptshot via httr, bypassing hoopR.
## Modelled on a known-working scraper (get_dean_wade_cns_by_contest.R):
##   - Full URL string as first arg to GET() (not handle + path)
##   - simplifyVector = FALSE for safe JSON parsing
##   - do.call(rbind, rowSet) for row assembly
##   - Matching header set (Origin, no Host)
##
## Returns a data frame with the standard LeagueDashPTShots columns,
## or NULL if the response is empty / errored.

.PTSHOT_URL <- "https://stats.nba.com/stats/leaguedashplayerptshot"
user_agent_str <- paste0("Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
                         "AppleWebKit/537.36 (KHTML, like Gecko) ",
                         "Chrome/120.0.0.0 Safari/537.36")

.PTSHOT_HEADERS <- add_headers(`User-Agent` = user_agent_str,
                               Accept       = "application/json, text/plain, */*",
                               `Accept-Language`    = "en-US,en;q=0.9",
                               Referer              = "https://www.nba.com/stats/",
                               Origin               = "https://www.nba.com",
                               Connection           = "keep-alive",
                               `x-nba-stats-origin` = "stats",
                               `x-nba-stats-token`  = "true")

.nba_stats_ptshot_direct <- function(date_from            = "",
                                     date_to              = "",
                                     season               = "",
                                     season_type          = "Regular Season",
                                     per_mode             = "Totals",
                                     league_id            = "00",
                                     period               = 0,
                                     shot_clock_range     = "",
                                     dribble_range        = "",
                                     close_def_dist_range = "",
                                     general_range        = "",
                                     delay                = 0.1,
                                     max_retries          = 3L) {

  Sys.sleep(delay)

  q <- list(DateFrom          = date_from,
            DateTo            = date_to,
            Season            = season,
            SeasonType        = season_type,
            PerMode           = per_mode,
            LeagueID          = league_id,
            Period            = period,
            ShotClockRange    = shot_clock_range,
            DribbleRange      = dribble_range,
            CloseDefDistRange = close_def_dist_range,
            College = "", Conference = "", Country = "", Division = "",
            DraftPick = "", DraftYear = "", GameScope = "", GameSegment = "",
            GeneralRange = general_range, Height = "", LastNGames = 0,
            Location = "", Month = 0, OpponentTeamID = 0,
            Outcome = "", PORound = 0, PaceAdjust = "N",
            PlayerExperience = "", PlayerPosition = "", PlusMinus = "N",
            Rank = "N", SeasonSegment = "", ShotDistRange = "",
            StarterBench = "", TeamID = 0, TouchTimeRange = "",
            VsConference = "", VsDivision = "", Weight = "")

  last_err <- NULL

  for (attempt in seq_len(max_retries)) {
    resp <- try(GET(.PTSHOT_URL, query = q, .PTSHOT_HEADERS, timeout(30)),
                silent = TRUE)

    if (inherits(resp, "try-error")) {
      last_err <- as.character(resp)
      Sys.sleep(1 * (2^(attempt - 1)))
      next
    }

    sc <- status_code(resp)

    if (sc == 200L) {
      j <- fromJSON(content(resp, as = "text", encoding = "UTF-8"),
                    simplifyVector = FALSE)
      rs0 <- j$resultSets[[1]]

      if (is.null(rs0$rowSet) || length(rs0$rowSet) == 0) return(NULL)

      headers_chr <- unlist(rs0$headers, use.names = FALSE)
      mat         <- do.call(rbind, rs0$rowSet)
      df <- as.data.frame(mat, stringsAsFactors = FALSE)
      colnames(df) <- headers_chr
      return(df)
    }

    if (sc %in% c(403L, 429L, 500L, 502L, 503L, 504L)) {
      Sys.sleep(1 * (2^(attempt - 1)))
      next
    }

    message("  HTTP ", sc, " — skipping")
    return(NULL)
  }

  message("  All ", max_retries, " retries failed. Last: ", last_err)
  NULL
}


## ─── Parallel worker function ──────────────────────────────────────────────
##
## .fetch_combo_batch()
##
## Called by each parallel worker.  Receives a data frame of combos (each row
## = one API call) and processes them sequentially.  Returns a single combined
## data frame.  This function is self-contained — it rebuilds the httr headers
## and helper objects internally so that it works inside a forked/spawned
## worker process without relying on the parent environment.

.fetch_combo_batch <- function(combos_df, date_mdy, season_str) {

  library(httr)
  library(jsonlite)
  library(dplyr)
  
  user_agent_str <- paste0("Mozilla/5.0 (Windows NT 10.0; Win64; x64) ",
                           "AppleWebKit/537.36 (KHTML, like Gecko) ",
                           "Chrome/120.0.0.0 Safari/537.36")

  ptshot_url <- "https://stats.nba.com/stats/leaguedashplayerptshot"
  ptshot_headers <- add_headers(`User-Agent` = user_agent_str,
                                Accept       = "application/json, text/plain, */*",
                                `Accept-Language`    = "en-US,en;q=0.9",
                                Referer              = "https://www.nba.com/stats/",
                                Origin               = "https://www.nba.com",
                                Connection           = "keep-alive",
                                `x-nba-stats-origin` = "stats",
                                `x-nba-stats-token`  = "true")

  results <- vector("list", nrow(combos_df))

  for (i in seq_len(nrow(combos_df))) {
    Sys.sleep(0.1)

    q <- list(DateFrom = date_mdy, DateTo = date_mdy,
              Season = season_str, SeasonType = "Regular Season",
              PerMode = "Totals", LeagueID = "00",
              Period            = combos_df$period[i],
              ShotClockRange    = combos_df$shot_clock_range[i],
              DribbleRange      = combos_df$dribble_range[i],
              CloseDefDistRange = combos_df$close_def_dist_range[i],
              College = "", Conference = "", Country = "", Division = "",
              DraftPick = "", DraftYear = "", GameScope = "", GameSegment = "",
              GeneralRange = combos_df$general_range[i], Height = "", LastNGames = 0,
              Location = "", Month = 0, OpponentTeamID = 0,
              Outcome = "", PORound = 0, PaceAdjust = "N",
              PlayerExperience = "", PlayerPosition = "", PlusMinus = "N",
              Rank = "N", SeasonSegment = "", ShotDistRange = "",
              StarterBench = "", TeamID = 0, TouchTimeRange = "",
              VsConference = "", VsDivision = "", Weight = "")

    ok <- FALSE
    for (attempt in 1:3) {
      resp <- try(GET(ptshot_url, query = q, ptshot_headers, timeout(30)),
                  silent = TRUE)
      if (inherits(resp, "try-error")) {
        Sys.sleep(2^(attempt - 1))
        next
      }
      http_sc <- status_code(resp)
      if (http_sc == 200L) { ok <- TRUE; break }
      if (http_sc %in% c(403L, 429L, 500L, 502L, 503L, 504L)) {
        Sys.sleep(2^(attempt - 1))
        next
      }
      break   # non-retryable HTTP code
    }

    if (!ok) next

    j   <- fromJSON(content(resp, as = "text", encoding = "UTF-8"),
                    simplifyVector = FALSE)
    rs0 <- j$resultSets[[1]]
    if (is.null(rs0$rowSet) || length(rs0$rowSet) == 0) next

    headers_chr <- unlist(rs0$headers, use.names = FALSE)
    mat         <- do.call(rbind, rs0$rowSet)
    df          <- as.data.frame(mat, stringsAsFactors = FALSE)
    colnames(df) <- headers_chr

    df$period            <- combos_df$period[i]
    df$shot_clock_range  <- combos_df$shot_clock_range[i]
    df$dribble_range     <- combos_df$dribble_range[i]
    df$def_dist_range    <- combos_df$close_def_dist_range[i]
    df$general_range     <- combos_df$general_range[i]

    results[[i]] <- df
  }

  bind_rows(results)
}





## ═════════════════════════════════════════════════════════════════════════════
## 01 — SCHEDULE   =============================================================
## ═════════════════════════════════════════════════════════════════════════════

get_schedule <- function(season_year) {
  nba_schedule(season = year_to_season(season_year)) %>%
    as.data.frame() %>%
    filter(season_type_description == "Regular Season") %>%
    select(game_date, game_id,
           home_team_id, home_team_tricode,
           away_team_id, away_team_tricode,
           season, season_type_description,
           game_status_text)
}





## ═════════════════════════════════════════════════════════════════════════════
## 02 — PBP WITH LINEUPS   =====================================================
## ═════════════════════════════════════════════════════════════════════════════

get_game_with_lineups <- function(game_id) {
  message("  Scraping game ", game_id)
  
  pbp <- safe_call(nba_live_pbp, game_id = game_id)
  pbp <- standardize_live_pbp(pbp)
  if (is.null(pbp) || nrow(pbp) == 0) return(NULL)
  
  rot <- safe_call(nba_gamerotation, game_id = game_id)
  if (is.null(rot)) return(NULL)
  
  home_team_id <- as.character(unique(rot$HomeTeam$TEAM_ID)[1])
  away_team_id <- as.character(unique(rot$AwayTeam$TEAM_ID)[1])
  
  # Starting Five's
  home_starters <- as.character(rot$HomeTeam %>%
                                  filter(IN_TIME_REAL == "0") %>%
                                  pull(PERSON_ID))
  away_starters <- as.character(rot$AwayTeam %>%
                                  filter(IN_TIME_REAL == "0") %>%
                                  pull(PERSON_ID))
  
  pad5 <- function(v) c(v, rep(NA_character_, max(0, 5 - length(v))))[1:5]
  cur_home <- pad5(home_starters)
  cur_away <- pad5(away_starters)
  
  # v12: Batch-process subs at each (period, clock) — outs before ins —
  #      without reordering rows.  When the loop hits the first sub at a
  #      given clock, it looks ahead to find ALL subs at that clock,
  #      processes every "out" then every "in", and marks the batch done.
  #      Non-sub events and row order are completely untouched.
  is_sub <- !is.na(pbp$action_type) &
    tolower(pbp$action_type) == "substitution"
  sub_batch_key <- ifelse(is_sub,
                          paste0(pbp$period, "|", pbp$clock),
                          NA_character_)
  
  home_lineups     <- vector("list", nrow(pbp))
  away_lineups     <- vector("list", nrow(pbp))
  processed_clocks <- list()
  
  for (i in seq_len(nrow(pbp))) {
    if (is_sub[i]) {
      batch_key <- sub_batch_key[i]
      if (is.null(processed_clocks[[batch_key]])) {
        batch_idx <- which(sub_batch_key == batch_key)
        for (pass in c("out", "in")) {
          for (bi in batch_idx) {
            sub_dir <- tolower(as.character(pbp$sub_type[bi]))
            if (!grepl(pass, sub_dir)) next
            pid    <- as.character(pbp$player1_id[bi])
            tid    <- as.character(pbp$team_id[bi])
            lineup <- if (tid == home_team_id) cur_home else cur_away
            if (pass == "out") {
              idx <- which(lineup == pid)
              if (length(idx) > 0) lineup[idx[1]] <- NA_character_
            } else {
              na_idx <- which(is.na(lineup))
              if (length(na_idx) > 0) lineup[na_idx[1]] <- pid
            }
            if (tid == home_team_id) cur_home <- lineup else cur_away <- lineup
          }
        }
        processed_clocks[[batch_key]] <- TRUE
      }
    }
    home_lineups[[i]] <- cur_home
    away_lineups[[i]] <- cur_away
  }
  
  
  # Each team's 5-man lineup at every play
  pbp %>%
    mutate(game_id = game_id,
           home_p1 = map_chr(home_lineups, ~ .x[1]),
           home_p2 = map_chr(home_lineups, ~ .x[2]),
           home_p3 = map_chr(home_lineups, ~ .x[3]),
           home_p4 = map_chr(home_lineups, ~ .x[4]),
           home_p5 = map_chr(home_lineups, ~ .x[5]),
           away_p1 = map_chr(away_lineups, ~ .x[1]),
           away_p2 = map_chr(away_lineups, ~ .x[2]),
           away_p3 = map_chr(away_lineups, ~ .x[3]),
           away_p4 = map_chr(away_lineups, ~ .x[4]),
           away_p5 = map_chr(away_lineups, ~ .x[5]),
           qualifiers = map_chr(qualifiers, ~ if (length(.x) > 0) {
             paste(.x, collapse = "_")
           } else NA_character_),
           person_ids_filter = map_chr(person_ids_filter, ~ if (length(.x) > 0){
             paste(.x, collapse = "_")
           } else NA_character_)) %>%
    as.data.frame()
}





## ═════════════════════════════════════════════════════════════════════════════
## 03 — FEATURE ENGINEERING   ==================================================
## ═════════════════════════════════════════════════════════════════════════════

engineer_features <- function(pbp) {
  pbp %>%
    select(-any_of(c("time_actual", "edited", "is_target_score_last_period"))) %>%
    mutate(player1_id      = as.character(player1_id),
           team_id         = as.character(team_id),
           offense_team_id = as.character(offense_team_id),
           
           # shot flags
           fga = as.integer(is_field_goal == 1),
           fgm = as.integer(fga == 1 & shot_result == "Made"),
           fta = as.integer(tolower(action_type) == "freethrow"),
           ftm = as.integer(fta == 1 & shot_result == "Made"),
           
           # geometry — use x_legacy/y_legacy (NBA standard tenths-of-foot coords)
           x_legacy      = as.numeric(x_legacy),
           y_legacy      = as.numeric(y_legacy),
           shot_distance = as.numeric(shot_distance),
           shot_angle    = atan2(abs(x_legacy), pmax(y_legacy, 1e-1)) * (180 / pi),
           
           # shot classification
           is_three = as.integer(str_detect(action_type, "3pt")),
           is_dunk  = as.integer(str_detect(toupper(coalesce(sub_type, "")), "DUNK")),
           is_rim   = as.integer((area == "Restricted Area" | 
                                    sub_type == "DUNK" |
                                    grepl("alley-oop", descriptor) |
                                    grepl("tip", descriptor) |
                                    grepl("putback", descriptor) |
                                    shot_distance < 4) &
                                   fga == 1 & action_type == "2pt"),
           is_jump  = as.integer(is_rim == 0 & fga == 1),
           shot_family = case_when(is_rim == 1                  ~ "rim",
                                   is_jump == 1 & is_three == 0 ~ "j2",
                                   is_jump == 1 & is_three == 1 ~ "j3",
                                   TRUE                         ~ NA_character_),
           shot_type = case_when(shot_family == "rim" ~ 1L,
                                 shot_family == "j2"  ~ 2L,
                                 shot_family == "j3"  ~ 3L),
           # clock
           period          = as.integer(period),
           elapsed_seconds = clock_to_seconds(clock),
           game_clock_sec  = 720 - elapsed_seconds,
           period_start    = (period - 1L) * 720L,
           total_elapsed   = period_start + elapsed_seconds,
           
           # possession context
           is_turnover     = as.integer(action_type == "turnover"),
           is_defrebound   = as.integer(action_type == "rebound" &
                                          sub_type == "defensive"),
           is_shootingfoul = as.integer(action_type == "foul" &
                                          coalesce(descriptor, "") == "shooting"),
           is_2ndchance    = as.integer(str_detect(coalesce(qualifiers, ""), 
                                                   "2ndchance")),
           is_fastbreak    = as.integer(str_detect(coalesce(qualifiers, ""), 
                                                   "fastbreak")),
           is_fromturnover = as.integer(str_detect(coalesce(qualifiers, ""), 
                                                   "fromturnover")),
           
           # free throw parsing
           is_last_freethrow = {
             m <- str_match(sub_type, "^(\\d+)\\s*of\\s*(\\d+)$")
             as.integer(!is.na(m[, 2]) & m[, 2] == m[, 3] & fta == 1)
           },
           ft_of_n = {
             m <- str_match(sub_type, "^(\\d+)\\s*of\\s*(\\d+)$")
             as.integer(m[, 3])
           },
           
           # possession end
           period_end = as.integer(action_type == "period" &
                                     coalesce(sub_type, "") == "end"),
           possession_end = as.integer(fgm == 1 |
                                         (ftm == 1 & is_last_freethrow == 1) |
                                         is_turnover == 1 |
                                         is_defrebound == 1 |
                                         period_end == 1|
                                         # jump ball @ period start => possession ends
                                         (action_type == "jumpball" & 
                                            ((lag(action_type) == "period" & 
                                                lag(sub_type) == "start") |
                                               # ends iff offense_team_id != team_id
                                               !coalesce(
                                                 suppressWarnings(
                                                   as.integer(offense_team_id)) ==
                                                   suppressWarnings(
                                                     as.integer(team_id)),
                                                 FALSE)))),
           possession_id = cumsum(lag(possession_end, default = 0L) == 1L),
           # preserve block/foul IDs for deterministic defender assignment
           block_person_id      = as.character(block_person_id),
           foul_drawn_person_id = as.character(foul_drawn_person_id)) %>%
    group_by(game_id) %>%
    arrange(order, event_num, .by_group = TRUE) %>%
    mutate(time_since_last_event = pmax(coalesce(total_elapsed - lag(total_elapsed), 
                                                 0), 0),
           game_clock_sec = as.numeric(game_clock_sec)) %>%
    arrange(game_id, total_elapsed, order) %>%
    group_by(game_id, possession_id) %>%
    mutate(sec_since_play_start = cumsum(time_since_last_event)) %>%
    ungroup() %>% 
    group_by(game_id) %>%
    
    # Shot Clock
    group_modify(~{
      df <- .x
      n  <- nrow(df)
      
      shot_clock              <- rep(NA_real_, n)
      poss_start_is_liveball  <- rep(FALSE, n)   # v9: for wider sc_to_key blending
      
      # state = shot clock "after" previous event (post-event)
      sc_state <- NA_real_
      
      # v9: track whether the current possession started from a live-ball event
      # (steal or block → DRB).  Used downstream for wider SC_BLEND_EPS.
      cur_poss_liveball <- FALSE
      # Track whether the most recent missed FGA was blocked (for ORB-after-block)
      last_miss_was_blocked <- FALSE
      
      ## ── v13: inbound delay after made FGs ─────────────────────────────────
      ## After a made field goal the game clock keeps running (except in
      ## specific late-period situations), but the shot clock does not start
      ## until the ball is inbounded — typically ~2 seconds later.  We track
      ## an "inbound credit" that gets consumed from the first real dt after
      ## a qualifying made-FG possession end.
      ##
      ## Game clock STOPS after a made FG only in:
      ##   • Final minute of Q1/Q2/Q3  (period ≤ 3 & game_clock ≤ 60)
      ##   • Last two minutes of Q4    (period == 4 & game_clock ≤ 120)
      ##   • Last two minutes of OT    (period ≥ 5 & game_clock ≤ 120)
      ## In those windows, game clock and shot clock resume simultaneously
      ## on the inbound touch, so no correction is needed.
      INBOUND_DELAY <- 2.0
      
      prev_ended_made_fg  <- FALSE
      made_fg_gc          <- NA_real_     # game_clock_sec at the made FG
      made_fg_period      <- NA_integer_  # period of the made FG
      inbound_credit      <- 0            # seconds of dt to absorb
      ## ─────────────────────────────────────────────────────────────────────
      
      for (i in seq_len(n)) {
        dt <- df$time_since_last_event[i]
        if (is.na(dt)) dt <- 0
        
        gc <- df$game_clock_sec[i]
        gc_prev <- if (i > 1) df$game_clock_sec[i - 1] else NA_real_
        prev_poss_end <- (i > 1) && isTRUE(df$possession_end[i - 1] == 1L)
        
        new_poss <- (i == 1) || (df$possession_id[i] != df$possession_id[i - 1])
        
        period_start <- identical(df$action_type[i], "period") && 
          identical(df$sub_type[i], "start")
        
        jump_after_period_start <- identical(df$action_type[i], "jumpball") && 
          identical(df$sub_type[i], "recovered") &&
          i > 1 && identical(df$action_type[i - 1], "period") && 
          identical(df$sub_type[i - 1], "start")
        
        # foul/penalty helpers
        descrip <- df$descriptor[i]
        qual <- coalesce(df$qualifiers[i], "")
        
        off_id  <- suppressWarnings(as.integer(df$offense_team_id[i]))
        team_id <- suppressWarnings(as.integer(df$team_id[i]))
        is_def_foul <-
          identical(df$action_type[i], "foul") && 
          identical(df$sub_type[i], "personal") &&
          !is.na(off_id) && !is.na(team_id) && 
          team_id != off_id
        
        in_penalty_2ft <- str_detect(qual, "inpenalty") && 
          str_detect(qual, "2freethrow")
        
        def3sec <- !is.na(descrip) && descrip == "defensive-3-second"
        
        loose_ball_def_foul <- !is.na(descrip) && 
          descrip == "loose ball" && 
          is_def_foul
        
        nonshoot_def_notpen <-
          is_def_foul &&
          (is.na(descrip) || descrip == "NA") &&
          (is.na(df$qualifiers[i]) || df$qualifiers[i] == "NA") &&
          !in_penalty_2ft
        
        orb <- identical(df$action_type[i], "rebound") && 
          identical(df$sub_type[i], "offensive")
        drb <- identical(df$action_type[i], "rebound") && 
          identical(df$sub_type[i], "defensive")
        
        # v9: ORB after a blocked shot does NOT reset shot clock to 14.
        # The ball stayed live; the offense retained possession with the clock
        # continuing from wherever it was.
        orb_after_block <- orb && last_miss_was_blocked
        
        bump14_if_under <- (orb && !orb_after_block) || 
          def3sec || 
          loose_ball_def_foul || 
          nonshoot_def_notpen
        
        made_last_ft <- isTRUE(df$is_last_freethrow[i] == 1L && df$ftm[i] == 1L)
        
        # v9: detect live-ball possession starts (steal or block→DRB)
        if (new_poss && i > 1) {
          # Check if previous possession ended due to a steal
          prev_at <- df$action_type[i - 1]
          prev_steal_pid <- df$steal_person_id[i - 1]
          prev_was_steal <- identical(prev_at, "turnover") &&
            !is.na(prev_steal_pid) &&
            as.integer(prev_steal_pid) > 0L
          
          # Check if DRB after a blocked shot
          prev_was_block_drb <- drb && last_miss_was_blocked
          
          cur_poss_liveball <- prev_was_steal || prev_was_block_drb
        } else if (new_poss) {
          cur_poss_liveball <- FALSE
        }
        
        # Track whether this FGA was blocked (for ORB-after-block detection)
        if (isTRUE(df$fga[i] == 1L) && isTRUE(df$fgm[i] == 0L)) {
          blk_id <- as.character(df$block_person_id[i])
          last_miss_was_blocked <- !is.na(blk_id) && blk_id != "" && 
            blk_id != "NA" && blk_id != "0"
        } else if (isTRUE(df$fga[i] == 1L) && isTRUE(df$fgm[i] == 1L)) {
          last_miss_was_blocked <- FALSE
        }
        # Don't reset on non-shot events (subs, fouls, etc) so ORB detection works
        
        poss_start_is_liveball[i] <- cur_poss_liveball
        
        ## ── v13: set inbound credit at possession start ─────────────────────
        if (new_poss) {
          inbound_credit <- 0
          if (prev_ended_made_fg) {
            # Was the game clock stopped at the time of the made FG?
            p_fg <- made_fg_period
            g_fg <- made_fg_gc
            clock_was_stopped <- (!is.na(p_fg) && !is.na(g_fg)) &&
              ((p_fg <= 3L && g_fg <= 60.0) ||
                 (p_fg >= 4L && g_fg <= 120.0))
            if (!clock_was_stopped) {
              inbound_credit <- INBOUND_DELAY
            }
          }
        }
        ## ─────────────────────────────────────────────────────────────────────
        
        ## ── v13: compute adjusted dt for shot-clock decrement ───────────────
        ## The inbound credit absorbs the first N seconds of game-clock time
        ## that elapsed while the ball was dead (being inbounded).  This credit
        ## persists across rows with dt=0 (subs, etc.) until real time elapses.
        adj_dt <- dt
        if (inbound_credit > 0 && dt > 0) {
          credit_used    <- min(inbound_credit, dt)
          adj_dt         <- dt - credit_used
          inbound_credit <- inbound_credit - credit_used
        }
        ## ─────────────────────────────────────────────────────────────────────
        
        # 1) Initialize at possession start (BETWEEN rows), THEN decrement by dt
        if (period_start) {
          sc_pre  <- 0
          sc_post <- 0
          
        } else if (jump_after_period_start) {
          # shot clock starts on the tip outcome, not 2 seconds earlier
          sc_pre  <- 24
          sc_post <- 24
          
        } else {
          
          if (new_poss) {
            
            # game clock at the moment the new possession began:
            # prefer previous-row game clock (possession-end event),
            # otherwise fall back to gc + dt (same idea if prev is missing)
            gc_poss_start <- if (prev_poss_end && !is.na(gc_prev)) gc_prev else {
              if (!is.na(gc)) gc + dt else NA_real_
            }
            
            # if the possession ended with <=24s left, shot clock = game clock
            # otherwise full 24
            sc_state <- if (!is.na(gc_poss_start) && 
                            prev_poss_end && 
                            gc_poss_start <= 24.0) {
              gc_poss_start
            } else {
              24
            }
          }
          
          # v13: use adj_dt (inbound-delay-corrected) instead of raw dt
          sc_pre <- if (is.na(sc_state)) NA_real_ else max(sc_state - adj_dt, 0)
          
          # 2) Apply event-based resets AFTER the event
          sc_post <- sc_pre
          
          # Defensive rebound -> new possession, reset to 24 
          # (unless <24 remaining -> off)
          if (drb) {
            sc_post <- if (!is.na(gc) && gc < 24) NA_real_ else 24
          }
          
          # Made final FT -> reset to 24 (unless <24 remaining -> off)
          if (made_last_ft) {
            sc_post <- if (!is.na(gc) && gc < 24) NA_real_ else 24
          }
          
          if (bump14_if_under) {
            if (!is.na(gc) && gc <= 14.0) {
              sc_post <- gc
            } else {
              if (is.na(sc_post)) sc_post <- 14
              else sc_post <- if (sc_post < 14) 14 else sc_post
            }
          }
          
          # game clock vs shot clock: if shot clock > game clock, turn off
          if (!is.na(gc)) {
            if (!is.na(sc_pre)  && sc_pre  > gc) {
              sc_pre  <- gc
            }
            if (!is.na(sc_post) && sc_post > gc) {
              sc_post <- gc
            }
          }
        }
        
        # 3) What to REPORT on this row?
        #    - Shots should show sc_pre (value before attempt)
        #    - Rebounds / foul-bump / jumpball-start should show sc_post
        is_shot <- isTRUE(df$fga[i] == 1L) || 
          identical(df$action_type[i], "2pt") || 
          identical(df$action_type[i], "3pt")
        
        report_post <- period_start || 
          jump_after_period_start || 
          drb || 
          bump14_if_under || 
          made_last_ft
        
        shot_clock[i] <- if (is_shot && !report_post) sc_pre else sc_post
        
        sc_state <- sc_post
        
        ## ── v13: track made-FG possession ends for next iteration ───────────
        if (isTRUE(df$possession_end[i] == 1L)) {
          prev_ended_made_fg <- isTRUE(df$fgm[i] == 1L)
          if (prev_ended_made_fg) {
            made_fg_gc     <- gc
            made_fg_period <- df$period[i]
          }
        }
        ## ─────────────────────────────────────────────────────────────────────
      }
      
      df$shot_clock <- shot_clock
      df$poss_start_is_liveball <- poss_start_is_liveball
      df
    }) %>%
    ungroup() %>%
    { 
      col_order <- c("game_id","event_num","clock","period","period_type",
                     "action_type","sub_type","qualifiers",
                     "player1_id","x","y","offense_team_id",
                     "home_score","away_score","order","x_legacy","y_legacy",
                     "is_field_goal","side","description","person_ids_filter",
                     "team_id","team_tricode","descriptor",
                     "jump_ball_recovered_name","jump_ball_recoverd_person_id",
                     "player_name","player_name_i",
                     "jump_ball_won_player_name","jump_ball_won_person_id",
                     "jump_ball_lost_player_name","jump_ball_lost_person_id",
                     "area","area_detail","shot_distance","shot_result",
                     "shot_action_number", "rebound_total","rebound_defensive_total",
                     "rebound_offensive_total","points_total",
                     "assist_player_name_initial","assist_person_id","assist_total",
                     "turnover_total","steal_player_name","steal_person_id",
                     "official_id","foul_personal_total","foul_technical_total",
                     "foul_drawn_player_name", "foul_drawn_person_id",
                     "block_player_name","block_person_id","player2_id","player3_id",
                     "home_p1","home_p2","home_p3","home_p4","home_p5",
                     "away_p1","away_p2","away_p3","away_p4","away_p5",
                     "fga","fgm","fta","ftm","shot_angle","is_three","is_dunk",
                     "is_rim","is_jump","shot_family","shot_type","elapsed_seconds",
                     "game_clock_sec","period_start","total_elapsed","is_turnover",
                     "is_defrebound","is_shootingfoul","is_2ndchance","is_fastbreak",
                     "is_fromturnover","is_last_freethrow","ft_of_n","period_end",
                     "possession_end","possession_id","time_since_last_event",
                     "sec_since_play_start","shot_clock","poss_start_is_liveball")
      
      dplyr::select(., dplyr::any_of(col_order), dplyr::everything())
    }
}




## ═════════════════════════════════════════════════════════════════════════════
## 04 — FOUL-DRAWING LINKAGE   =================================================
## ═════════════════════════════════════════════════════════════════════════════

link_shooting_fouls <- function(pbp) {
  pbp <- pbp %>% mutate(row_id = row_number())

  foul_rows <- which(pbp$is_shootingfoul == 1)

  foul_trips <- lapply(foul_rows, function(fi) {
    foul_evt <- pbp[fi, ]
    drawn_by <- foul_evt$foul_drawn_person_id
    gid      <- foul_evt$game_id
    if (is.na(drawn_by) || drawn_by %in% c("", "NA")) return(NULL)

    # The player who committed the foul (= closest defender)
    fouling_player <- as.character(foul_evt$player1_id)

    # Gather FTs following this foul
    ft_idx <- c()
    for (j in (fi + 1):min(fi + 8, nrow(pbp))) {
      if (pbp$game_id[j] != gid) break
      if (pbp$fta[j] == 1 && as.character(pbp$player1_id[j]) == drawn_by)
        ft_idx <- c(ft_idx, j)
      at <- pbp$action_type[j]
      if (!(at %in% c("freethrow", "rebound", "foul", "substitution",
                       "timeout", "violation"))) break
    }
    n_ft    <- length(ft_idx)
    ft_made <- if (n_ft > 0) sum(pbp$ftm[ft_idx]) else 0L
    is_and1 <- (n_ft == 1)

    # For and-1: find the preceding made FGA by the fouled player
    preceding_fga_idx <- NA_integer_
    if (is_and1) {
      for (j in (fi - 1):max(fi - 5, 1)) {
        if (pbp$game_id[j] != gid) break
        if (pbp$fga[j] == 1 && as.character(pbp$player1_id[j]) == drawn_by &&
            pbp$fgm[j] == 1) {
          preceding_fga_idx <- j; break
        }
      }
    }

    tibble(foul_row_id = fi, 
           foul_game_id = gid,
           foul_drawn_by = drawn_by,
           fouling_player_id = fouling_player,
           fouling_team_id = as.character(foul_evt$team_id),
           n_ft_awarded = as.integer(n_ft), 
           n_ft_made = as.integer(ft_made),
           is_and1 = is_and1, 
           preceding_fga_idx = preceding_fga_idx)
  }) %>% bind_rows()

  # Tag and-1 FGA rows
  pbp$drew_and1        <- 0L
  pbp$and1_ft_made     <- 0L
  pbp$and1_fouler_id   <- NA_character_

  and1s <- foul_trips %>% filter(is_and1, !is.na(preceding_fga_idx))
  if (nrow(and1s) > 0) {
    pbp$drew_and1[and1s$preceding_fga_idx]      <- 1L
    pbp$and1_ft_made[and1s$preceding_fga_idx]   <- and1s$n_ft_made
    pbp$and1_fouler_id[and1s$preceding_fga_idx] <- and1s$fouling_player_id
  }

  non_fga_fouls <- foul_trips %>% filter(!is_and1)

  # ── Tag missed FGA that drew non-and1 shooting fouls ──
  # These are shots where the shooter was fouled mid-attempt but missed,

  # then went to the line for 2 or 3 FTs. The fouler was in physical contact.
  pbp$had_shooting_foul       <- 0L
  pbp$shooting_foul_fouler_id <- NA_character_

  if (nrow(non_fga_fouls) > 0) {
    for (r in seq_len(nrow(non_fga_fouls))) {
      fi       <- non_fga_fouls$foul_row_id[r]
      gid      <- non_fga_fouls$foul_game_id[r]
      drawn_by <- non_fga_fouls$foul_drawn_by[r]
      fouler   <- non_fga_fouls$fouling_player_id[r]

      # Look backward for the preceding missed FGA by the fouled player
      for (j in (fi - 1):max(fi - 5, 1)) {
        if (pbp$game_id[j] != gid) break
        if (pbp$fga[j] == 1 && as.character(pbp$player1_id[j]) == drawn_by &&
            pbp$fgm[j] == 0) {
          pbp$had_shooting_foul[j]       <- 1L
          pbp$shooting_foul_fouler_id[j] <- fouler
          break
        }
      }
    }
  }

  # ── Unified contact flag ──
  # Any FGA where the defender physically contacted the shooter:
  #   - block_person_id present (blocker was touching the ball/shooter)
  #   - drew_and1 = 1 (fouled on a made shot)
  #   - had_shooting_foul = 1 (fouled on a missed shot)
  valid_block <- !is.na(pbp$block_person_id) &
    pbp$block_person_id != "" &
    pbp$block_person_id != "NA" &
    pbp$block_person_id != "0"
  pbp$is_contact_shot <- as.integer((pbp$fga == 1) &
                                      (valid_block | 
                                         pbp$drew_and1 == 1L | 
                                         pbp$had_shooting_foul == 1L))

  attr(pbp, "non_fga_foul_trips") <- non_fga_fouls

  pbp
}





## ═════════════════════════════════════════════════════════════════════════════
## 04b — FATIGUE FEATURES   ====================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Computes two per-FGA fatigue proxies from lineup/substitution data:
##
##   shooter_min_game   — cumulative minutes the shooter has been on court
##                        in the current game (summed across all stints)
##   shooter_min_stint  — minutes since the shooter's most recent entry
##                        (sub-in, or game start for starters)
##
## Algorithm:
##   For each game, iterate PBP chronologically. Maintain two dictionaries
##   keyed by player_id:
##     on_court_since[pid] — total_elapsed when player entered current stint
##     cumulative[pid]     — total seconds on court in completed stints
##
##   Sub-out: accumulate stint time into cumulative, remove from on_court.
##   Sub-in:  set on_court_since to current time.
##   At each FGA: shooter_min_game = (cumulative + current_stint) / 60.
##
##   Starters are initialised with on_court_since = 0 at game start.
## ═════════════════════════════════════════════════════════════════════════════

compute_fatigue_features <- function(pbp, verbose = TRUE) {

  pbp$shooter_min_game  <- NA_real_
  pbp$shooter_min_stint <- NA_real_

  games <- unique(pbp$game_id)

  for (gid in games) {
    game_mask <- which(pbp$game_id == gid)
    if (length(game_mask) == 0) next
    game_pbp <- pbp[game_mask, ]

    # Track: player_id -> total_elapsed when they came on court
    on_court <- list()
    # Track: player_id -> total seconds on court in completed stints
    cumulative <- list()

    # Initialize starters from first row's lineup columns
    first_row <- game_pbp[1, ]
    starter_ids <- c(first_row$home_p1, first_row$home_p2, first_row$home_p3,
                     first_row$home_p4, first_row$home_p5,
                     first_row$away_p1, first_row$away_p2, first_row$away_p3,
                     first_row$away_p4, first_row$away_p5)
    starter_ids <- unique(starter_ids[!is.na(starter_ids) & starter_ids != ""])

    for (pid in starter_ids) {
      on_court[[pid]]   <- 0
      cumulative[[pid]] <- 0
    }

    for (i in seq_len(nrow(game_pbp))) {
      current_time <- game_pbp$total_elapsed[i]
      if (is.na(current_time)) current_time <- 0

      # Handle substitutions
      act <- game_pbp$action_type[i]
      if (!is.na(act) && tolower(act) == "substitution") {
        pid     <- as.character(game_pbp$player1_id[i])
        sub_dir <- tolower(as.character(game_pbp$sub_type[i]))

        if (grepl("out", sub_dir) && !is.null(on_court[[pid]])) {
          # Player going out: accumulate stint time, remove from on_court
          stint_time <- max(current_time - on_court[[pid]], 0)
          cumulative[[pid]] <- (cumulative[[pid]] %||% 0) + stint_time
          on_court[[pid]] <- NULL

        } else if (grepl("in", sub_dir)) {
          # Player coming in
          on_court[[pid]] <- current_time
          if (is.null(cumulative[[pid]])) cumulative[[pid]] <- 0
        }
      }

      # For FGA rows, compute fatigue features for the shooter
      if (isTRUE(game_pbp$fga[i] == 1)) {
        shooter <- as.character(game_pbp$player1_id[i])

        if (!is.null(on_court[[shooter]])) {
          stint_sec <- max(current_time - on_court[[shooter]], 0)
          total_sec <- (cumulative[[shooter]] %||% 0) + stint_sec

          pbp$shooter_min_game[game_mask[i]]  <- total_sec / 60
          pbp$shooter_min_stint[game_mask[i]] <- stint_sec / 60
        }
        # else: shooter not tracked as on court (lineup data gap) → stays NA
      }
    }
  }

  if (verbose) {
    fga_mask <- pbp$fga == 1 & !is.na(pbp$fga)
    n_tagged <- sum(!is.na(pbp$shooter_min_game[fga_mask]))
    n_fga    <- sum(fga_mask)
    message("  Fatigue coverage: ", n_tagged, "/", n_fga,
            " FGA (", round(100 * n_tagged / max(n_fga, 1), 1), "%)")
    message("  Median game minutes at shot:  ",
            round(median(pbp$shooter_min_game[fga_mask], na.rm = TRUE), 1))
    message("  Median stint minutes at shot: ",
            round(median(pbp$shooter_min_stint[fga_mask], na.rm = TRUE), 1))
  }

  pbp
}



## ═════════════════════════════════════════════════════════════════════════════
## 05 — MATCHUP DATA + DEFENDER IMPUTATION   ===================================
## ═════════════════════════════════════════════════════════════════════════════

pull_game_matchups <- function(game_id) {
  res <- safe_call(nba_boxscorematchupsv3, game_id = game_id)
  if (is.null(res)) return(NULL)
  
  # Extract home and away matchup data
  home_df <- tryCatch(as.data.frame(res$home_team_player_matchups),
                      error = function(e) NULL)
  
  away_df <- tryCatch(as.data.frame(res$away_team_player_matchups),
                      error = function(e) NULL)
  
  # Combine both dataframes
  df <- bind_rows(home_df, away_df)
  
  if (is.null(df) || nrow(df) == 0) return(NULL)
  
  # Add game_id and return
  df %>% mutate(game_id = game_id)
}



## ═════════════════════════════════════════════════════════════════════════════
## build_period_cooccurrence()
##
## Builds a period-level co-occurrence lookup from PBP lineup data.
## For each (game, period, offensive_player), counts how many possessions
## each defensive player shared the floor.  This gives period-specific
## "defensive attention" weights that modulate the game-level matchup FGA
## weights in impute_closest_defender().
##
## No new API calls — uses lineup columns (home_p1–away_p5) already in PBP.
##
## Returns a named list keyed by "game_id|period|off_id", where each value
## is a named numeric vector: defender_id → shared_possession_count.
## ═════════════════════════════════════════════════════════════════════════════

build_period_cooccurrence <- function(pbp, home_lookup) {

  valid_id <- function(x) !is.na(x) & x != "" & x != "NA" & x != "0"

  ## Work only with possession-starting events to avoid overcounting
  ## (each possession counted once, not once per event within it).
  ## If poss_start isn't available, fall back to all FGA rows — still

  ## gives a reasonable co-occurrence signal, just slightly noisier.
  if ("poss_start" %in% names(pbp)) {
    idx <- which(pbp$poss_start == 1)
  } else {
    idx <- which(pbp$fga == 1)
  }

  if (length(idx) == 0) return(list())

  ## Pre-allocate vectors for a flat data.frame (much faster than

  ## building a list-of-data.frames and bind_rows-ing later).
  max_rows  <- length(idx) * 25L       # 5 off × 5 def per row
  rec_gid   <- character(max_rows)
  rec_per   <- integer(max_rows)
  rec_off   <- character(max_rows)
  rec_def   <- character(max_rows)
  n_rec     <- 0L

  for (k in idx) {
    gid      <- pbp$game_id[k]
    per      <- pbp$period[k]
    off_team <- pbp$team_id[k]
    home_tid <- home_lookup[gid]

    if (is.na(off_team) || is.na(home_tid)) next

    if (as.character(off_team) == as.character(home_tid)) {
      off_players <- c(pbp$home_p1[k], pbp$home_p2[k], pbp$home_p3[k],
                        pbp$home_p4[k], pbp$home_p5[k])
      def_players <- c(pbp$away_p1[k], pbp$away_p2[k], pbp$away_p3[k],
                        pbp$away_p4[k], pbp$away_p5[k])
    } else {
      off_players <- c(pbp$away_p1[k], pbp$away_p2[k], pbp$away_p3[k],
                        pbp$away_p4[k], pbp$away_p5[k])
      def_players <- c(pbp$home_p1[k], pbp$home_p2[k], pbp$home_p3[k],
                        pbp$home_p4[k], pbp$home_p5[k])
    }

    off_players <- as.character(off_players[valid_id(off_players)])
    def_players <- as.character(def_players[valid_id(def_players)])

    if (length(off_players) == 0L || length(def_players) == 0L) next

    ## Write out every (off, def) pair for this possession
    n_off <- length(off_players)
    n_def <- length(def_players)
    n_pairs <- n_off * n_def

    ## Grow vectors if needed (doubling strategy)
    while (n_rec + n_pairs > length(rec_gid)) {
      new_len   <- length(rec_gid) * 2L
      rec_gid   <- c(rec_gid,   character(new_len - length(rec_gid)))
      rec_per   <- c(rec_per,   integer(new_len   - length(rec_per)))
      rec_off   <- c(rec_off,   character(new_len - length(rec_off)))
      rec_def   <- c(rec_def,   character(new_len - length(rec_def)))
    }

    for (oi in seq_along(off_players)) {
      for (di in seq_along(def_players)) {
        n_rec <- n_rec + 1L
        rec_gid[n_rec] <- gid
        rec_per[n_rec] <- per
        rec_off[n_rec] <- off_players[oi]
        rec_def[n_rec] <- def_players[di]
      }
    }
  }

  if (n_rec == 0L) return(list())

  ## Trim and aggregate
  co_df <- data.frame(game_id = rec_gid[1:n_rec],
                       period  = rec_per[1:n_rec],
                       off_id  = rec_off[1:n_rec],
                       def_id  = rec_def[1:n_rec],
                       stringsAsFactors = FALSE)

  co_agg <- co_df %>%
    group_by(game_id, period, off_id, def_id) %>%
    summarise(shared_poss = n(), .groups = "drop")

  ## Build fast lookup: "game_id|period|off_id" → named vector (def_id → count)
  lookup <- list()
  for (i in seq_len(nrow(co_agg))) {
    key <- paste0(co_agg$game_id[i], "|", co_agg$period[i], "|",
                  co_agg$off_id[i])
    if (is.null(lookup[[key]])) lookup[[key]] <- numeric(0)
    lookup[[key]][co_agg$def_id[i]] <- co_agg$shared_poss[i]
  }

  lookup
}



## ═════════════════════════════════════════════════════════════════════════════
## impute_closest_defender()
##
## Assigns a defender to each FGA using game-level matchup data from
## nba_boxscorematchupsv3().
##
## IMPORTANT — matchup FGA overcounting:
##   The NBA's matchup data double-credits FGA on switch possessions.
##   If a shooter attacks off a screen and the defense switches, BOTH the
##   original and new defender are credited with 1 FGA for that single shot.
##   As a result, sum(matchup_field_goals_attempted) across defenders for a
##   given shooter typically exceeds the shooter's actual box-score FGA by
##   5–15%. This overcount is higher for players who attack switches
##   frequently (guards with high pick-and-roll usage).
##
##   Consequence: matchup FGA are used as PROPORTIONAL WEIGHTS for sampling,
##   NOT as exact integer budgets. The proportions remain meaningful and are
##   far more informative than possession-time weights (which conflate time
##   guarding with shot-attempt frequency).
##
## Matchup data orientation:
##   person_id                        = OFFENSIVE player (the shooter)
##   matchups_person_id               = DEFENDER
##   matchup_field_goals_attempted    = FGA credited (overcounts due to switches)
##   matchup_three_pointers_attempted = 3PA credited (same caveat)
##   partial_possessions              = fractional possession time (does NOT overcount)
##
## Algorithm:
##
##   PHASE 1 — DETERMINISTIC (from PBP event-level data)
##     Tier A: block_person_id on the FGA row (blocker = closest defender)
##     Tier B: and1_fouler_id on the FGA row (fouler contacted shooter mid-shot)
##
##   PHASE 1.5 — PERIOD CO-OCCURRENCE (v11)
##     Build a lookup from PBP lineup data counting how many possessions
##     each (shooter, defender) pair shared the floor within each period.
##     These counts are used to modulate matchup FGA weights in Phase 2.
##
##   PHASE 2 — PROPORTIONAL SAMPLING
##     For each remaining FGA by shooter S in game G, period P:
##       1. Get on-court opposing defenders
##       2. Look up matchup weights for S in game G:
##          a. Shot-type-specific: 3PA weights for threes, 2PA weights for twos
##          b. Total FGA weights (if no type-specific data for on-court defenders)
##          c. partial_possessions weights (if no FGA data at all)
##          d. Uniform random (no matchup data for any on-court defender)
##       2b. (v11) Modulate weights by period co-occurrence:
##           If co-occurrence data exists for (G, P, S), multiply each
##           defender's matchup weight by their shared-possession count.
##           This up-weights defenders who were on the floor more with S
##           in this specific period.  Falls through to game-level-only
##           weights if co-occurrence lookup is empty.
##       3. Sample one defender proportional to (modulated) weights
##
##   PHASE 3 — SANITY CHECK
##     Compares imputed defender PROPORTIONS to matchup FGA PROPORTIONS.
##     Anchors "actual FGA" to PBP count (ground truth), not matchup sum.
## ════════════════════════════════════════════════════════════════════════════

impute_closest_defender <- function(pbp, matchups, schedule, seed = 42,
                                    verbose = TRUE) {
  
  set.seed(seed)
  
  pbp$imputed_def_id <- NA_character_
  pbp$def_source     <- NA_character_
  
  fga_idx <- which(pbp$fga == 1)
  if (length(fga_idx) == 0) return(pbp)
  
  valid_id <- function(x) !is.na(x) & x != "" & x != "NA" & x != "0"
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # PHASE 1: DETERMINISTIC ASSIGNMENT
  # ═══════════════════════════════════════════════════════════════════════════
  
  # Tier A — blocks: block_person_id on the FGA event
  if ("block_person_id" %in% names(pbp)) {
    block_mask <- fga_idx[valid_id(pbp$block_person_id[fga_idx])]
    if (length(block_mask) > 0) {
      pbp$imputed_def_id[block_mask] <- as.character(pbp$block_person_id[block_mask])
      pbp$def_source[block_mask]     <- "block"
    }
  } else {
    block_mask <- integer(0)
  }
  
  # Tier B — and-1 fouls: fouler contacted shooter during made shot
  if ("and1_fouler_id" %in% names(pbp)) {
    foul_mask <- fga_idx[valid_id(pbp$and1_fouler_id[fga_idx]) &
                           is.na(pbp$imputed_def_id[fga_idx])]
    if (length(foul_mask) > 0) {
      pbp$imputed_def_id[foul_mask] <- as.character(pbp$and1_fouler_id[foul_mask])
      pbp$def_source[foul_mask]     <- "foul"
    }
  } else {
    foul_mask <- integer(0)
  }
  
  if (verbose) {
    message("    Phase 1 (deterministic): block=", length(block_mask),
            "  foul=", length(foul_mask))
  }
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # PREPARE MATCHUP WEIGHTS
  # ═══════════════════════════════════════════════════════════════════════════
  
  remaining_idx <- fga_idx[is.na(pbp$imputed_def_id[fga_idx])]
  
  if (length(remaining_idx) == 0 || is.null(matchups) || nrow(matchups) == 0) {
    if (verbose) message("    No remaining shots or no matchup data.")
    pbp <- run_sanity_check(pbp, matchups, fga_idx, verbose)
    return(pbp)
  }
  
  # Standardize matchup data
  # person_id = offense, matchups_person_id = defense
  mu_raw <- matchups %>% rename_with(tolower)
  mu_raw <- mu_raw[, !duplicated(names(mu_raw))]
  
  mu <- mu_raw %>%
    transmute(GAME_ID      = as.character(game_id),
              OFF_ID       = as.character(person_id),
              DEF_ID       = as.character(matchups_person_id),
              FGA_WEIGHT   = as.numeric(matchup_field_goals_attempted),
              TPA_WEIGHT   = as.numeric(matchup_three_pointers_attempted),
              TWOPA_WEIGHT = pmax(FGA_WEIGHT - TPA_WEIGHT, 0),
              POSS_WEIGHT  = as.numeric(partial_possessions)) %>%
    filter(!is.na(OFF_ID), !is.na(DEF_ID))
  
  # Build lookup structures: key = "GAME_ID|OFF_ID" → named weight vectors
  weight_lookup <- list()
  for (i in seq_len(nrow(mu))) {
    key <- paste0(mu$GAME_ID[i], "|", mu$OFF_ID[i])
    if (is.null(weight_lookup[[key]])) {
      weight_lookup[[key]] <- list(def_ids       = character(0),
                                   fga_weights   = numeric(0),
                                   tpa_weights   = numeric(0),
                                   twopa_weights = numeric(0),
                                   poss_weights  = numeric(0))
    }
    wl <- weight_lookup[[key]]
    wl$def_ids        <- c(wl$def_ids, mu$DEF_ID[i])
    wl$fga_weights    <- c(wl$fga_weights, mu$FGA_WEIGHT[i])
    wl$tpa_weights    <- c(wl$tpa_weights, mu$TPA_WEIGHT[i])
    wl$twopa_weights  <- c(wl$twopa_weights, mu$TWOPA_WEIGHT[i])
    wl$poss_weights   <- c(wl$poss_weights, mu$POSS_WEIGHT[i])
    weight_lookup[[key]] <- wl
  }
  
  # Home team lookup for on-court determination
  home_lookup <- setNames(as.character(schedule$home_team_id), schedule$game_id)


  # ═══════════════════════════════════════════════════════════════════════════
  # PHASE 1.5: PERIOD CO-OCCURRENCE (v11)
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Build a per-(game, period, shooter) lookup of how many possessions each
  # defender shared the floor.  Used to modulate matchup FGA weights in
  # Phase 2 so that defenders who were on the floor more in the *relevant
  # period* get proportionally higher weight.
  #
  # This is derived entirely from PBP lineup columns — no new API calls.

  if (verbose) message("    Building period co-occurrence lookup…")
  cooccur_lookup <- build_period_cooccurrence(pbp, home_lookup)
  if (verbose) message("    Co-occurrence lookup: ",
                        length(cooccur_lookup), " (game, period, shooter) keys")


  # ═══════════════════════════════════════════════════════════════════════════
  # PHASE 2: PROPORTIONAL SAMPLING (with period co-occurrence modulation)
  # ═══════════════════════════════════════════════════════════════════════════
  
  # Helper: get opposing team's on-court players
  get_on_court_def <- function(i) {
    shooter_team <- pbp$team_id[i]
    home_tid     <- home_lookup[pbp$game_id[i]]
    if (!is.na(shooter_team) && !is.na(home_tid) &&
        as.character(shooter_team) == as.character(home_tid)) {
      oc <- c(pbp$away_p1[i], pbp$away_p2[i], pbp$away_p3[i],
              pbp$away_p4[i], pbp$away_p5[i])
    } else {
      oc <- c(pbp$home_p1[i], pbp$home_p2[i], pbp$home_p3[i],
              pbp$home_p4[i], pbp$home_p5[i])
    }
    oc[valid_id(oc)]
  }
  
  # Counters
  n_fga_type_period <- 0L  # shot-type-specific FGA × period co-occurrence (v11)
  n_fga_type  <- 0L   # shot-type-specific FGA weights (game-level only)
  n_fga_total <- 0L   # total FGA weights (cross-type fallback)
  n_poss      <- 0L   # possession-time weights
  n_uniform   <- 0L   # no matchup data
  
  for (i in remaining_idx) {
    oc <- get_on_court_def(i)
    if (length(oc) == 0) next
    
    gid  <- pbp$game_id[i]
    pid  <- pbp$player1_id[i]
    per  <- pbp$period[i]
    is_3 <- (!is.na(pbp$is_three[i]) && pbp$is_three[i] == 1)
    key  <- paste0(gid, "|", pid)
    
    wl <- weight_lookup[[key]]
    
    if (is.null(wl)) {
      pbp$imputed_def_id[i] <- sample(oc, 1)
      pbp$def_source[i]     <- "uniform"
      n_uniform <- n_uniform + 1L
      next
    }
    
    # Which on-court defenders appear in matchup data?
    in_matchup <- match(oc, wl$def_ids)
    has_data   <- !is.na(in_matchup)
    
    if (!any(has_data)) {
      pbp$imputed_def_id[i] <- sample(oc, 1)
      pbp$def_source[i]     <- "uniform"
      n_uniform <- n_uniform + 1L
      next
    }
    
    mu_idx       <- in_matchup[has_data]
    oc_with_data <- oc[has_data]
    
    # Helper for weighted sampling
    wsample <- function(ids, wts) {
      if (length(ids) == 1) return(ids)
      sample(ids, 1, prob = wts / sum(wts))
    }

    # ── v11: Period co-occurrence weights for this shot ──────────────────
    # Look up shared-possession counts for (game, period, shooter).
    # cooccur_wts_oc will be a numeric vector aligned to oc_with_data,
    # with NA for defenders not in the co-occurrence lookup (shouldn't
    # happen if lineups are complete, but defensive fallback to 1).
    cooccur_key <- paste0(gid, "|", per, "|", pid)
    cooccur_raw <- cooccur_lookup[[cooccur_key]]
    has_cooccur <- !is.null(cooccur_raw)

    if (has_cooccur) {
      cooccur_wts_oc <- cooccur_raw[oc_with_data]
      cooccur_wts_oc[is.na(cooccur_wts_oc)] <- 0
    }

    # ── Tier 1: Shot-type-specific FGA weights ──
    type_wts <- if (is_3) wl$tpa_weights[mu_idx] else wl$twopa_weights[mu_idx]

    if (sum(type_wts) > 0) {
      # v11: Try period-modulated weights first
      if (has_cooccur) {
        combined_wts <- type_wts * cooccur_wts_oc
        if (sum(combined_wts) > 0) {
          pbp$imputed_def_id[i] <- wsample(oc_with_data, combined_wts)
          pbp$def_source[i]     <- "matchup_fga_period"
          n_fga_type_period <- n_fga_type_period + 1L
          next
        }
      }
      # Fallback: game-level-only type weights
      pbp$imputed_def_id[i] <- wsample(oc_with_data, type_wts)
      pbp$def_source[i]     <- "matchup_fga"
      n_fga_type <- n_fga_type + 1L
      next
    }
    
    # ── Tier 2: Total FGA weights ──
    fga_wts <- wl$fga_weights[mu_idx]
    
    if (sum(fga_wts) > 0) {
      # v11: Try period-modulated weights first
      if (has_cooccur) {
        combined_wts <- fga_wts * cooccur_wts_oc
        if (sum(combined_wts) > 0) {
          pbp$imputed_def_id[i] <- wsample(oc_with_data, combined_wts)
          pbp$def_source[i]     <- "matchup_fga_period"
          n_fga_type_period <- n_fga_type_period + 1L
          next
        }
      }
      # Fallback: game-level-only FGA weights
      pbp$imputed_def_id[i] <- wsample(oc_with_data, fga_wts)
      pbp$def_source[i]     <- "matchup_fga"
      n_fga_total <- n_fga_total + 1L
      next
    }
    
    # ── Tier 3: Partial possessions weights ──
    poss_wts <- wl$poss_weights[mu_idx]
    
    if (sum(poss_wts) > 0) {
      # v11: Try period-modulated weights first
      if (has_cooccur) {
        combined_wts <- poss_wts * cooccur_wts_oc
        if (sum(combined_wts) > 0) {
          pbp$imputed_def_id[i] <- wsample(oc_with_data, combined_wts)
          pbp$def_source[i]     <- "matchup_poss_period"
          n_fga_type_period <- n_fga_type_period + 1L
          next
        }
      }
      # Fallback: game-level-only possession weights
      pbp$imputed_def_id[i] <- wsample(oc_with_data, poss_wts)
      pbp$def_source[i]     <- "matchup_poss"
      n_poss <- n_poss + 1L
      next
    }
    
    # ── Tier 4: Uniform fallback ──
    pbp$imputed_def_id[i] <- sample(oc, 1)
    pbp$def_source[i]     <- "uniform"
    n_uniform <- n_uniform + 1L
  }
  
  if (verbose) {
    message("    Phase 2 (proportional): ",
            "fga_type_period=", n_fga_type_period,
            "  fga_type=", n_fga_type,
            "  fga_total=", n_fga_total,
            "  poss=", n_poss,
            "  uniform=", n_uniform)
    
    src_all <- table(pbp$def_source[fga_idx], useNA = "ifany")
    coverage <- round(100 * mean(!is.na(pbp$imputed_def_id[fga_idx])), 1)
    message("    Coverage: ", coverage, "%  |  ",
            paste(names(src_all), src_all, sep = "=", collapse = "  "))
  }
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # PHASE 3: SANITY CHECK
  # ═══════════════════════════════════════════════════════════════════════════
  
  pbp <- run_sanity_check(pbp, matchups, fga_idx, verbose)
  
  pbp
}


## ════════════════════════════════════════════════════════════════════════════
## SANITY CHECK
##
## Compares imputed defender DISTRIBUTION to matchup FGA DISTRIBUTION.
##
## Key insight: matchup FGA overcounts actual FGA by 5–15% (switch crediting),
## so we compare PROPORTIONS, not raw counts. "FGA" = PBP ground truth.
## "mFGA" = matchup sum (overcounted, shown for reference).
##
## Reports:
##   - Per-player overcount ratio (matchup sum vs PBP FGA)
##   - Per-player proportion MAE (avg |imputed_share - matchup_share|)
##   - Correlation between imputed and matchup FGA shares
##
## Stored as attribute "defender_sanity_check" for programmatic access.
## ════════════════════════════════════════════════════════════════════════════

run_sanity_check <- function(pbp, matchups, fga_idx, verbose = TRUE) {
  
  if (is.null(matchups) || nrow(matchups) == 0) return(pbp)
  
  # ── Matchup data (overcounted FGA, but proportions are meaningful) ──
  mu_raw <- matchups %>% rename_with(tolower)
  mu_raw <- mu_raw[, !duplicated(names(mu_raw))]
  
  matchup_shares <- mu_raw %>%
    transmute(game_id     = as.character(game_id),
              off_id      = as.character(person_id),
              off_name    = name_i,
              def_id      = as.character(matchups_person_id),
              def_name    = matchups_name_i,
              matchup_fga = as.integer(matchup_field_goals_attempted)) %>%
    group_by(game_id, off_id, off_name) %>%
    mutate(matchup_fga_sum = sum(matchup_fga),
           matchup_share   = if_else(matchup_fga_sum > 0,
                                     matchup_fga / matchup_fga_sum, 0)) %>%
    ungroup()
  
  # ── Imputed FGA per (game, shooter, defender) ──
  imputed <- pbp[fga_idx, ] %>%
    filter(!is.na(imputed_def_id)) %>%
    group_by(game_id, player1_id, imputed_def_id) %>%
    summarise(imputed_fga = n(), .groups = "drop") %>%
    rename(off_id = player1_id, def_id = imputed_def_id)
  
  # PBP FGA per shooter (ground truth)
  pbp_fga <- pbp[fga_idx, ] %>%
    group_by(game_id, player1_id) %>%
    summarise(pbp_fga = n(), .groups = "drop") %>%
    rename(off_id = player1_id)
  
  imputed <- imputed %>%
    left_join(pbp_fga, by = c("game_id", "off_id")) %>%
    mutate(imputed_share = imputed_fga / pbp_fga)
  
  # ── Full join: matchup shares ↔ imputed shares ──
  comparison <- matchup_shares %>%
    full_join(imputed, by = c("game_id", "off_id", "def_id")) %>%
    left_join(pbp_fga, by = c("game_id", "off_id"), suffix = c("", ".dup")) %>%
    mutate(matchup_fga   = coalesce(matchup_fga, 0L),
           matchup_share = coalesce(matchup_share, 0),
           imputed_fga   = coalesce(imputed_fga, 0L),
           imputed_share = coalesce(imputed_share, 0),
           pbp_fga       = coalesce(pbp_fga, pbp_fga.dup, 0L),
           share_diff    = imputed_share - matchup_share) %>%
    select(-any_of("pbp_fga.dup"))
  
  eval_pairs <- comparison %>% filter(matchup_fga > 0 | imputed_fga > 0)
  
  # ── Per offensive player summary ──
  player_check <- eval_pairs %>%
    group_by(game_id, off_id, off_name) %>%
    summarise(pbp_fga         = first(pbp_fga),
              matchup_fga_sum = sum(matchup_fga),
              overcount_pct   = round((matchup_fga_sum / max(pbp_fga, 1) - 1) * 100, 1),
              n_defenders     = n(),
              proportion_mae  = round(mean(abs(share_diff)), 3),
              max_share_diff  = round(max(abs(share_diff)), 3),
              cor_shares = if (n() >= 3 &&
                               sd(matchup_share) > 0 &&
                               sd(imputed_share) > 0) {
                round(cor(matchup_share, imputed_share), 3)
                } else NA_real_,
              .groups = "drop") %>%
    arrange(desc(pbp_fga))
  
  # ── Overall metrics ──
  overall_prop_mae <- round(mean(abs(eval_pairs$share_diff)), 4)
  
  player_cors <- player_check %>% filter(!is.na(cor_shares), pbp_fga >= 5)
  wtd_cor <- if (nrow(player_cors) > 0) {
    round(weighted.mean(player_cors$cor_shares, player_cors$pbp_fga), 3)
  } else NA_real_
  
  total_pbp_fga     <- sum(pbp_fga$pbp_fga)
  total_matchup_fga <- sum(as.integer(mu_raw$matchup_field_goals_attempted),
                           na.rm = TRUE)
  overcount_pct     <- round((total_matchup_fga / max(total_pbp_fga, 1) - 1) * 100, 1)
  
  if (verbose) {
    message("\n    ═══ SANITY CHECK: imputed vs matchup FGA proportions ═══")
    message("    PBP FGA (ground truth):      ", total_pbp_fga)
    message("    Matchup FGA sum (overcounted): ", total_matchup_fga,
            " (+", overcount_pct, "% from switch crediting)")
    message("    Overall proportion MAE:      ", overall_prop_mae)
    message("    Volume-weighted correlation: ", wtd_cor)
    
    # big_players <- player_check %>% filter(pbp_fga >= 5)
    # if (nrow(big_players) > 0) {
    #   message("\n    Per-player summary (5+ FGA):")
    #   message("    ", sprintf("%-20s %5s %5s %7s %8s %5s",
    #                           "Player", "FGA", "mFGA", "Over%", "PropMAE", "Cor"))
    #   for (i in seq_len(nrow(big_players))) {
    #     p <- big_players[i, ]
    #     cor_str <- if (is.na(p$cor_shares)) {
    #       "  N/A"
    #       } else sprintf("%5.2f", p$cor_shares)
    #     message("    ", sprintf("%-20s %5d %5d  %+5.1f%% %7.3f %s",
    #                             coalesce(p$off_name, p$off_id),
    #                             p$pbp_fga, p$matchup_fga_sum,
    #                             p$overcount_pct,
    #                             p$proportion_mae, cor_str))
    #   }
    # }
  }
  
  # Store for programmatic access
  attr(pbp, "defender_sanity_check") <- list(
    comparison        = comparison,
    player_check      = player_check,
    overall_prop_mae  = overall_prop_mae,
    wtd_correlation   = wtd_cor,
    total_pbp_fga     = total_pbp_fga,
    total_matchup_fga = total_matchup_fga,
    overcount_pct     = overcount_pct
  )
  
  pbp
}





## ════════════════════════════════════════════════════════════════════════════
## 06 — BIOMETRICS   ==========================================================
## ════════════════════════════════════════════════════════════════════════════

get_biometrics <- function(season_str, season_year) {
  bio_raw <- safe_call(nba_leaguedashplayerbiostats,
                       season = season_str, season_type = "Regular Season")
  if (is.null(bio_raw)) {
    message("    WARNING: bio endpoint returned NULL")
    return(tibble(PLAYER_ID = character(), PLAYER_NAME = character(),
                  PLAYER_HEIGHT_INCHES = numeric()))#, wingspan = numeric()))
  }
  bio <- as.data.frame(bio_raw$LeagueDashPlayerBioStats) %>%
    select(PLAYER_ID, PLAYER_NAME, PLAYER_HEIGHT_INCHES ) %>%
    mutate(PLAYER_HEIGHT_INCHES = as.numeric(PLAYER_HEIGHT_INCHES)) %>% 
    filter(!is.na(PLAYER_HEIGHT_INCHES))

  combine_list <- list()
  for (yr in 2000:season_year) {
    res <- safe_call(nba_draftcombineplayeranthro,
                     season_year = as.character(yr), delay = 0.4)
    if (!is.null(res) && !is.null(res$Results) && nrow(res$Results) > 0)
      combine_list[[as.character(yr)]] <- as.data.frame(res$Results)
  }
  if (length(combine_list) > 0) {
    combine <- bind_rows(combine_list) %>%
      mutate(PLAYER_ID = as.character(PLAYER_ID),
             wingspan  = as.numeric(WINGSPAN)) %>%
      filter(!is.na(wingspan), !is.na(PLAYER_ID)) %>%
      group_by(PLAYER_ID) %>%
      slice_max(order_by = PLAYER_NAME, n = 1, with_ties = FALSE) %>%
      ungroup() %>%
      select(PLAYER_ID, wingspan)
    bio <- bio %>% left_join(combine, by = "PLAYER_ID")
  } else {
    bio$wingspan <- NA_real_
  }
  bio %>% mutate(wingspan = coalesce(wingspan, PLAYER_HEIGHT_INCHES * 1.06))
}


## ════════════════════════════════════════════════════════════════════════════
## 07 — TRACKING AGGREGATES (GAME-DATE LEVEL)  =================================
##
## Pulls the JOINT distribution of period × shot_clock_range × dribble_range
## × def_dist_range per player per game date, using parallel API calls.
##
## Speed improvements:
##
##   1. PARALLEL REQUESTS — N_WORKERS concurrent API connections split the
##      combo space evenly.  The NBA Stats API takes ~5s per response, so
##      4 workers → ~4× speedup.  480 calls: ~56 min → ~14 min per date.
##
##   2. ADAPTIVE PERIOD COUNT — game_status_text determines actual max period.
##      "Final" → 4 periods (480 calls), "Final/OT" → 5 (600), etc.
##
##   3. DIRECT API CALLS — bypasses hoopR, hits stats.nba.com directly.
##
##   4. MINIMAL INTER-CALL DELAY — 0.1s (the ~5s server response time is the
##      natural rate limiter; adding 0.4s on top of that wastes time).
##
## Caching:
##   Each game date is cached in cache/trk_dates_v3_<season>/.
##   Compatible with existing v3 caches (same schema, same directory).
## ════════════════════════════════════════════════════════════════════════════

## Helper: derive max period count from game_status_text
.max_period_from_status <- function(status_texts) {
  ot_numbers <- vapply(status_texts, function(s) {
    if (is.na(s) || !grepl("OT", s, fixed = TRUE)) return(0L)
    ot_part <- sub(".*OT", "", s)
    if (ot_part == "") return(1L)
    n <- suppressWarnings(as.integer(ot_part))
    if (is.na(n)) return(1L)
    n
  }, integer(1L), USE.NAMES = FALSE)
  4L + max(ot_numbers, na.rm = TRUE)
}

get_game_level_tracking <- function(schedule, season_str, cache_dir = "cache") {

  season_tag     <- gsub("-", "_20", substr(season_str, 1, 7))
  date_cache_dir <- file.path(cache_dir, paste0("shot_tracking_", season_tag))
  dir.create(date_cache_dir, showWarnings = FALSE, recursive = TRUE)

  game_dates <- sort(unique(as.character(schedule$game_date)))
  n_dates    <- length(game_dates)
  combos_per_period <- length(SHOT_CLOCK_RANGES) *
                       length(DRIBBLE_RANGES) *
                       length(DEF_DIST_RANGES) *
                       length(GENERAL_RANGES)          # 6 × 5 × 4 × 2 = 240

  # ── Pre-compute max period per date from game_status_text ───────────────
  has_status <- "game_status_text" %in% names(schedule)
  if (has_status) {
    date_max_period <- schedule %>%
      mutate(game_date = as.character(game_date)) %>%
      group_by(game_date) %>%
      summarise(.max_per = .max_period_from_status(game_status_text),
                .groups = "drop")
    date_max_per <- setNames(
      pmin(date_max_period$.max_per, MAX_PERIOD),
      date_max_period$game_date
    )
  } else {
    date_max_per <- setNames(rep(MAX_PERIOD, n_dates), game_dates)
  }

  total_calls <- sum(vapply(game_dates, function(d)
    as.integer(date_max_per[d]) * combos_per_period, integer(1L)))
  ot_dates    <- sum(date_max_per[game_dates] > 4L)

  use_parallel <- N_WORKERS > 1L
  message("  Game-level tracking: ", n_dates, " game-date(s), ",
          ot_dates, " with OT, ", N_WORKERS, " worker(s)")
  message("  Total API calls: ", total_calls,
          "  (est. ", round(total_calls * 5.5 / N_WORKERS / 60, 0), " min ",
          "@ ~5.5s/call ÷ ", N_WORKERS, " workers)")
  message("  Per-date cache dir: ", date_cache_dir)

  all_records <- vector("list", n_dates)

  for (d_idx in seq_along(game_dates)) {

    date_ymd   <- game_dates[d_idx]
    date_mdy   <- format(as.Date(date_ymd), "%m/%d/%Y")
    cache_file <- file.path(date_cache_dir, paste0(date_ymd, ".rds"))

    # ── Load from cache if available ────────────────────────────────────
    if (file.exists(cache_file)) {
      all_records[[d_idx]] <- readRDS(cache_file)
      if (d_idx %% 20 == 0 || d_idx == n_dates)
        message("  [", d_idx, "/", n_dates, "] ", date_ymd, " — loaded from cache")
      next
    }

    max_per       <- as.integer(date_max_per[date_ymd])
    date_n_combos <- max_per * combos_per_period

    message("  [", d_idx, "/", n_dates, "] ", date_ymd,
            " (", max_per, " per, ", date_n_combos, " calls, ",
            N_WORKERS, " workers) scraping…")

    # ── Build full combo grid for this date ─────────────────────────────
    combos <- expand.grid(
      period            = seq_len(max_per),
      shot_clock_range  = SHOT_CLOCK_RANGES,
      dribble_range     = DRIBBLE_RANGES,
      close_def_dist_range = DEF_DIST_RANGES,
      general_range     = GENERAL_RANGES,
      stringsAsFactors  = FALSE,
      KEEP.OUT.ATTRS    = FALSE
    )

    t_start <- Sys.time()

    if (use_parallel) {
      # ── PARALLEL: split combos across workers ───────────────────────
      # Split into N_WORKERS roughly equal chunks
      chunk_ids <- sort(rep(seq_len(N_WORKERS), length.out = nrow(combos)))
      chunks    <- split(combos, chunk_ids)

      cl <- makeCluster(N_WORKERS)
      on.exit(stopCluster(cl), add = TRUE)

      chunk_results <- parLapply(cl, chunks, .fetch_combo_batch,
                                 date_mdy   = date_mdy,
                                 season_str = season_str)

      stopCluster(cl)
      on.exit(NULL)   # clear the on.exit since we stopped manually

      # Combine chunk results
      date_df <- bind_rows(chunk_results)

    } else {
      # ── SEQUENTIAL: single-threaded with progress ───────────────────
      combo_results <- vector("list", date_n_combos)

      for (i in seq_len(nrow(combos))) {
        if (i %% 30 == 1 || i == nrow(combos)) {
          remaining <- date_n_combos - i
          elapsed   <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))
          rate      <- if (i > 1) elapsed / (i - 1) else 5.5
          message("    combo ", i, "/", date_n_combos,
                  "  (", remaining, " left, ~",
                  round(remaining * rate / 60, 1), " min remaining)",
                  "  P", combos$period[i],
                  " | ", combos$shot_clock_range[i],
                  " | ", combos$dribble_range[i],
                  " | ", combos$close_def_dist_range[i],
                  " | ", ifelse(combos$general_range[i] == "", "Overall",
                                combos$general_range[i]))
        }

        df <- .nba_stats_ptshot_direct(
          date_from            = date_mdy,
          date_to              = date_mdy,
          season               = season_str,
          period               = combos$period[i],
          shot_clock_range     = combos$shot_clock_range[i],
          dribble_range        = combos$dribble_range[i],
          close_def_dist_range = combos$close_def_dist_range[i],
          general_range        = combos$general_range[i]
        )

        if (!is.null(df) && nrow(df) > 0) {
          df$period            <- combos$period[i]
          df$shot_clock_range  <- combos$shot_clock_range[i]
          df$dribble_range     <- combos$dribble_range[i]
          df$def_dist_range    <- combos$close_def_dist_range[i]
          df$general_range     <- combos$general_range[i]
          combo_results[[i]]  <- df
        }
      }
      date_df <- bind_rows(combo_results)
    }

    # ── Normalise columns ───────────────────────────────────────────────
    if (!is.null(date_df) && nrow(date_df) > 0) {
      date_df <- date_df %>%
        transmute(
          PLAYER_ID         = as.character(PLAYER_ID),
          period            = as.integer(period),
          shot_clock_range  = shot_clock_range,
          dribble_range     = dribble_range,
          def_dist_range    = def_dist_range,
          general_range     = general_range,
          fg2a              = as.integer(FG2A),
          fg3a              = as.integer(FG3A),
          fg2m              = as.integer(FG2M),
          fg3m              = as.integer(FG3M),
          fgm               = as.integer(FGM),
          fga               = as.integer(FGA)
        )
      date_df$game_date <- date_ymd
      # ── De-overlap general_range ──────────────────────────────────────
      # Overall ("") is a superset that includes "Less Than 10 ft".
      # Subtract LT10 counts from Overall to produce a non-overlapping
      # complement (stays "").  Keeps "Less Than 10 ft" rows as-is.
      if ("general_range" %in% names(date_df) &&
          any(date_df$general_range == "Less Than 10 ft")) {
        
        join_cols <- c("PLAYER_ID", "game_date", "period",
                       "shot_clock_range", "dribble_range", "def_dist_range")
        
        overall <- date_df %>% filter(general_range == "")
        lt10    <- date_df %>% filter(general_range == "Less Than 10 ft")
        
        gt10 <- overall %>%
          left_join(lt10 %>% select(all_of(join_cols),
                                    fg2a_lt = fg2a, fg3a_lt = fg3a,
                                    fg2m_lt = fg2m, fg3m_lt = fg3m,
                                    fgm_lt  = fgm,  fga_lt  = fga),
                    by = join_cols) %>%
          mutate(across(ends_with("_lt"), ~ coalesce(.x, 0L)),
                 fg2a = fg2a - fg2a_lt,
                 fg3a = fg3a - fg3a_lt,
                 fg2m = fg2m - fg2m_lt,
                 fg3m = fg3m - fg3m_lt,
                 fgm  = fgm  - fgm_lt,
                 fga  = fga  - fga_lt) %>%
                 # general_range stays "" (= 10+ ft complement)
          select(-ends_with("_lt")) %>%
          filter(fga > 0L)
        
        date_df <- bind_rows(lt10, gt10)
      }
    } else {
      date_df <- tibble(
        PLAYER_ID = character(), period = integer(),
        shot_clock_range = character(), dribble_range = character(),
        def_dist_range = character(), general_range = character(),
        fg2a = integer(), fg3a = integer(),
        fg2m = integer(), fg3m = integer(),
        fgm = integer(), fga = integer(),
        game_date = character()
      )
    }
    
    elapsed <- round(as.numeric(difftime(Sys.time(), t_start, units = "secs")), 1)
    message("    ✓ ", nrow(date_df), " rows  |  ",
            n_distinct(date_df$PLAYER_ID), " players  |  ",
            elapsed, "s elapsed (",
            round(elapsed / 60, 1), " min)")
    
    saveRDS(date_df, cache_file)
    all_records[[d_idx]] <- date_df
  }
  

  out <- bind_rows(all_records)
  message("  Total tracking rows: ", nrow(out),
          "  |  unique players: ", n_distinct(out$PLAYER_ID),
          "  |  dates: ", n_distinct(out$game_date))
  out
}



## ════════════════════════════════════════════════════════════════════════════
## get_lt10_tracking()  — Retrofit existing tracking caches with LT10 split
##
## Reads each existing date-level .rds cache (which has no general_range
## column), scrapes ONLY the "Less Than 10 ft" slice from the NBA Stats API,
## then merges and de-overlaps so each cached file has non-overlapping
## general_range values:
##   ""                 = shots >= 10 ft  (Overall minus LT10)
##   "Less Than 10 ft"  = shots < 10 ft   (scraped directly)
##
## The de-overlapped files can then be used by impute_shot_context() to
## condition imputation on shot distance.
##
## Parallel scraping via N_WORKERS, identical to get_game_level_tracking().
## ════════════════════════════════════════════════════════════════════════════

get_lt10_tracking <- function(schedule, season_str, cache_dir = "cache") {

  season_tag     <- gsub("-", "_20", substr(season_str, 1, 7))
  date_cache_dir <- file.path(cache_dir, paste0("shot_tracking_", season_tag))

  if (!dir.exists(date_cache_dir)) {
    stop("Cache dir not found: ", date_cache_dir,
         "\n  Run get_game_level_tracking() first.")
  }

  game_dates <- sort(unique(as.character(schedule$game_date)))
  n_dates    <- length(game_dates)

  # Combo grid for LT10 only (no period × sc × dr × dd, same as main scraper
  # but with general_range fixed to "Less Than 10 ft")
  combos_per_period <- length(SHOT_CLOCK_RANGES) *
                       length(DRIBBLE_RANGES) *
                       length(DEF_DIST_RANGES)           # 6 × 5 × 4 = 120

  # ── Pre-compute max period per date ──────────────────────────────────────
  has_status <- "game_status_text" %in% names(schedule)
  if (has_status) {
    date_max_period <- schedule %>%
      mutate(game_date = as.character(game_date)) %>%
      group_by(game_date) %>%
      summarise(.max_per = .max_period_from_status(game_status_text),
                .groups = "drop")
    date_max_per <- setNames(
      pmin(date_max_period$.max_per, MAX_PERIOD),
      date_max_period$game_date
    )
  } else {
    date_max_per <- setNames(rep(MAX_PERIOD, n_dates), game_dates)
  }

  total_calls <- sum(vapply(game_dates, function(d)
    as.integer(date_max_per[d]) * combos_per_period, integer(1L)))

  use_parallel <- N_WORKERS > 1L
  message("  LT10 retrofit: ", n_dates, " dates, ",
          N_WORKERS, " worker(s), ", total_calls, " total calls")
  message("  Est. ", round(total_calls * 5.5 / N_WORKERS / 60, 0),
          " min @ ~5.5s/call")

  all_records <- vector("list", n_dates)

  for (d_idx in seq_along(game_dates)) {

    date_ymd   <- game_dates[d_idx]
    date_mdy   <- format(as.Date(date_ymd), "%m/%d/%Y")
    cache_file <- file.path(date_cache_dir, paste0(date_ymd, ".rds"))

    # ── Load existing cache ─────────────────────────────────────────────
    if (!file.exists(cache_file)) {
      message("  [", d_idx, "/", n_dates, "] ", date_ymd, " — no cache, skipping")
      next
    }

    existing <- readRDS(cache_file)

    # Already retrofitted? Skip.
    if ("general_range" %in% names(existing)) {
      all_records[[d_idx]] <- existing
      if (d_idx %% 20 == 0 || d_idx == n_dates)
        message("  [", d_idx, "/", n_dates, "] ", date_ymd,
                " — already has general_range, skipping")
      next
    }

    # Tag existing data as Overall ("")
    existing$general_range <- ""

    max_per       <- as.integer(date_max_per[date_ymd])
    date_n_combos <- max_per * combos_per_period

    message("  [", d_idx, "/", n_dates, "] ", date_ymd,
            " (", max_per, " per, ", date_n_combos, " LT10 calls) scraping…")

    # ── Build LT10-only combo grid ──────────────────────────────────────
    combos <- expand.grid(
      period               = seq_len(max_per),
      shot_clock_range     = SHOT_CLOCK_RANGES,
      dribble_range        = DRIBBLE_RANGES,
      close_def_dist_range = DEF_DIST_RANGES,
      stringsAsFactors     = FALSE,
      KEEP.OUT.ATTRS       = FALSE
    )
    combos$general_range <- "Less Than 10 ft"

    t_start <- Sys.time()

    if (use_parallel) {
      chunk_ids <- sort(rep(seq_len(N_WORKERS), length.out = nrow(combos)))
      chunks    <- split(combos, chunk_ids)
      cl <- makeCluster(N_WORKERS)
      on.exit(stopCluster(cl), add = TRUE)
      chunk_results <- parLapply(cl, chunks, .fetch_combo_batch,
                                 date_mdy   = date_mdy,
                                 season_str = season_str)
      stopCluster(cl)
      on.exit(NULL)
      lt10_df <- bind_rows(chunk_results)
    } else {
      combo_results <- vector("list", nrow(combos))
      for (i in seq_len(nrow(combos))) {
        df <- .nba_stats_ptshot_direct(
          date_from            = date_mdy,
          date_to              = date_mdy,
          season               = season_str,
          period               = combos$period[i],
          shot_clock_range     = combos$shot_clock_range[i],
          dribble_range        = combos$dribble_range[i],
          close_def_dist_range = combos$close_def_dist_range[i],
          general_range        = "Less Than 10 ft"
        )
        if (!is.null(df) && nrow(df) > 0) {
          df$period            <- combos$period[i]
          df$shot_clock_range  <- combos$shot_clock_range[i]
          df$dribble_range     <- combos$dribble_range[i]
          df$def_dist_range    <- combos$close_def_dist_range[i]
          df$general_range     <- "Less Than 10 ft"
          combo_results[[i]]  <- df
        }
      }
      lt10_df <- bind_rows(combo_results)
    }

    # ── Normalise LT10 columns to match existing schema ─────────────────
    if (!is.null(lt10_df) && nrow(lt10_df) > 0) {
      lt10_df <- lt10_df %>%
        transmute(
          PLAYER_ID         = as.character(PLAYER_ID),
          period            = as.integer(period),
          shot_clock_range  = shot_clock_range,
          dribble_range     = dribble_range,
          def_dist_range    = def_dist_range,
          general_range     = general_range,
          fg2a              = as.integer(FG2A),
          fg3a              = as.integer(FG3A),
          fg2m              = as.integer(FG2M),
          fg3m              = as.integer(FG3M),
          fgm               = as.integer(FGM),
          fga               = as.integer(FGA)
        )
      lt10_df$game_date <- date_ymd
    } else {
      lt10_df <- tibble(
        PLAYER_ID = character(), period = integer(),
        shot_clock_range = character(), dribble_range = character(),
        def_dist_range = character(), general_range = character(),
        fg2a = integer(), fg3a = integer(),
        fg2m = integer(), fg3m = integer(),
        fgm = integer(), fga = integer(),
        game_date = character()
      )
    }

    # ── De-overlap: subtract LT10 from Overall to get complement ────────
    join_cols <- c("PLAYER_ID", "game_date", "period",
                   "shot_clock_range", "dribble_range", "def_dist_range")

    complement <- existing %>%
      left_join(lt10_df %>% select(all_of(join_cols),
                                   fg2a_lt = fg2a, fg3a_lt = fg3a,
                                   fg2m_lt = fg2m, fg3m_lt = fg3m,
                                   fgm_lt  = fgm,  fga_lt  = fga),
                by = join_cols) %>%
      mutate(across(ends_with("_lt"), ~ coalesce(.x, 0L)),
             fg2a = fg2a - fg2a_lt,
             fg3a = fg3a - fg3a_lt,
             fg2m = fg2m - fg2m_lt,
             fg3m = fg3m - fg3m_lt,
             fgm  = fgm  - fgm_lt,
             fga  = fga  - fga_lt) %>%
      select(-ends_with("_lt")) %>%
      filter(fga > 0L)
    # complement retains general_range = "" (representing 10+ ft)

    date_df <- bind_rows(lt10_df, complement)

    elapsed <- round(as.numeric(difftime(Sys.time(), t_start, units = "secs")), 1)
    n_lt10 <- sum(date_df$general_range == "Less Than 10 ft")
    n_gt10 <- sum(date_df$general_range == "")
    message("    ✓ ", nrow(date_df), " rows (LT10=", n_lt10,
            ", 10+=", n_gt10, ")  |  ",
            n_distinct(date_df$PLAYER_ID), " players  |  ",
            elapsed, "s")

    # ── Overwrite cache with de-overlapped version ──────────────────────
    saveRDS(date_df, cache_file)
    all_records[[d_idx]] <- date_df
  }

  out <- bind_rows(all_records)
  message("  LT10 retrofit complete: ", nrow(out), " total rows  |  ",
          n_distinct(out$PLAYER_ID), " players  |  ",
          n_distinct(out$game_date), " dates")
  out
}



## ════════════════════════════════════════════════════════════════════════════
## impute_shot_context()  — v4.0
##
## Per-shot imputation using the game-date-level joint tracking distribution,
## conditioned on (1) shot clock bucket, (2) period, and now (3) shot distance
## via general_range ("Less Than 10 ft" vs "" = 10+ ft).
##
## v4.0 changes vs v3.3:
##   - SHOT DISTANCE CONDITIONING: if game_tracking has a general_range column,
##     separate lookup tables are built for LT10 and 10+ ft.  Each shot first
##     tries the distance-specific lookups; if empty, falls back to combined.
##   - PRE-SAMPLING CONSTRAINT APPLICATION: override logic (descriptor, contact,
##     force-zero-drib) is applied to the weight matrix BEFORE drawing, not
##     as post-hoc fixups.  This produces more accurate joint (dr, dd) draws
##     and eliminates the need for post-hoc re-balancing.
##   - GROUP KEY includes dist_bin ("lt10" / "10plus") when available.
##
## FALLBACK CHAIN (per lookup set, 7 levels most→least specific):
##   1. pgd + specific_period + specific_sc
##   2. pgd + period=0        + specific_sc
##   3. season + specific_period + specific_sc
##   4. season + period=0        + specific_sc
##   5. league + specific_period + specific_sc
##   6. league + period=0        + specific_sc
##   7. league + period=0 + ALL
##
## If the distance-specific lookup set yields NULL at all 7 levels,
## the combined (all-distance) lookup set is tried as ultimate fallback.
## ════════════════════════════════════════════════════════════════════════════

impute_shot_context <- function(pbp, game_tracking, seed = 42, verbose = TRUE) {

  set.seed(seed)

  n_dr <- length(DRIBBLE_RANGES)
  n_dd <- length(DEF_DIST_RANGES)
  n_sc <- length(SHOT_CLOCK_RANGES)

  bin_defs <- list("0-2 Feet - Very Tight" = c(0.0, 2.0),
                   "2-4 Feet - Tight"      = c(2.0, 4.0),
                   "4-6 Feet - Open"       = c(4.0, 6.0),
                   "6+ Feet - Wide Open"   = c(6.0, 10.0))

  non_cs_pattern <- "pullup|step.?back|driving"

  force_zero_drib_pattern <- "^tip$|alley.?oop|^putback$"


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: shot_clock → sc_key
  # ═══════════════════════════════════════════════════════════════════════════

  sc_to_key <- function(sc, eps = SC_BLEND_EPS) {
    if (is.na(sc) || sc < 0) return("ALL")

    for (b in seq_along(SC_BOUNDARIES)) {
      if (abs(sc - SC_BOUNDARIES[b]) <= eps)
        return(paste0(SHOT_CLOCK_RANGES[b], "||", SHOT_CLOCK_RANGES[b + 1L]))
    }

    if (sc > 22) return("24-22")
    if (sc > 18) return("22-18 Very Early")
    if (sc > 15) return("18-15 Early")
    if (sc > 7)  return("15-7 Average")
    if (sc > 4)  return("7-4 Late")
    "4-0 Very Late"
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: build 5×4 matrices from a data frame slice
  # ═══════════════════════════════════════════════════════════════════════════

  build_matrices <- function(df) {
    m2  <- matrix(0L, nrow = n_dr, ncol = n_dd,
                  dimnames = list(DRIBBLE_RANGES, DEF_DIST_RANGES))
    m3  <- matrix(0L, nrow = n_dr, ncol = n_dd,
                  dimnames = list(DRIBBLE_RANGES, DEF_DIST_RANGES))
    m2m <- matrix(0L, nrow = n_dr, ncol = n_dd,
                  dimnames = list(DRIBBLE_RANGES, DEF_DIST_RANGES))
    m3m <- matrix(0L, nrow = n_dr, ncol = n_dd,
                  dimnames = list(DRIBBLE_RANGES, DEF_DIST_RANGES))
    has_2m <- "fg2m" %in% names(df)
    has_3m <- "fg3m" %in% names(df)
    for (r in seq_len(nrow(df))) {
      dr <- df$dribble_range[r]
      dd <- df$def_dist_range[r]
      if (dr %in% DRIBBLE_RANGES && dd %in% DEF_DIST_RANGES) {
        m2[dr, dd]  <- m2[dr, dd]  + as.integer(coalesce(df$fg2a[r], 0L))
        m3[dr, dd]  <- m3[dr, dd]  + as.integer(coalesce(df$fg3a[r], 0L))
        if (has_2m) m2m[dr, dd] <- m2m[dr, dd] + as.integer(coalesce(df$fg2m[r], 0L))
        if (has_3m) m3m[dr, dd] <- m3m[dr, dd] + as.integer(coalesce(df$fg3m[r], 0L))
      }
    }
    list(w2 = m2, w3 = m3, w2m = m2m, w3m = m3m)
  }

  # NULL-safe matrix addition
  add_mats <- function(a, b) {
    if (is.null(a)) return(b)
    if (is.null(b)) return(a)
    list(w2  = a$w2  + b$w2,
         w3  = a$w3  + b$w3,
         w2m = a$w2m + b$w2m,
         w3m = a$w3m + b$w3m)
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: resolve weight matrices from a lookup
  # ═══════════════════════════════════════════════════════════════════════════

  resolve_wl <- function(base_key, sc_key, lookup) {
    if (!grepl("||", sc_key, fixed = TRUE)) {
      return(lookup[[paste0(base_key, sc_key)]])
    }
    parts <- strsplit(sc_key, "||", fixed = TRUE)[[1]]
    a <- lookup[[paste0(base_key, parts[1])]]
    b <- lookup[[paste0(base_key, parts[2])]]
    add_mats(a, b)
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: build all 3 lookup tiers from a game_tracking subset
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Returns list(pgd = ..., ps = ..., league = ..., pgd_keys = ...)
  # Each is a named list of 5×4 matrix-sets keyed by the usual scheme.

  build_tracking_lookups <- function(gt) {

    if (is.null(gt) || nrow(gt) == 0)
      return(list(pgd = list(), ps = list(), league = list(), pgd_keys = character()))

    pgd <- list()
    ps  <- list()
    lg  <- list()

    # ── Primary: player × game_date × period × sc_bucket ─────────────────

    # Period-specific × sc-specific
    gt_pgd_per_sc <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
             period %in% seq_len(MAX_PERIOD)) %>%
      mutate(.key = paste0(PLAYER_ID, "|", game_date, "|", period, "|", shot_clock_range))
    for (k in unique(gt_pgd_per_sc$.key))
      pgd[[k]] <- build_matrices(gt_pgd_per_sc[gt_pgd_per_sc$.key == k, ])

    # Period-specific × ALL-sc
    gt_pgd_per_all <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
             period %in% seq_len(MAX_PERIOD)) %>%
      group_by(PLAYER_ID, game_date, period, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    for (k in unique(paste0(gt_pgd_per_all$PLAYER_ID, "|",
                             gt_pgd_per_all$game_date, "|",
                             gt_pgd_per_all$period))) {
      pts <- strsplit(k, "\\|")[[1]]
      sl  <- gt_pgd_per_all %>%
        filter(PLAYER_ID == pts[1], game_date == pts[2], period == as.integer(pts[3]))
      pgd[[paste0(k, "|ALL")]] <- build_matrices(sl)
    }

    # Period=0 × sc-specific
    gt_pgd_0_sc <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
      mutate(.key = paste0(PLAYER_ID, "|", game_date, "|0|", shot_clock_range))
    for (k in unique(gt_pgd_0_sc$.key))
      pgd[[k]] <- build_matrices(gt_pgd_0_sc[gt_pgd_0_sc$.key == k, ])

    # Period=0 × ALL-sc
    gt_pgd_all <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
      group_by(PLAYER_ID, game_date, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    pgd_all_keys <- unique(paste0(gt_pgd_all$PLAYER_ID, "|", gt_pgd_all$game_date))
    for (pgd_k in pgd_all_keys) {
      parts <- strsplit(pgd_k, "\\|")[[1]]
      slice <- gt_pgd_all[gt_pgd_all$PLAYER_ID == parts[1] &
                            gt_pgd_all$game_date  == parts[2], ]
      pgd[[paste0(pgd_k, "|0|ALL")]] <- build_matrices(slice)
    }

    # ── Secondary: player season × period × sc_bucket ────────────────────

    # Period-specific × sc-specific
    gt_ps_per_sc <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
             period %in% seq_len(MAX_PERIOD)) %>%
      group_by(PLAYER_ID, period, shot_clock_range, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop") %>%
      mutate(.key = paste0(PLAYER_ID, "|", period, "|", shot_clock_range))
    for (k in unique(gt_ps_per_sc$.key))
      ps[[k]] <- build_matrices(gt_ps_per_sc[gt_ps_per_sc$.key == k, ])

    # Period-specific × ALL-sc
    gt_ps_per_all <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
             period %in% seq_len(MAX_PERIOD)) %>%
      group_by(PLAYER_ID, period, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    for (k in unique(paste0(gt_ps_per_all$PLAYER_ID, "|",
                             gt_ps_per_all$period))) {
      pts <- strsplit(k, "\\|")[[1]]
      sl  <- gt_ps_per_all %>%
        filter(PLAYER_ID == pts[1], period == as.integer(pts[2]))
      ps[[paste0(k, "|ALL")]] <- build_matrices(sl)
    }

    # Period=0 × sc-specific
    gt_ps_0_sc <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
      group_by(PLAYER_ID, shot_clock_range, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop") %>%
      mutate(.key = paste0(PLAYER_ID, "|0|", shot_clock_range))
    for (k in unique(gt_ps_0_sc$.key))
      ps[[k]] <- build_matrices(gt_ps_0_sc[gt_ps_0_sc$.key == k, ])

    # Period=0 × ALL-sc
    gt_ps_all <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
      group_by(PLAYER_ID, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    for (pid in unique(gt_ps_all$PLAYER_ID))
      ps[[paste0(pid, "|0|ALL")]] <-
        build_matrices(gt_ps_all[gt_ps_all$PLAYER_ID == pid, ])

    # ── Tertiary: league × period × sc_bucket ────────────────────────────

    # Period-specific × sc-specific
    gt_league_per_sc <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
             period %in% seq_len(MAX_PERIOD)) %>%
      group_by(period, shot_clock_range, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    for (per in seq_len(MAX_PERIOD)) {
      for (sc in SHOT_CLOCK_RANGES) {
        sl <- gt_league_per_sc %>% filter(period == per, shot_clock_range == sc)
        if (nrow(sl) > 0)
          lg[[paste0(per, "|", sc)]] <- build_matrices(sl)
      }
    }

    # Period-specific × ALL-sc
    gt_league_per_all <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
             period %in% seq_len(MAX_PERIOD)) %>%
      group_by(period, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    for (per in seq_len(MAX_PERIOD)) {
      sl <- gt_league_per_all %>% filter(period == per)
      if (nrow(sl) > 0)
        lg[[paste0(per, "|ALL")]] <- build_matrices(sl)
    }

    # Period=0 × sc-specific
    gt_league_0_sc <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
      group_by(shot_clock_range, dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    for (sc in SHOT_CLOCK_RANGES)
      lg[[paste0("0|", sc)]] <-
        build_matrices(gt_league_0_sc[gt_league_0_sc$shot_clock_range == sc, ])

    # Period=0 × ALL-sc (absolute last resort)
    gt_league_all <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
      group_by(dribble_range, def_dist_range) %>%
      summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
                fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
                .groups = "drop")
    lg[["0|ALL"]] <- build_matrices(gt_league_all)

    list(pgd = pgd, ps = ps, league = lg, pgd_keys = pgd_all_keys)
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: 7-level fallback resolution across one lookup set
  # ═══════════════════════════════════════════════════════════════════════════

  resolve_7level <- function(g_pid, g_date, g_per, g_sc, lkps) {
    pgd_per_base    <- paste0(g_pid, "|", g_date, "|", g_per, "|")
    pgd_0_base      <- paste0(g_pid, "|", g_date, "|0|")
    ps_per_base     <- paste0(g_pid, "|",           g_per, "|")
    ps_0_base       <- paste0(g_pid, "|0|")
    league_per_base <- paste0(g_per, "|")

    wl  <- resolve_wl(pgd_per_base, g_sc, lkps$pgd)
    src <- "pgd-period"

    if (is.null(wl)) { wl <- resolve_wl(pgd_0_base, g_sc, lkps$pgd);      src <- "pgd" }
    if (is.null(wl)) { wl <- resolve_wl(ps_per_base, g_sc, lkps$ps);      src <- "season-period" }
    if (is.null(wl)) { wl <- resolve_wl(ps_0_base, g_sc, lkps$ps);        src <- "season" }
    if (is.null(wl)) { wl <- resolve_wl(league_per_base, g_sc, lkps$league); src <- "league-period" }
    if (is.null(wl)) { wl <- resolve_wl("0|", g_sc, lkps$league);         src <- "league" }
    if (is.null(wl)) { wl <- lkps$league[["0|ALL"]];                      src <- "league-all" }

    if (is.null(wl)) return(NULL)
    list(wl = wl, src = src)
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # INITIALIZE OUTPUT COLUMNS
  # ═══════════════════════════════════════════════════════════════════════════

  pbp$is_catch_shoot         <- NA_integer_
  pbp$p_catch_shoot          <- NA_real_
  pbp$expected_def_dist      <- NA_real_
  pbp$imputed_dribble_range  <- NA_character_
  pbp$imputed_def_dist_range <- NA_character_

  fga_idx <- which(pbp$fga == 1)
  if (length(fga_idx) == 0) return(pbp)

  if (is.null(game_tracking) || nrow(game_tracking) == 0) {
    message("  WARNING: no game-level tracking data — shot context will be NA")
    return(pbp)
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # BUILD LOOKUP TABLES — distance-specific + combined
  # ═══════════════════════════════════════════════════════════════════════════

  has_general_range <- "general_range" %in% names(game_tracking)

  message("  Building tracking lookup tables",
          if (has_general_range) " (distance-specific + combined)…" else "…")

  if (has_general_range) {
    gt_lt10   <- game_tracking %>% filter(general_range == "Less Than 10 ft")
    gt_10plus <- game_tracking %>% filter(general_range == "")

    # Combined: sum across general_range slices
    gt_combined <- game_tracking %>%
      group_by(PLAYER_ID, game_date, period, shot_clock_range,
               dribble_range, def_dist_range) %>%
      summarise(across(c(fg2a, fg3a, fg2m, fg3m, fgm, fga), sum),
                .groups = "drop")

    lookups_lt10   <- build_tracking_lookups(gt_lt10)
    lookups_10plus <- build_tracking_lookups(gt_10plus)
    lookups_all    <- build_tracking_lookups(gt_combined)

    message("    LT10 pgd keys:  ", length(lookups_lt10$pgd))
    message("    10+  pgd keys:  ", length(lookups_10plus$pgd))
    message("    Combined pgd:   ", length(lookups_all$pgd))

  } else {
    lookups_all    <- build_tracking_lookups(game_tracking)
    lookups_lt10   <- NULL
    lookups_10plus <- NULL

    message("    Combined pgd: ", length(lookups_all$pgd),
            " (no general_range column — no distance split)")
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # ANNOTATE FGA ROWS WITH PER-SHOT METADATA
  # ═══════════════════════════════════════════════════════════════════════════

  shot_clocks <- coalesce(pbp$shot_clock[fga_idx], pbp$game_clock_sec[fga_idx])
  shot_dists  <- as.numeric(pbp$shot_distance[fga_idx])

  fga_meta <- data.frame(
    row_idx      = fga_idx,
    pid          = as.character(pbp$player1_id[fga_idx]),
    gdate        = as.character(pbp$game_date[fga_idx]),
    period       = as.integer(pbp$period[fga_idx]),
    is_3         = as.integer(pbp$is_three[fga_idx] == 1),
    is_rim       = as.integer(pbp$shot_family[fga_idx] == "rim"),
    is_contact   = as.integer(pbp$is_contact_shot[fga_idx] == 1),
    descriptor   = pbp$descriptor[fga_idx],
    shot_clock   = shot_clocks,
    shot_dist    = shot_dists,
    stringsAsFactors = FALSE
  )
  fga_meta$sc_key <- vapply(seq_len(nrow(fga_meta)), function(j) {
    eps_j <- if (isTRUE(pbp$poss_start_is_liveball[fga_idx[j]])) {
      SC_BLEND_EPS_LIVEBALL
    } else {
      SC_BLEND_EPS
    }
    sc_to_key(fga_meta$shot_clock[j], eps = eps_j)
  }, character(1L))

  # Distance bin: "lt10" if shot_distance < 10, "10plus" otherwise
  fga_meta$dist_bin <- ifelse(!is.na(fga_meta$shot_dist) & fga_meta$shot_dist < 10,
                              "lt10", "10plus")

  fga_meta$definite_non_cs <-
    (!is.na(fga_meta$descriptor) &
       grepl(non_cs_pattern, fga_meta$descriptor, ignore.case = TRUE))

  fga_meta$force_zero_drib <-
    (!is.na(fga_meta$descriptor) &
       grepl(force_zero_drib_pattern, fga_meta$descriptor, ignore.case = TRUE))


  n_fga        <- nrow(fga_meta)
  res_dr       <- rep(NA_character_, n_fga)
  res_dd       <- rep(NA_character_, n_fga)
  res_p_cs     <- rep(NA_real_,      n_fga)
  res_src      <- rep(NA_character_, n_fga)
  res_assign   <- rep(NA_character_, n_fga)


  # ═══════════════════════════════════════════════════════════════════════════
  # STAGE 1: PER-SHOT ASSIGNMENT WITH PRE-SAMPLING CONSTRAINTS
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Group key: (pid, gdate, period, is_3, sc_key, dist_bin)
  #
  # Within each group, the UNCONSTRAINED weight matrix is resolved once.
  # Then, for each shot, per-shot constraints are applied to a copy of the
  # matrix before drawing a single (dr, dd) pair.
  #
  # Constraints (applied as row/column masks):
  #   - Override A (definite_non_cs):  zero out "0 Dribbles" row
  #   - Override C (force_zero_drib):  zero out all rows EXCEPT "0 Dribbles"
  #   - Override B (is_contact):       zero out all columns except "0-2 Feet"
  #
  # These constraints are mutually compatible (A and B, C and B can co-occur).

  group_keys    <- paste0(fga_meta$pid,    "|", fga_meta$gdate, "|",
                          fga_meta$period, "|", fga_meta$is_3,  "|",
                          fga_meta$sc_key, "|", fga_meta$dist_bin)
  unique_groups <- unique(group_keys)

  wl_cache <- list()   # group_key → list(wl, src)

  n_desc_override   <- 0L
  n_contact_override <- 0L
  n_force_zero      <- 0L
  n_dist_specific   <- 0L
  n_dist_fallback   <- 0L

  for (gk in unique_groups) {

    grp_mask <- which(group_keys == gk)
    n_shots  <- length(grp_mask)

    # Parse group key
    parts    <- strsplit(gk, "\\|")[[1]]
    g_pid    <- parts[1]
    g_date   <- parts[2]
    g_per    <- as.integer(parts[3])
    g_is3    <- as.integer(parts[4])
    # sc_key may contain "|" from "||" blend
    g_distb  <- parts[length(parts)]
    g_sc     <- paste(parts[5:(length(parts) - 1)], collapse = "|")

    # ── Resolve unconstrained weight matrix for this group ────────────
    if (is.null(wl_cache[[gk]])) {

      wl  <- NULL
      src <- NA_character_

      # Try distance-specific lookups first
      if (has_general_range) {
        dist_lkps <- if (g_distb == "lt10") lookups_lt10 else lookups_10plus
        if (!is.null(dist_lkps)) {
          res_dist <- resolve_7level(g_pid, g_date, g_per, g_sc, dist_lkps)
          if (!is.null(res_dist)) {
            wl  <- res_dist$wl
            src <- paste0(res_dist$src, "-dist")
            n_dist_specific <- n_dist_specific + n_shots
          }
        }
      }

      # Fallback to combined lookups
      if (is.null(wl)) {
        res_comb <- resolve_7level(g_pid, g_date, g_per, g_sc, lookups_all)
        if (!is.null(res_comb)) {
          wl  <- res_comb$wl
          src <- res_comb$src
          n_dist_fallback <- n_dist_fallback + n_shots
        }
      }

      wl_cache[[gk]] <- list(wl = wl, src = src)
    }

    wl  <- wl_cache[[gk]]$wl
    src <- wl_cache[[gk]]$src

    if (is.null(wl)) {
      res_dr[grp_mask]     <- sample(DRIBBLE_RANGES,  n_shots, replace = TRUE)
      res_dd[grp_mask]     <- sample(DEF_DIST_RANGES, n_shots, replace = TRUE)
      res_assign[grp_mask] <- "uniform"
      res_src[grp_mask]    <- "uniform"
      res_p_cs[grp_mask]   <- 0.0
      next
    }

    # ── Shot-type-specific attempt matrix ────────────────────────────
    w_att_base <- if (g_is3 == 1L) wl$w3 else wl$w2
    if (sum(w_att_base) == 0L) w_att_base <- wl$w2 + wl$w3

    # ── p_catch_shoot (from unconstrained matrix) ────────────────────
    p_cs_group <- if (sum(w_att_base) > 0L) {
      sum(w_att_base["0 Dribbles", ]) / sum(w_att_base)
    } else 0.0
    res_p_cs[grp_mask] <- p_cs_group
    res_src[grp_mask]  <- src

    # ── Per-shot constrained sampling ────────────────────────────────
    for (j in grp_mask) {

      w_att <- w_att_base    # start with group matrix

      # Override C: force 0 Dribbles (tip / alley-oop / putback)
      if (fga_meta$force_zero_drib[j]) {
        w_att[-which(DRIBBLE_RANGES == "0 Dribbles"), ] <- 0L
        n_force_zero <- n_force_zero + 1L
      }

      # Override A: definite non-C&S → exclude 0 Dribbles
      if (fga_meta$definite_non_cs[j]) {
        w_att["0 Dribbles", ] <- 0L
        n_desc_override <- n_desc_override + 1L
      }

      # Override B: contact shot → force 0-2 Feet
      if (fga_meta$is_contact[j] == 1L) {
        w_att[, -which(DEF_DIST_RANGES == "0-2 Feet - Very Tight")] <- 0L
        n_contact_override <- n_contact_override + 1L
      }

      # ── Draw one (dr, dd) pair from constrained matrix ────────────
      total <- sum(w_att)
      if (total > 0L) {
        flat_probs <- as.vector(w_att) / total
        draw       <- sample(n_dr * n_dd, 1L, prob = flat_probs)
        r_idx      <- ((draw - 1L) %% n_dr) + 1L
        c_idx      <- ((draw - 1L) %/% n_dr) + 1L
        res_dr[j]  <- DRIBBLE_RANGES[r_idx]
        res_dd[j]  <- DEF_DIST_RANGES[c_idx]
        res_assign[j] <- "constrained"
      } else {
        # All cells zeroed by constraints — use constraint-aware fallback
        if (fga_meta$force_zero_drib[j]) {
          res_dr[j] <- "0 Dribbles"
        } else if (fga_meta$definite_non_cs[j]) {
          res_dr[j] <- sample(DRIBBLE_RANGES[-1], 1L)  # exclude 0 Dribbles
        } else {
          res_dr[j] <- sample(DRIBBLE_RANGES, 1L)
        }

        if (fga_meta$is_contact[j] == 1L) {
          res_dd[j] <- "0-2 Feet - Very Tight"
        } else {
          res_dd[j] <- sample(DEF_DIST_RANGES, 1L)
        }
        res_assign[j] <- "uniform-constrained"
      }
    }
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # STAGE 2: WRITE RESULTS TO PBP
  # ═══════════════════════════════════════════════════════════════════════════

  for (j in seq_len(n_fga)) {
    i  <- fga_meta$row_idx[j]
    dr <- res_dr[j]
    dd <- res_dd[j]

    pbp$imputed_dribble_range[i]  <- dr
    pbp$imputed_def_dist_range[i] <- dd
    pbp$is_catch_shoot[i]         <- as.integer(!is.na(dr) && dr == "0 Dribbles")
    pbp$p_catch_shoot[i]          <- res_p_cs[j]

    if (fga_meta$is_contact[j] == 1L) {
      pbp$expected_def_dist[i] <- runif(1, 0.0, 1.0)
    } else if (!is.na(dd)) {
      bounds <- bin_defs[[dd]]
      pbp$expected_def_dist[i] <- runif(1, bounds[1], bounds[2])
    }
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # VERBOSE SUMMARY
  # ═══════════════════════════════════════════════════════════════════════════

  if (verbose) {
    fga <- pbp[fga_idx, ]

    src_tab <- table(res_src, useNA = "ifany")
    message("    Tracking lookup source (shots):  ",
            paste(names(src_tab), src_tab, sep = "=", collapse = "  "))

    assign_tab <- table(res_assign, useNA = "ifany")
    message("    Assignment method (shots):  ",
            paste(names(assign_tab), assign_tab, sep = "=", collapse = "  "))

    # Shot-clock key distribution
    sc_type <- ifelse(fga_meta$sc_key == "ALL", "no-sc",
               ifelse(grepl("||", fga_meta$sc_key, fixed = TRUE), "blend", "single"))
    sc_type_tab <- table(sc_type)
    message("    Shot-clock key type (shots):  ",
            paste(names(sc_type_tab), sc_type_tab, sep = "=", collapse = "  "))

    if (has_general_range) {
      message("    Distance-specific resolved: ", n_dist_specific,
              "  combined-fallback: ", n_dist_fallback)
    }

    message("    Pre-sampling overrides:  descriptor-non-cs=", n_desc_override,
            "  contact-0-2ft=", n_contact_override,
            "  force-zero-drib=", n_force_zero)

    for (fam in c("rim", "j2", "j3")) {
      sub <- fga %>% filter(shot_family == fam)
      if (nrow(sub) > 0) {
        message(sprintf(
          "      %-3s: n=%-4d  mean_def_dist=%4.2f  contact=%d  cs_rate=%5.3f  p_cs=%5.3f",
          fam, nrow(sub),
          mean(sub$expected_def_dist,  na.rm = TRUE),
          sum(sub$is_contact_shot == 1, na.rm = TRUE),
          mean(sub$is_catch_shoot,     na.rm = TRUE),
          mean(sub$p_catch_shoot,      na.rm = TRUE)
        ))
      }
    }

    exact_groups <- sum(tapply(res_assign, group_keys,
                               function(x) all(x == "constrained")), na.rm = TRUE)
    total_groups <- length(unique_groups)
    message("    Constrained groups: ", exact_groups, "/", total_groups,
            " (", round(100 * exact_groups / max(total_groups, 1), 1), "%)")

    dr_tab <- table(fga$imputed_dribble_range, useNA = "ifany")
    message("    Dribble dist: ",
            paste(names(dr_tab), dr_tab, sep = "=", collapse = "  "))

    dd_tab <- table(fga$imputed_def_dist_range, useNA = "ifany")
    message("    Def-dist dist: ",
            paste(names(dd_tab), dd_tab, sep = "=", collapse = "  "))
  }

  pbp
}


assemble_and_scale <- function(pbp, schedule, bio, game_tracking, seed = 42) {
  set.seed(seed)
  
  # Ensure join keys are character (bio$PLAYER_ID is character; pbp IDs can be numeric)
  pbp <- pbp %>%
    mutate(player1_id     = as.character(player1_id),
           imputed_def_id = as.character(imputed_def_id))

  pbp <- pbp %>%
    left_join(schedule %>% select(game_id, game_date, home_team_id,
                                  home_team_tricode, away_team_id,
                                  away_team_tricode, season),
              by = "game_id")

  pbp <- pbp %>%
    mutate(def_team_abbr = case_when(
      as.character(team_id) == as.character(home_team_id) ~ away_team_tricode,
      as.character(team_id) == as.character(away_team_id) ~ home_team_tricode,
      TRUE ~ NA_character_))

  # Shooter biometrics
  pbp <- pbp %>%
    left_join(bio %>% select(PLAYER_ID, 
                             player_name_full = PLAYER_NAME,
                             shooter_ht = PLAYER_HEIGHT_INCHES,
                             shooter_ws = wingspan),
              by = c("player1_id" = "PLAYER_ID"))

  # Defender biometrics (using imputed defender)
  pbp <- pbp %>%
    left_join(bio %>% select(PLAYER_ID, 
                             imputed_defender_name = PLAYER_NAME,
                             defender_ht = PLAYER_HEIGHT_INCHES,
                             defender_ws = wingspan),
              by = c("imputed_def_id" = "PLAYER_ID"))

  league_avg_ht <- median(bio$PLAYER_HEIGHT_INCHES, na.rm = TRUE)
  pbp <- pbp %>%
    mutate(shooter_ht     = coalesce(shooter_ht, league_avg_ht),
           defender_ht    = coalesce(defender_ht, league_avg_ht))#,
           #height_matchup = shooter_ht - defender_ht)


  # Tracking aggregates
  # per-shot context imputation from game-level joint distributions
  # (game_date is now available in pbp from the schedule join above)
  pbp <- impute_shot_context(pbp, game_tracking, seed = seed)

  # ── Unified player index mapping ──
  # Build a single mapping from player_id → integer index, used for BOTH
  # shooter and defender. This ensures that if Tatum is player_idx=1 as a
  # shooter, he is also defender_idx=1 when imputed as closest defender.
  # Stan doesn't care — a_player[k] and a_defender[k] are separate parameter
  # vectors that happen to share the same indexing. But shared mapping gives
  # us one lookup table and easy cross-referencing of offensive/defensive effects.
  all_player_ids <- unique(c(
    pbp$player1_id[pbp$fga == 1 & pbp$player1_id != 0],
    pbp$imputed_def_id[pbp$fga == 1 & !is.na(pbp$imputed_def_id)]
  ))
  all_player_ids <- sort(all_player_ids[!is.na(all_player_ids)])
  pid_to_idx <- setNames(seq_along(all_player_ids), all_player_ids)

  pbp <- pbp %>%
    mutate(player_idx   = pid_to_idx[as.character(player1_id)],
           defender_idx = pid_to_idx[as.character(imputed_def_id)],
           defteam_idx  = as.integer(factor(def_team_abbr)),
           shooter_ht = case_when(!is.na(player_idx) ~ shooter_ht,
                                  TRUE ~ NA),
           shooter_ws = case_when(!is.na(player_idx) ~ shooter_ws,
                                  TRUE ~ NA),
           defender_ht = case_when(!is.na(defender_idx) ~ defender_ht,
                                  TRUE ~ NA),
           defender_ws = case_when(!is.na(defender_idx) ~ defender_ws,
                                  TRUE ~ NA))

  # Scale continuous features within shot family.
  # NOTE: Scaling is NOT required for XGBoost (tree splits are invariant to
  # monotonic transformations). It IS required for Stan/MCMC and useful for
  # diagnostic plots. Retained here for both purposes; the XGBoost script
  # uses raw (unscaled) features directly.
  #
  # New v3 categorical columns (imputed_dribble_range, imputed_def_dist_range)
  # are left as character — XGBoost will one-hot encode them; Stan will need
  # integer-coded versions built in the model prep script.
  safe_scale <- function(x) {
    s <- sd(x, na.rm = TRUE)
    m <- mean(x, na.rm = TRUE)
    if (is.na(s) || s < 1e-6) return(x - m)   # center only if no variance
    (x - m) / s
  }

  pbp <- pbp %>%
    group_by(shot_family) %>%
    mutate(D               = safe_scale(shot_distance),
           A               = safe_scale(shot_angle),
           T_scaled        = safe_scale(case_when(!is.na(shot_clock) ~ shot_clock, 
                                                  TRUE ~ game_clock_sec)),
           def_dist_scaled = safe_scale(expected_def_dist)) %>%
    ungroup()# %>%
    #mutate(ht_match_scaled = safe_scale(height_matchup))

  # Scaling params (for prediction grids later)
  fga_rows <- pbp %>% filter(fga == 1, !is.na(shot_family))
  scaling_params <- fga_rows %>%
    group_by(shot_family) %>%
    summarise(mu_dist = mean(shot_distance, na.rm = TRUE),
              sd_dist = sd(shot_distance, na.rm = TRUE),
              mu_ang  = mean(shot_angle, na.rm = TRUE),
              sd_ang  = sd(shot_angle, na.rm = TRUE),
              mu_T    = mean(sec_since_play_start, na.rm = TRUE),
              sd_T    = sd(sec_since_play_start, na.rm = TRUE),
              mu_defdist = mean(expected_def_dist, na.rm = TRUE),
              sd_defdist = sd(expected_def_dist, na.rm = TRUE),
              .groups = "drop")

  attr(pbp, "scaling_params") <- scaling_params
  #attr(pbp, "ht_match_mu")    <- mean(fga_rows$height_matchup, na.rm = TRUE)
  #attr(pbp, "ht_match_sd")    <- sd(fga_rows$height_matchup, na.rm = TRUE)

  # Player map includes ALL players in the unified index (shooters + defenders)
  # This is the single source of truth for player_idx ↔ player_id mapping.
  player_map <- tibble(
    player_idx = as.integer(pid_to_idx),
    player_id  = names(pid_to_idx)
  ) %>% arrange(player_idx)

  # Try to attach names from PBP (shooters have player_name, defenders may not)
  # name_lookup <- fga_rows %>%
  #   distinct(player1_id, player_name) %>%
  #   filter(!is.na(player_name))
  # Try to attach names (prefer full name from bio; fall back to PBP)
  name_lookup <- fga_rows %>%
    distinct(player1_id, player_name_full, player_name) %>%
    mutate(name = coalesce(player_name_full, player_name)) %>%
    select(player1_id, name) %>%
    filter(!is.na(name))
  
  player_map <- player_map %>%
    left_join(name_lookup, by = c("player_id" = "player1_id"))
  
  # player_map <- player_map %>%
  #   left_join(name_lookup, by = c("player_id" = "player1_id")) %>%
  #   rename(name = player_name)

  attr(pbp, "player_map") <- player_map
  attr(pbp, "defteam_map")    <- fga_rows %>%
    filter(!is.na(defteam_idx)) %>%
    distinct(defteam_idx, def_team_abbr) %>% arrange(defteam_idx)

  pbp
}





## ═════════════════════════════════════════════════════════════════════════════
## 09 - MASTER FUNCTION   ======================================================
## ═════════════════════════════════════════════════════════════════════════════

build_season_pbp <- function(season_year, game_ids = NULL, cache_dir = "cache",
                             save_maps = TRUE) {
  #
  # Args:
  #   season_year : integer, e.g. 2024 for the 2024-25 season
  #   game_ids    : character vector of specific game IDs to process,
  #                 or NULL for the full season.
  #   cache_dir   : directory for intermediate RDS caches
  #
  # Returns: full PBP data frame (invisibly). Also saves CSVs.
  #

  season_str <- year_to_season(season_year)
  schedule <- get_schedule(season_year)
  is_test    <- !is.null(game_ids)

  ## ── 1. Schedule ──────────────────────────────────────────────────────────
  message("─── Step 1: Schedule ───")
  message("  Full season: ", nrow(schedule), " games")

  if (is_test) {
    missing <- setdiff(game_ids, schedule$game_id)
    if (length(missing) > 0) {
      message("  WARNING: ", length(missing), " game_id(s) not found in schedule: ",
              paste(missing[1:min(3, length(missing))], collapse = ", "))
    }
    schedule_subset <- schedule %>% filter(game_id %in% game_ids)
    message("  Test subset: ", nrow(schedule_subset), " games")
  } else {
    schedule_subset <- schedule
  }

  # ── Build date-range tag from the actual dates being processed ──────────
  # e.g. "20241022_20250413" for a full season, "20241031_20241105" for a chunk
  subset_dates <- sort(unique(as.Date(schedule_subset$game_date)))
  date_tag     <- paste0(format(min(subset_dates), "%Y%m%d"), "_",
                         format(max(subset_dates), "%Y%m%d"))
  tag          <- date_tag

  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  cp <- function(name) file.path(cache_dir, paste0(name, "_", 
                                                   season_year, "_", 
                                                   season_year + 1, "/", 
                                                   name, "_", 
                                                   tag, ".rds"))

  message("\n══════════════════════════════════════════════════════")
  if (is_test) {
    message("  RUN: ", length(game_ids), " game(s) from ", season_str,
            "  [", tag, "]")
  } else {
    message("  FULL SEASON: ", season_str, "  [", tag, "]")
  }
  message("══════════════════════════════════════════════════════\n")


  ## ── 2. PBP with lineups ─────────────────────────────────────────────────
  message("─── Step 2: PBP with lineups ───")
  if (file.exists(cp("pbp_raw"))) {
    message("  Loading cached raw PBP…")
    pbp_raw <- readRDS(cp("pbp_raw"))
  } else {
    message("  Scraping PBP for ", nrow(schedule_subset), " game(s)…")
    pbp_raw <- map(schedule_subset$game_id, function(gid) {
      tryCatch(get_game_with_lineups(gid), error = function(e) {
        message("    Error on ", gid, ": ", e$message); NULL
      })
    }, .progress = !is_test) %>% bind_rows()
    saveRDS(pbp_raw, cp("pbp_raw"))
  }
  #saveRDS(pbp_raw, cp("pbp_raw"))
  message("  Raw PBP rows: ", nrow(pbp_raw),
          "  |  Games: ", n_distinct(pbp_raw$game_id))


  ## ── 3. Feature engineering ──────────────────────────────────────────────
  message("─── Step 3: Feature engineering ───")
  pbp <- engineer_features(pbp_raw)
  n_fga <- sum(pbp$fga == 1, na.rm = TRUE)
  message("  FGA: ", n_fga,
          "  |  rim=", sum(pbp$shot_family == "rim", na.rm = TRUE),
          "  j2=", sum(pbp$shot_family == "j2", na.rm = TRUE),
          "  j3=", sum(pbp$shot_family == "j3", na.rm = TRUE))


  ## ── 5. Foul linkage ─────────────────────────────────────────────────────
  message("─── Step 5: Shooting foul linkage ───")
  pbp <- link_shooting_fouls(pbp)
  nf <- attr(pbp, "non_fga_foul_trips")
  message("  And-1s: ", sum(pbp$drew_and1, na.rm = TRUE),
          "  |  Non-FGA foul trips: ", if (!is.null(nf)) nrow(nf) else 0,
          "  |  Contact shots: ", sum(pbp$is_contact_shot, na.rm = TRUE))


  ## ── 5b. Fatigue features ───────────────────────────────────────────────
  message("─── Step 5b: Fatigue features ───")
  pbp <- compute_fatigue_features(pbp)


  ## ── 6. Matchup data + defender imputation ───────────────────────────────
  message("─── Step 6: Matchup data & defender imputation ───")
  if (file.exists(cp("matchups"))) {
    message("  Loading cached matchups…")
    matchups <- readRDS(cp("matchups"))
  } else {
    gids_to_scrape <- unique(pbp$game_id)
    message("  Scraping matchups for ", length(gids_to_scrape), " game(s)…")
    matchups <- map(gids_to_scrape, pull_game_matchups,
                    .progress = !is_test) %>% bind_rows()
    saveRDS(matchups, cp("matchups"))
  }
  message("  Matchup rows: ", nrow(matchups))

  pbp <- impute_closest_defender(pbp, matchups, schedule_subset)

  coverage <- round(100 * mean(!is.na(pbp$imputed_def_id[pbp$fga == 1])), 1)
  message("  Overall defender coverage: ", coverage, "%")


  ## ── 7. Biometrics ──────────────────────────────────────────────────────
  # Biometrics are season-wide (not game-specific), so use season-level cache
  message("─── Step 7: Biometrics ───")
  bio_cache <- file.path(cache_dir, "bio.rds")
  if (file.exists(bio_cache)) {
    message("  Loading cached biometrics…")
    bio <- readRDS(bio_cache)
  } else {
    bio <- get_biometrics(season_str, season_year)
    saveRDS(bio, bio_cache)
  }
  message("  Players with height: ", nrow(bio))


  ## ── 8. Tracking aggregates (game-date level) ───────────────────────────
  # Now scraped at the game-date level and cached per-date.
  # No single season cache file — each date is independently cached in
  # cache/trk_dates_<season>/ so interrupted scrapes resume from where they
  # left off.  Pass schedule_subset so test runs only scrape relevant dates.
  message("─── Step 8: Tracking aggregates (game-date level) ───")
  game_tracking <- get_game_level_tracking(schedule_subset, season_str, cache_dir)
  message("  Tracking rows: ", nrow(game_tracking),
          "  |  players: ", n_distinct(game_tracking$PLAYER_ID),
          "  |  dates: ", n_distinct(game_tracking$game_date))

  ## ── 9. Assembly + scaling ──────────────────────────────────────────────
  message("─── Step 9: Assembly & scaling ───")
  pbp <- assemble_and_scale(pbp, schedule_subset, bio, game_tracking, seed = 42)

  scaling_params <- attr(pbp, "scaling_params")
  player_map     <- attr(pbp, "player_map")
  defteam_map    <- attr(pbp, "defteam_map")
  non_fga_fouls  <- attr(pbp, "non_fga_foul_trips")


  ## ── 10. Save ───────────────────────────────────────────────────────────
  message("─── Step 10: Saving outputs ───")

  # write_csv(pbp %>% select(-any_of("on_court_defenders") %>% 
  #                            group_by(game_id) %>%
  #                            arrange(order, event_num, .by_group = TRUE) %>%
  #                            ungroup()),
  #           paste0("pbp_", tag, ".csv"))
  write_csv(pbp %>% select(-any_of("on_court_defenders")) %>%
                             group_by(game_id) %>%
                             arrange(order, event_num, .by_group = TRUE) %>%
                             ungroup(),
            paste0("pbp_", season_year, "_", season_year + 1, 
                   "/pbp_", tag, ".csv"))
  
  # v9: Per-chunk maps/scaling are only saved in standalone (non-chunked) mode.
  # In chunked mode, season-level maps are built after all chunks are combined.
  if (save_maps) {
    write_csv(scaling_params, paste0("scaling_params/scaling_params_", tag, ".csv"))
    write_csv(player_map,     paste0("player_mapping/player_map_", tag, ".csv"))
    write_csv(defteam_map,    paste0("team_mapping/defteam_map_", tag, ".csv"))
  }
  if (!is.null(non_fga_fouls) && nrow(non_fga_fouls) > 0)
    write_csv(non_fga_fouls, paste0("non_fga_fouls_", tag, ".csv"))


  ## ── Summary ────────────────────────────────────────────────────────────
  fga <- pbp %>% filter(fga == 1, !is.na(shot_family))

  message("\n═══ DATASET SUMMARY ═══")
  message("Tag:                ", tag)
  message("Season:             ", season_str)
  message("Games:              ", n_distinct(pbp$game_id))
  message("Total PBP events:   ", nrow(pbp))
  message("Total FGA:          ", nrow(fga))
  message("Players:            ", n_distinct(fga$player_idx))
  message("Def teams:          ", n_distinct(fga$defteam_idx))
  message("Shot families:      rim=", sum(fga$shot_family == "rim"),
          "  j2=", sum(fga$shot_family == "j2"),
          "  j3=", sum(fga$shot_family == "j3"))
  message("Second chance:      ", sum(fga$is_2ndchance),
          " (", round(100 * mean(fga$is_2ndchance), 1), "%)")
  message("Fastbreak:          ", sum(fga$is_fastbreak),
          " (", round(100 * mean(fga$is_fastbreak), 1), "%)")
  message("From turnover:      ", sum(fga$is_fromturnover),
          " (", round(100 * mean(fga$is_fromturnover), 1), "%)")
  message("And-1s drawn:       ", sum(fga$drew_and1, na.rm = TRUE))
  message("Contact shots:      ", sum(fga$is_contact_shot, na.rm = TRUE),
          " (", round(100 * mean(fga$is_contact_shot, na.rm = TRUE), 1), "%)")
  message("C&S rate (jumpers): ",
          round(100 * mean(fga$is_catch_shoot[fga$is_jump == 1],
                           na.rm = TRUE), 1), "%")

  # v3: dribble and def-dist distributions
  if ("imputed_dribble_range" %in% names(fga)) {
    dr_tab <- table(fga$imputed_dribble_range, useNA = "ifany")
    message("Dribble dist:       ",
            paste(names(dr_tab), dr_tab, sep = "=", collapse = "  "))
  }
  if ("imputed_def_dist_range" %in% names(fga)) {
    dd_tab <- table(fga$imputed_def_dist_range, useNA = "ifany")
    message("Def-dist dist:      ",
            paste(names(dd_tab), dd_tab, sep = "=", collapse = "  "))
  }
  message("Median game min:    ",
          round(median(fga$shooter_min_game, na.rm = TRUE), 1))
  message("Median stint min:   ",
          round(median(fga$shooter_min_stint, na.rm = TRUE), 1))

  # Defender source breakdown
  def_src <- table(fga$def_source, useNA = "ifany")
  message("Defender sources:   ", paste(names(def_src), def_src,
                                         sep = "=", collapse = "  "))

  message("\nSaved:  pbp_", tag, ".csv  +  scaling/player/defteam/foul CSVs")
  message("\n─── Pipeline complete ───\n")

  invisible(pbp)
}


## ═════════════════════════════════════════════════════════════════════════════
## 10 - CHUNKED SEASON BUILDER   ===============================================
## ═════════════════════════════════════════════════════════════════════════════
##
## build_season_pbp_inChunks(season_year, chunk_size, cache_dir)
##
## Scrapes a full season in chunks of `chunk_size` game dates at a time.
## After each chunk completes, the output CSV is saved immediately with a
## date-range filename (e.g. pbp_20241022_20241027.csv).  If the run crashes
## or times out mid-season, all previously completed chunks are safely on disk.
##
## The function checks for existing output CSVs before scraping each chunk,
## so re-running after a crash skips already-completed chunks automatically.
##
## v9: Per-chunk maps/scaling are NO LONGER saved.  After all chunks are
## complete, the PBP CSVs are rbind'd and a single set of season-level
## player_map, defteam_map, and scaling_params is computed and saved.
## player_idx, defender_idx, and defteam_idx are reassigned consistently
## across the entire season.
##
## Multi-season support:
##   existing_player_map — optional data frame with columns (player_idx,
##     player_id) from a previous season.  New players (rookies, etc.) are
##     appended with indices starting after max(existing_player_map$player_idx).
##     This ensures consistent player indexing across multiple seasons.
##
## Usage:
##   build_season_pbp_inChunks(2024, 5)    # 2024-25 season, 5 game-dates per chunk
##   build_season_pbp_inChunks(2024, 10)   # larger chunks = fewer CSVs, less overhead
##   # Multi-season: pass the player map from a previous season
##   prev_map <- read_csv("player_mapping/player_map_2024_2025.csv")
##   build_season_pbp_inChunks(2025, 5, existing_player_map = prev_map)

build_season_pbp_inChunks <- function(season_year, chunk_size = 5,
                                      cache_dir = "cache",
                                      existing_player_map = NULL,
                                      existing_defteam_map = NULL) {

  season_str <- year_to_season(season_year)
  message("\n══════════════════════════════════════════════════════")
  message("  CHUNKED BUILD: ", season_str, 
          "  |  ", chunk_size, 
          " game-date(s) per chunk")
  message("══════════════════════════════════════════════════════\n")

  # Get full schedule once (shared across all chunks)
  schedule <- get_schedule(season_year)
  message("  Full season: ", nrow(schedule), " games")

  # Unique sorted game dates
  all_dates  <- sort(unique(as.Date(schedule$game_date)))
  n_dates    <- length(all_dates)
  n_chunks   <- ceiling(n_dates / chunk_size)
  message("  Game dates: ", n_dates,
          "  →  ", n_chunks, " chunk(s) of ≤", chunk_size, " dates each\n")

  all_chunk_pbps <- vector("list", n_chunks)

  for (ch in seq_len(n_chunks)) {

    # Date range for this chunk
    i_start    <- (ch - 1L) * chunk_size + 1L
    i_end      <- min(ch * chunk_size, n_dates)
    chunk_dates <- all_dates[i_start:i_end]

    date_tag <- paste0(format(min(chunk_dates), "%Y%m%d"), "_",
                       format(max(chunk_dates), "%Y%m%d"))

    # Check if this chunk's output CSV already exists → skip if so
    csv_path <- paste0("pbp_", season_year, "_", season_year+1, 
                       "/pbp_", date_tag, ".csv")
    if (file.exists(csv_path)) {
      message("═══ Chunk ", ch, "/", n_chunks, " [", date_tag, "] — ",
              "CSV exists, skipping\n")
      next
    }

    message("═══ Chunk ", ch, "/", n_chunks, " [", date_tag, "] — ",
            length(chunk_dates), " date(s) ═══")

    # Get game_ids for this chunk's dates
    chunk_game_ids <- schedule %>%
      filter(as.Date(game_date) %in% chunk_dates) %>%
      pull(game_id)

    message("  Games in chunk: ", length(chunk_game_ids))

    # Run the full pipeline for this chunk (save_maps = FALSE: skip per-chunk maps)
    chunk_pbp <- tryCatch(
      build_season_pbp(season_year,
                       game_ids  = chunk_game_ids,
                       cache_dir = cache_dir,
                       save_maps = FALSE),
      error = function(e) {
        message("\n  ⚠ Chunk ", ch, " [", date_tag, "] FAILED: ", e$message)
        message("  Continuing to next chunk…\n")
        NULL
      }
    )

    if (!is.null(chunk_pbp)) {
      all_chunk_pbps[[ch]] <- chunk_pbp
    }

    message("")
  }

  # Summary
  completed <- sum(!vapply(all_chunk_pbps, is.null, logical(1L)))
  message("\n══════════════════════════════════════════════════════")
  message("  CHUNKED BUILD COMPLETE: ", completed, "/", n_chunks, " chunks saved")
  message("══════════════════════════════════════════════════════\n")


  # ═══════════════════════════════════════════════════════════════════════════
  # SEASON-LEVEL CONSOLIDATION
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Rbind all chunk CSVs, reassign idx columns consistently across the
  # entire season, and save a single set of maps + scaling params.

  message("\n─── Season-level consolidation ───")

  pbp_dir <- paste0("pbp_", season_year, "_", season_year + 1)
  csv_files <- sort(list.files(pbp_dir, pattern = "^pbp_\\d{8}_\\d{8}\\.csv$",
                                full.names = TRUE))
  if (length(csv_files) == 0) {
    message("  No chunk CSVs found in ", pbp_dir, " — skipping consolidation")
    return(invisible(all_chunk_pbps))
  }

  message("  Reading ", length(csv_files), " chunk CSVs…")
  pbp_all <- bind_rows(lapply(csv_files, read_csv, show_col_types = FALSE))
  message("  Combined PBP rows: ", nrow(pbp_all),
          "  |  Games: ", n_distinct(pbp_all$game_id))

  # ── Build unified player index mapping ────────────────────────────────
  # Collect ALL player_ids (shooters + defenders) across the full season.
  fga_mask <- pbp_all$fga == 1
  all_player_ids <- unique(c(
    pbp_all$player1_id[fga_mask & pbp_all$player1_id != 0],
    pbp_all$imputed_def_id[fga_mask & !is.na(pbp_all$imputed_def_id)]
  ))
  all_player_ids <- sort(all_player_ids[!is.na(all_player_ids)])

  if (!is.null(existing_player_map) && nrow(existing_player_map) > 0) {
    # Multi-season: keep existing indices, append new players
    existing_ids  <- as.character(existing_player_map$player_id)
    max_existing  <- max(existing_player_map$player_idx, na.rm = TRUE)
    new_ids       <- setdiff(as.character(all_player_ids), existing_ids)
    if (length(new_ids) > 0) {
      message("  Multi-season: ", length(existing_ids), " existing players + ",
              length(new_ids), " new players")
    }
    pid_to_idx <- setNames(
      c(existing_player_map$player_idx, 
        seq_along(new_ids) + max_existing),
      c(existing_ids, new_ids)
    )
  } else {
    pid_to_idx <- setNames(seq_along(all_player_ids),
                            as.character(all_player_ids))
  }

  # ── Build unified defteam index mapping ───────────────────────────────
  all_defteams <- sort(unique(pbp_all$def_team_abbr[fga_mask &
                                                      !is.na(pbp_all$def_team_abbr)]))

  if (!is.null(existing_defteam_map) && nrow(existing_defteam_map) > 0) {
    existing_teams <- as.character(existing_defteam_map$def_team_abbr)
    max_existing_t <- max(existing_defteam_map$defteam_idx, na.rm = TRUE)
    new_teams      <- setdiff(all_defteams, existing_teams)
    defteam_to_idx <- setNames(
      c(existing_defteam_map$defteam_idx,
        seq_along(new_teams) + max_existing_t),
      c(existing_teams, new_teams)
    )
  } else {
    defteam_to_idx <- setNames(seq_along(all_defteams), all_defteams)
  }

  # ── Reassign idx columns ──────────────────────────────────────────────
  pbp_all <- pbp_all %>%
    mutate(player_idx   = pid_to_idx[as.character(player1_id)],
           defender_idx = pid_to_idx[as.character(imputed_def_id)],
           defteam_idx  = defteam_to_idx[as.character(def_team_abbr)])

  message("  Players (unified): ", length(pid_to_idx),
          "  |  Def teams: ", length(defteam_to_idx))

  # ── Recompute scaling params from full season ─────────────────────────
  safe_scale_params <- function(x) {
    s <- sd(x, na.rm = TRUE)
    m <- mean(x, na.rm = TRUE)
    list(mu = m, sd = if (is.na(s) || s < 1e-6) NA_real_ else s)
  }

  fga_rows <- pbp_all %>% filter(fga == 1, !is.na(shot_family))
  scaling_params <- fga_rows %>%
    group_by(shot_family) %>%
    summarise(mu_dist    = mean(shot_distance, na.rm = TRUE),
              sd_dist    = sd(shot_distance, na.rm = TRUE),
              mu_ang     = mean(shot_angle, na.rm = TRUE),
              sd_ang     = sd(shot_angle, na.rm = TRUE),
              mu_T       = mean(sec_since_play_start, na.rm = TRUE),
              sd_T       = sd(sec_since_play_start, na.rm = TRUE),
              mu_defdist = mean(expected_def_dist, na.rm = TRUE),
              sd_defdist = sd(expected_def_dist, na.rm = TRUE),
              .groups    = "drop")

  # ── Player map ────────────────────────────────────────────────────────
  player_map <- tibble(
    player_idx = as.integer(pid_to_idx),
    player_id  = names(pid_to_idx)
  ) %>% arrange(player_idx)

  # name_lookup <- fga_rows %>%
  #   distinct(player1_id, player_name) %>%
  #   filter(!is.na(player_name)) %>%
  #   mutate(player1_id = as.character(player1_id))
  # 
  # player_map <- player_map %>%
  #   left_join(name_lookup, by = c("player_id" = "player1_id")) %>%
  #   rename(name = player_name)
  
  # Prefer full name column created from bio.rds when present; fall back to PBP name.
  name_lookup <- fga_rows %>%
    distinct(player1_id, player_name_full, player_name) %>%
    mutate(name = coalesce(player_name_full, player_name),
           player1_id = as.character(player1_id)) %>%
    select(player1_id, name) %>%
    filter(!is.na(name))
  
  player_map <- player_map %>%
    left_join(name_lookup, by = c("player_id" = "player1_id"))

  # ── Defteam map ───────────────────────────────────────────────────────
  defteam_map <- tibble(
    defteam_idx   = as.integer(defteam_to_idx),
    def_team_abbr = names(defteam_to_idx)
  ) %>% arrange(defteam_idx)

  # ── Re-scale continuous features with season-level params ─────────────
  safe_scale <- function(x) {
    s <- sd(x, na.rm = TRUE)
    m <- mean(x, na.rm = TRUE)
    if (is.na(s) || s < 1e-6) return(x - m)
    (x - m) / s
  }
  # pbp_all <- pbp_all %>%
  #   group_by(shot_family) %>%
  #   mutate(D               = safe_scale(shot_distance),
  #          A               = safe_scale(shot_angle),
  #          T_scaled        = safe_scale(case_when(!is.na(shot_clock) ~ shot_clock,
  #                                                  TRUE ~ game_clock_sec)),
  #          def_dist_scaled = safe_scale(expected_def_dist)) %>%
  #   ungroup()

  # ── Save season-level outputs ─────────────────────────────────────────
  season_tag <- paste0(season_year, "_", season_year + 1)

  dir.create("scaling_params",  showWarnings = FALSE, recursive = TRUE)
  dir.create("player_mapping",  showWarnings = FALSE, recursive = TRUE)
  dir.create("team_mapping",    showWarnings = FALSE, recursive = TRUE)

  write_csv(pbp_all %>%
              select(-any_of(c("x", "y", "D", "A", "T_scaled", "def_dist_scaled"))) %>%
              relocate(shot_angle, .after = shot_distance) %>% 
              group_by(game_id) %>%
              arrange(order, event_num, .by_group = TRUE) %>%
              ungroup() %>% 
              arrange(game_date, game_id, order) %>% 
              as.data.frame(),
            paste0(pbp_dir, "/pbp_", season_tag, "_FULL.csv"))

  write_csv(scaling_params, paste0("scaling_params/scaling_params_", season_tag, ".csv"))
  write_csv(player_map,     paste0("player_mapping/player_map_", season_tag, ".csv"))
  write_csv(defteam_map,    paste0("team_mapping/defteam_map_", season_tag, ".csv"))

  message("\n  Saved season-level files:")
  message("    pbp_", season_tag, "_FULL.csv  (",
          nrow(pbp_all), " rows, ",
          n_distinct(pbp_all$game_id), " games)")
  message("    scaling_params_", season_tag, ".csv")
  message("    player_map_", season_tag, ".csv  (",
          nrow(player_map), " players)")
  message("    defteam_map_", season_tag, ".csv  (",
          nrow(defteam_map), " teams)")

  message("\n══════════════════════════════════════════════════════")
  message("  SEASON CONSOLIDATION COMPLETE")
  message("══════════════════════════════════════════════════════\n")

  invisible(list(pbp = pbp_all,
                 player_map = player_map,
                 defteam_map = defteam_map,
                 scaling_params = scaling_params))
}


## ═════════════════════════════════════════════════════════════════════════════
## 11 - MULTI-SEASON BUILDER   =================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## build_multi_season_pbp(season_years, chunk_size, cache_dir)
##
## Processes multiple seasons sequentially, carrying forward the player and
## team index maps so that indices are consistent across all seasons.
## A player who appears in any season gets a single permanent index.
##
## Returns a list with:
##   - player_map   : the cumulative player map across all seasons
##   - defteam_map  : the cumulative team map across all seasons
##   - season_results : per-season results from build_season_pbp_inChunks

build_multi_season_pbp <- function(season_years, chunk_size = 5,
                                   cache_dir = "cache") {

  cumulative_player_map  <- NULL
  cumulative_defteam_map <- NULL
  season_results <- vector("list", length(season_years))
  names(season_results) <- as.character(season_years)

  for (sy in season_years) {
    message("\n", strrep("═", 60))
    message("  MULTI-SEASON: Starting ", sy, "-", sy + 1)
    message(strrep("═", 60), "\n")

    result <- build_season_pbp_inChunks(
      season_year         = sy,
      chunk_size          = chunk_size,
      cache_dir           = cache_dir,
      existing_player_map = cumulative_player_map,
      existing_defteam_map = cumulative_defteam_map
    )

    # Update cumulative maps from this season's output
    if (!is.null(result) && is.list(result)) {
      if (!is.null(result$player_map))  cumulative_player_map  <- result$player_map
      if (!is.null(result$defteam_map)) cumulative_defteam_map <- result$defteam_map
      season_results[[as.character(sy)]] <- result
    }
  }

  # Save cumulative maps if multi-season
  if (length(season_years) > 1L && !is.null(cumulative_player_map)) {
    tag_multi <- paste0(min(season_years), "_", max(season_years) + 1)
    dir.create("player_mapping", showWarnings = FALSE, recursive = TRUE)
    dir.create("team_mapping",   showWarnings = FALSE, recursive = TRUE)
    write_csv(cumulative_player_map,
              paste0("player_mapping/player_map_multi_", tag_multi, ".csv"))
    write_csv(cumulative_defteam_map,
              paste0("team_mapping/defteam_map_multi_", tag_multi, ".csv"))
    message("\n  Saved cumulative multi-season maps: ", tag_multi)
  }

  invisible(list(player_map      = cumulative_player_map,
                 defteam_map     = cumulative_defteam_map,
                 season_results  = season_results))
}





## ═════════════════════════════════════════════════════════════════════════════
## 12 - USAGE   ================================================================
## ═════════════════════════════════════════════════════════════════════════════

## ── Test with 1 game ──
# schedule <- get_schedule(2024)
# test1 <- build_season_pbp(2024, game_ids = schedule$game_id[1])

## ── Test with first 5 games ──
# test5 <- build_season_pbp(2024, game_ids = schedule$game_id[1:5])

## ── Full season in chunks of 5 game-dates ──
build_season_pbp_inChunks(2024, 5)

## ── Full season in one go ──
# pbp_2024 <- build_season_pbp(2024)

## ── Multi-season (consistent indices across seasons) ──
# multi <- build_multi_season_pbp(c(2023, 2024), chunk_size = 5)

get_lt10_tracking(schedule_24, "2024-25")










# Step 1: identify the missing game_ids
schedule_24 <- get_schedule(2024)
pbp_all <- read_csv("pbp_2024_2025/pbp_2024_2025_FULL.csv") %>% 
  rename(xLoc = x_legacy,
         yLoc = y_legacy,
         event_team_id = team_id,
         event_team = team_tricode,
         steal_player = steal_player_name,
         steal_player_id = steal_person_id,
         assist_player = assist_player_name_initial,
         assist_player_id = assist_person_id,
         foul_drawn_player = foul_drawn_player_name,
         foul_drawn_player_id = foul_drawn_person_id,
         block_player = block_player_name,
         block_player_id = block_person_id,
         dribble_range = imputed_dribble_range,
         defender_dist_range = imputed_def_dist_range,
         player_min_game = shooter_min_game,
         player_min_stint = shooter_min_stint,
         player_height = shooter_ht,
         player_wingspan = shooter_ws,
         defender_height = defender_ht,
         defender_wingspan = defender_ws,
         defender_team = def_team_abbr,
         defender_id = imputed_def_id,
         defender_name = imputed_defender_name,
         home_team = home_team_tricode,
         away_team = away_team_tricode) %>% 
  select(-any_of(c("is_field_goal", "poss_start_is_liveball", "row_id",
                   "elapsed_seconds", "total_elapsed", "period_start",
                   "period_end", "possession_end", "side",
                   "shot_action_number", "is_last_freethrow",
                   "time_since_last_event", "sec_since_play_start",
                   "player_name_i", "person_ids_filter",
                   "player2_id", "player3_id", "p_catch_shoot", 
                   "expected_def_dist", "def_source"))) %>%
  select(-contains(c("jump_ball"))) %>% 
  relocate(season, .after = game_id) %>% 
  relocate(game_date, .after = season) %>% 
  relocate(order, .after = game_date) %>% 
  relocate(home_team_id, .after = order) %>% 
  relocate(home_team, .after = home_team_id) %>% 
  relocate(away_team_id, .after = home_team) %>% 
  relocate(away_team, .after = away_team_id) %>% 
  relocate(player_name_full, .after = player_name) %>% 
  relocate(event_num, .after = period_type) %>% 
  relocate(event_team_id, .after = event_num) %>% 
  relocate(event_team, .after = event_team_id) %>% 
  relocate(descriptor, .after = qualifiers) %>% 
  relocate(description, .after = descriptor) %>% 
  relocate(player_height, .after = player_name_full) %>% 
  relocate(player_wingspan, .after = player_height) %>% 
  relocate(player_min_game, .after = player_wingspan) %>% 
  relocate(player_min_stint, .after = player_min_game) %>% 
  relocate(defender_team, .after = player_min_stint) %>% 
  relocate(defteam_idx, .after = defender_team) %>% 
  relocate(defender_id, .after = defteam_idx) %>% 
  relocate(defender_name, .after = defender_id) %>% 
  relocate(defender_height, .after = defender_name) %>% 
  relocate(defender_wingspan, .after = defender_height) %>% 
  relocate(defender_dist_range, .after = defender_wingspan) %>% 
  as.data.frame()









## ─── Quick single-date test: LT10 retrofit (parallel) ───────────────────────
## This is the PRIMARY test — it reads an existing cache, scrapes LT10 only,
## merges and de-overlaps, then shows the result.
if (TRUE) {
  test_date_ymd <- "2024-10-22"
  test_date_mdy <- "10/22/2024"
  test_season   <- "2024-25"
  test_max_per  <- 4L
  test_workers  <- N_WORKERS
  cache_file    <- file.path("cache", "shot_tracking_2024_2025",
                             paste0(test_date_ymd, ".rds"))

  ## Step 1: Read existing cache (should have NO general_range column)
  existing <- readRDS(cache_file)
  cat("Existing cache rows:", nrow(existing), "\n")
  cat("Has general_range?", "general_range" %in% names(existing), "\n")

  ## Step 2: Scrape LT10 only (parallel)
  test_combos <- expand.grid(
    period               = seq_len(test_max_per),
    shot_clock_range     = SHOT_CLOCK_RANGES,
    dribble_range        = DRIBBLE_RANGES,
    close_def_dist_range = DEF_DIST_RANGES,
    stringsAsFactors     = FALSE,
    KEEP.OUT.ATTRS       = FALSE
  )
  test_combos$general_range <- "Less Than 10 ft"
  cat("LT10 combos:", nrow(test_combos), "\n")

  chunk_ids    <- sort(rep(seq_len(test_workers), length.out = nrow(test_combos)))
  chunks       <- split(test_combos, chunk_ids)

  cat("Launching", test_workers, "workers…\n")
  t0 <- Sys.time()
  cl <- parallel::makeCluster(test_workers)
  chunk_results <- parallel::parLapply(cl, chunks, .fetch_combo_batch,
                                       date_mdy   = test_date_mdy,
                                       season_str = test_season)
  parallel::stopCluster(cl)
  lt10_raw <- dplyr::bind_rows(chunk_results)
  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
  cat("LT10 raw rows:", nrow(lt10_raw), " |", elapsed, "s\n")

  ## Normalise LT10 columns to match existing schema
  lt10_df <- lt10_raw %>%
    dplyr::transmute(
      PLAYER_ID         = as.character(PLAYER_ID),
      period            = as.integer(period),
      shot_clock_range  = shot_clock_range,
      dribble_range     = dribble_range,
      def_dist_range    = def_dist_range,
      general_range     = general_range,
      fg2a              = as.integer(FG2A),
      fg3a              = as.integer(FG3A),
      fg2m              = as.integer(FG2M),
      fg3m              = as.integer(FG3M),
      fgm               = as.integer(FGM),
      fga               = as.integer(FGA)
    )
  lt10_df$game_date <- test_date_ymd

  ## Step 3: Tag existing as Overall, de-overlap
  existing$general_range <- ""

  join_cols <- c("PLAYER_ID", "game_date", "period",
                 "shot_clock_range", "dribble_range", "def_dist_range")

  complement <- existing %>%
    dplyr::left_join(lt10_df %>% dplyr::select(dplyr::all_of(join_cols),
                                  fg2a_lt = fg2a, fg3a_lt = fg3a,
                                  fg2m_lt = fg2m, fg3m_lt = fg3m,
                                  fgm_lt  = fgm,  fga_lt  = fga),
                     by = join_cols) %>%
    dplyr::mutate(dplyr::across(dplyr::ends_with("_lt"), ~ dplyr::coalesce(.x, 0L)),
                  fg2a = fg2a - fg2a_lt,
                  fg3a = fg3a - fg3a_lt,
                  fg2m = fg2m - fg2m_lt,
                  fg3m = fg3m - fg3m_lt,
                  fgm  = fgm  - fgm_lt,
                  fga  = fga  - fga_lt) %>%
    dplyr::select(-dplyr::ends_with("_lt")) %>%
    dplyr::filter(fga > 0L)

  test_results <- dplyr::bind_rows(lt10_df, complement)

  ## Step 4: Verify
  cat("\n=== De-overlapped results ===\n")
  cat("Total rows:", nrow(test_results), "\n")
  cat("general_range distribution:\n")
  print(table(test_results$general_range, useNA = "ifany"))
  cat("\nOverall FGA by general_range:\n")
  print(test_results %>% dplyr::group_by(general_range) %>%
          dplyr::summarise(total_fga = sum(fga), total_fg2a = sum(fg2a),
                           total_fg3a = sum(fg3a)))

  ## Reconcile: overall FGA should equal existing FGA
  cat("\nReconciliation:\n")
  cat("  Existing total FGA:", sum(readRDS(cache_file)$fga), "\n")
  cat("  De-overlapped total FGA (LT10 + complement):", sum(test_results$fga), "\n")

  ## Show Anunoby example
  cat("\nAnunoby (1628384) de-overlapped:\n")
  print(test_results %>%
          dplyr::filter(PLAYER_ID == "1628384") %>%
          dplyr::arrange(period, shot_clock_range))
}


## ─── Quick single-date test: FULL combo grid (both General Ranges) ──────────
## Use this for scraping a brand-new date with both "" and "Less Than 10 ft".
if (FALSE) {
  test_date     <- "10/22/2024"
  test_season   <- "2024-25"
  test_max_per  <- 4L
  test_workers  <- N_WORKERS

  test_combos <- expand.grid(
    period               = seq_len(test_max_per),
    shot_clock_range     = SHOT_CLOCK_RANGES,
    dribble_range        = DRIBBLE_RANGES,
    close_def_dist_range = DEF_DIST_RANGES,
    general_range        = GENERAL_RANGES,
    stringsAsFactors     = FALSE,
    KEEP.OUT.ATTRS       = FALSE
  )
  cat("Total combos:", nrow(test_combos), "\n")

  chunk_ids    <- sort(rep(seq_len(test_workers), length.out = nrow(test_combos)))
  chunks       <- split(test_combos, chunk_ids)

  cat("Launching", test_workers, "workers…\n")
  t0 <- Sys.time()
  cl <- parallel::makeCluster(test_workers)
  chunk_results <- parallel::parLapply(cl, chunks, .fetch_combo_batch,
                                       date_mdy   = test_date,
                                       season_str = test_season)
  parallel::stopCluster(cl)
  test_results <- dplyr::bind_rows(chunk_results)
  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)

  cat("Rows returned:", nrow(test_results), " |", elapsed, "s\n")
  cat("general_range distribution:\n")
  print(table(test_results$general_range, useNA = "ifany"))
}


missing_games <- setdiff(schedule_24$game_id, unique(pbp_all$game_id))
cat("Missing:", length(missing_games), "\n")
print(missing_games)

# Step 2: find which chunk CSVs contain those dates and delete them
missing_dates <- schedule_24 %>%
  filter(game_id %in% missing_games) %>%
  pull(game_date) %>% as.Date() %>% unique()
print(missing_dates)
# Then manually delete the affected pbp_YYYYMMDD_YYYYMMDD.csv files
# so those chunks get re-scraped on the next run




library(vroom)
csv_files <- sort(list.files("pbp_2024_2025", 
                             pattern = "^pbp_\\d{8}_\\d{8}\\.csv$",
                             full.names = TRUE))
pbp_test <- vroom(csv_files)
# shows exact file, row, column, expected vs actual type
problems(pbp_test) %>% as.data.frame()  
problems(pbp_test) %>% as.data.frame() %>% select(col) %>% unique()
problems(pbp_test) %>% as.data.frame() %>% select(file) %>% unique() %>% nrow()
problems(pbp_test) %>% as.data.frame() %>% nrow()



