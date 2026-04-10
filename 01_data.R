## ═════════════════════════════════════════════════════════════════════════════
## 01_data.R
## EPAA Pipeline — Full data pipeline
##
## OBJECTIVE:
##   Scrapes, joins, and enriches every NBA regular-season field goal attempt
##   into a single analysis-ready row. The output feeds directly into Stage 1
##   (XGBoost shot difficulty) and Stage 2 (Stan shooting talent).
##
## All intermediate results are cached to cache/ as .rds files so interrupted
## or re-run scrapes skip already-completed API calls automatically.
##
## OVERVIEW
##   1. Schedule        — game_id, game_date, team ids
##   2. PBP + lineups   — event-level play-by-play with 5-man lineup tracking
##   3. Feature eng.    — shot geometry, shot clock, possession context flags
##   4. (internal)      — shot clock reconstruction
##   5. Foul linkage    — and-1s, shooting fouls, unified contact flag
##   5b. Fatigue        — cumulative and stint minutes at the time of each FGA
##   6. Defender imp.   — closest defender assigned via matchup + co-occurrence
##   7. Biometrics      — player height and wingspan
##   8. Tracking agg.   — game-date-level joint dribble × defender-dist matrix
##   9. Assembly        — shot context imputation, player indices, feature scaling
##   10. Save           — per-chunk CSVs + season-level maps and scaling params
##
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


## Tracking constants
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

## Numeric upper bounds of each shot-clock bucket (seconds remaining).
## SC_BOUNDARIES[k] is the threshold between SHOT_CLOCK_RANGES[k] and [k+1].
## E.g., SC_BOUNDARIES[1] = 22 → shots with sc > 22 land in "24-22",
##                                shots with 18 < sc ≤ 22 land in "22-18 Very Early".
SC_BOUNDARIES <- c(22, 18, 15, 7, 4)   # length = length(SHOT_CLOCK_RANGES) - 1

## Half-width of the ambiguity window around each shot-clock boundary (seconds).
## A shot whose shot_clock falls within SC_BLEND_EPS of a boundary gets
## the two adjacent buckets' matrices SUMMED rather than choosing a side,
## reducing sensitivity to PBP timestamp rounding.
##
## v9: Widened from 0.5s to 2.0s to account for PBP timestamp imprecision.
## Live-ball events (steals, blocks → DRB) have an even wider 4.0s window
## because the shot clock may not start ticking until the defending team has
## gained physical possession, which can take a few seconds.
SC_BLEND_EPS          <- 2.0
SC_BLEND_EPS_LIVEBALL <- 4.0

## Maximum period to scrape (1–4 = regulation, 5–7 = OT1/OT2/OT3).
MAX_PERIOD <- 7L

## Number of parallel workers for the tracking scrape
N_WORKERS <- 8L

## Per-call delay (seconds) inside each parallel worker
CALL_DELAY <- 0.3

## Pause (seconds) between date groups in the tracking scrape. Gives the NBA
## CDN a breather between bursts of activity. 5–10s is enough to reset any
## short-term rate counters without materially affecting wall time.
GROUP_PAUSE <- 5


## ─── Helpers ────────────────────────────────────────────────────────────────

## safe_call() wraps hoopR API calls with:
##   - Mandatory inter-call delay (rate limiting)
##   - Detection of non-retryable errors (empty-result hoopR crashes)
##   - One retry for genuine transient network/timeout errors
##
## hoopR's nba_leaguedashplayerptshot throws "object 'df_list' not found"
## when the API returns an empty result set for a given filter combination.
## This is a hoopR bug — NOT transient. Retrying the identical call produces
## the identical crash. We detect these patterns and return NULL immediately
## rather than burning the 3s retry delay hundreds of times per game date.

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

## clock_to_seconds() converts the ISO 8601 duration string used in the live
## PBP feed (e.g. "PT11M43.00S") into seconds ELAPSED in the period.
## The live feed reports time REMAINING; we convert to elapsed for arithmetic.
## Formula: elapsed = 720 - remaining (720s = 12-minute quarter).
clock_to_seconds <- function(clock_str) {
  sapply(clock_str, function(cs) {
    if (is.na(cs) || cs == "") return(NA_real_)
    minutes   <- as.numeric(gsub("PT(\\d+)M.*", "\\1", cs))
    seconds   <- as.numeric(gsub(".*M([0-9.]+)S", "\\1", cs))
    remaining <- minutes * 60 + seconds
    720 - remaining
  }, USE.NAMES = FALSE)
}



## standardize_live_pbp() normalises the data frame returned by nba_live_pbp()
## to a consistent schema before lineup tracking and feature engineering.
##
## The live PBP feed uses camelCase column names and different field names than
## the historical boxscore feed. This function:
##   - Adds any columns that downstream code expects but the API omitted
##     (e.g. when a stat never occurred in that game, hoopR drops the column)
##   - Creates list-column stubs for qualifiers and person_ids_filter
##   - Aliases coordinate columns (xLegacy ↔ x_legacy, yLegacy ↔ y_legacy)
##     so both naming conventions work throughout the pipeline
##   - Renames columns to the shared schema (event_num, player1_id, etc.)
##   - Builds player2_id (assister) and player3_id (blocker/stealer/fouler)
##     from the individual person_id fields

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
    dplyr::rename(dplyr::any_of(c("period"          = "period",
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
                                  "order"           = "order_number"))) %>%
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
  
  # Keep legacy coords for engineer_features() 
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
## This avoids the "object 'df_list' not found" crash hoopR throws on empty
## result sets, and gives us direct control over retries and backoff.
##
## Implementation follows a known-working scraper pattern:
##   - Full URL string as first arg to GET() (not handle + path)
##   - simplifyVector = FALSE for safe JSON parsing
##   - do.call(rbind, rowSet) for row assembly
##   - Matching browser-like header set (Origin, no Host)
##
## Returns a data frame with standard LeagueDashPTShots columns, or NULL
## if the response is empty or all retries fail.

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
                                     delay                = CALL_DELAY,
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

  # Escalating backoff: 2s, 5s, 15s, 30s, 60s
  retry_waits <- c(2, 5, 15, 30, 60)
  for (attempt in seq_along(retry_waits)) {
    resp <- try(GET(.PTSHOT_URL, query = q, .PTSHOT_HEADERS, timeout(30)),
                silent = TRUE)

    if (inherits(resp, "try-error")) {
      last_err <- as.character(resp)
      Sys.sleep(retry_waits[attempt])
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
      Sys.sleep(retry_waits[attempt])
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
## Called by each parallel worker process. Receives a data frame where each
## row specifies one API call (period, shot_clock_range, dribble_range, etc.)
## and processes them sequentially, returning a combined data frame.
##
## This function is fully self-contained: it rebuilds the httr headers and
## all helper objects internally so it works correctly inside a forked or
## spawned worker process without relying on any parent-environment variables.
##
## Consecutive-timeout tracking: if 3+ consecutive calls time out, the IP
## may be temporarily blocked. The function pauses for 2 minutes to allow
## the block to expire before resuming.

.fetch_combo_batch <- function(combos_df, date_mdy, season_str,
                               date_to_mdy = date_mdy,
                               call_delay  = 0.5) {

  
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
  consec_timeouts <- 0L          # track consecutive timeouts across calls

  for (i in seq_len(nrow(combos_df))) {
    Sys.sleep(call_delay)

    q <- list(DateFrom = date_mdy, DateTo = date_to_mdy,
              Season = season_str, SeasonType = "Regular Season",
              PerMode = "Totals", LeagueID = "00",
              Period            = combos_df$period[i],
              ShotClockRange    = combos_df$shot_clock_range[i],
              DribbleRange      = combos_df$dribble_range[i],
              CloseDefDistRange = combos_df$close_def_dist_range[i],
              College = "", Conference = "", Country = "", Division = "",
              DraftPick = "", DraftYear = "", GameScope = "", GameSegment = "",
              GeneralRange = combos_df$general_range[i], 
              Height = "", LastNGames = 0,
              Location = "", Month = 0, OpponentTeamID = 0,
              Outcome = "", PORound = 0, PaceAdjust = "N",
              PlayerExperience = "", PlayerPosition = "", PlusMinus = "N",
              Rank = "N", SeasonSegment = "", ShotDistRange = "",
              StarterBench = "", TeamID = 0, TouchTimeRange = "",
              VsConference = "", VsDivision = "", Weight = "")

    # If we've seen 3+ consecutive timeouts, the IP is probably blocked.
    # Pause for 2 minutes to let the block expire before continuing.
    if (consec_timeouts >= 3L) {
      Sys.sleep(120)
      consec_timeouts <- 0L
    }

    ok <- FALSE
    # Escalating backoff: 2s, 5s, 15s, 30s, 60s
    retry_waits <- c(2, 5, 15, 30, 60)
    for (attempt in seq_along(retry_waits)) {
      resp <- try(GET(ptshot_url, query = q, ptshot_headers, timeout(30)),
                  silent = TRUE)
      if (inherits(resp, "try-error")) {
        Sys.sleep(retry_waits[attempt])
        next
      }
      http_sc <- status_code(resp)
      if (http_sc == 200L) { ok <- TRUE; break }
      if (http_sc %in% c(403L, 429L, 500L, 502L, 503L, 504L)) {
        Sys.sleep(retry_waits[attempt])
        next
      }
      break   # non-retryable HTTP code
    }

    if (!ok) {
      consec_timeouts <- consec_timeouts + 1L
      next
    }

    consec_timeouts <- 0L    # reset on success

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


## ════════════════════════════════════════════════════════════════════════════
## .scrape_combos_parallel()
##
## Splits the full combo grid (period × shot_clock × dribble × def_dist ×
## general_range) into small chunks of ~combos_per_chunk rows each, then
## dispatches them in waves of n_workers chunks via parLapply.
##
## ════════════════════════════════════════════════════════════════════════════

.scrape_combos_parallel <- function(combos, n_workers, date_mdy, season_str,
                                    date_to_mdy = date_mdy,
                                    call_delay  = CALL_DELAY,
                                    combos_per_chunk = 20L,
                                    label = "") {

  n_combos  <- nrow(combos)
  n_chunks  <- ceiling(n_combos / combos_per_chunk)
  chunk_ids <- sort(rep(seq_len(n_chunks), length.out = n_combos))
  chunks    <- split(combos, chunk_ids)
  n_waves   <- ceiling(n_chunks / n_workers)

  cl <- makeCluster(n_workers)
  on.exit(stopCluster(cl), add = TRUE)

  all_results <- vector("list", n_chunks)
  completed   <- 0L
  t0          <- Sys.time()

  for (w in seq_len(n_waves)) {
    # Indices for this wave
    idx_start <- (w - 1L) * n_workers + 1L
    idx_end   <- min(w * n_workers, n_chunks)
    wave_chunks <- chunks[idx_start:idx_end]

    wave_results <- parLapply(cl, wave_chunks, .fetch_combo_batch,
                              date_mdy    = date_mdy,
                              season_str  = season_str,
                              date_to_mdy = date_to_mdy,
                              call_delay  = call_delay)

    all_results[idx_start:idx_end] <- wave_results
    completed <- idx_end

    # Progress bar
    pct        <- round(completed / n_chunks * 100)
    elapsed    <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    rate       <- elapsed / completed
    remaining  <- (n_chunks - completed) * rate
    bar_width  <- 30L
    filled     <- round(pct / 100 * bar_width)
    bar        <- paste0("[",
                         strrep("█", filled),
                         strrep("░", bar_width - filled),
                         "]")

    message("\r    ", label, bar, " ", pct, "%  ",
            "(",  completed * combos_per_chunk, "/", n_combos, " calls)  ",
            round(elapsed / 60, 1), "m elapsed  ~",
            round(remaining / 60, 1), "m left",
            appendLF = (w == n_waves))
  }

  stopCluster(cl)
  on.exit(NULL)

  bind_rows(all_results)
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

## get_game_with_lineups() scrapes the live PBP feed and the rotation data
## for a single game, then tracks the 5-man lineup on court for both teams
## at every event. The lineup columns are needed for:
##   - Who was on the court to guard the shooter?
##   - Which defenders shared the floor with the shooter most often?
##   - How many minutes a player has played at the time of a shot.

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

## engineer_features() transforms the raw PBP data frame into an analysis-ready
## set of columns that describe every event — and especially every FGA — in
## terms of:
##   - Shot flags (fga, fgm, fta, ftm)
##   - Shot geometry (distance, angle in degrees from center)
##   - Shot classification (rim / j2 / j3, dunk, three-pointer)
##   - Game clock (elapsed seconds, game clock remaining, shot clock)
##   - Possession context (2nd chance, fastbreak, from turnover, transition)
##   - Possession structure (possession_id via cumulative possession_end flags)
##   - Free throw parsing (which free throw in a sequence, is_last_freethrow)
##
## The shot clock is reconstructed via a row-by-row state machine in the
## group_modify block (see inline comments there). All columns are written
## in a single mutate chain to avoid multiple passes over the data.

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
           is_dunk  = as.integer(str_detect(toupper(coalesce(sub_type, "")), 
                                            "DUNK")),
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
                                         # jumpball @ period start => poss ends
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
      poss_start_is_liveball  <- rep(FALSE, n)
      
      # state = shot clock "after" previous event (post-event)
      sc_state <- NA_real_
      
      # v9: track whether the current possession started from a live-ball event
      # (steal or block → DRB).  Used downstream for wider SC_BLEND_EPS.
      cur_poss_liveball <- FALSE
      # Track whether the most recent missed FGA was blocked (for ORB-after-block)
      last_miss_was_blocked <- FALSE
      
      ## After a made field goal the game clock keeps running (except in
      ## specific late-period situations), but the shot clock does not start
      ## until the ball is inbounded — typically ~2 seconds later.
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
        
        orb_after_block <- orb && last_miss_was_blocked
        
        bump14_if_under <- (orb && !orb_after_block) || 
          def3sec || 
          loose_ball_def_foul || 
          nonshoot_def_notpen
        
        made_last_ft <- isTRUE(df$is_last_freethrow[i] == 1L && df$ftm[i] == 1L)
        
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
        
        # Track whether this FGA was blocked
        if (isTRUE(df$fga[i] == 1L) && isTRUE(df$fgm[i] == 0L)) {
          blk_id <- as.character(df$block_person_id[i])
          last_miss_was_blocked <- !is.na(blk_id) && blk_id != "" && 
            blk_id != "NA" && blk_id != "0"
        } else if (isTRUE(df$fga[i] == 1L) && isTRUE(df$fgm[i] == 1L)) {
          last_miss_was_blocked <- FALSE
        }
        
        poss_start_is_liveball[i] <- cur_poss_liveball
        
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
        
        ## The inbound credit absorbs the first N seconds of game-clock time
        ## that elapsed while the ball was dead (being inbounded).  This credit
        ## persists across rows with dt=0 (subs, etc.) until real time elapses.
        adj_dt <- dt
        if (inbound_credit > 0 && dt > 0) {
          credit_used    <- min(inbound_credit, dt)
          adj_dt         <- dt - credit_used
          inbound_credit <- inbound_credit - credit_used
        }
        
        # Initialize at possession start
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
          
          # use adj_dt (inbound-delay-corrected)
          sc_pre <- if (is.na(sc_state)) NA_real_ else max(sc_state - adj_dt, 0)
          
          # Apply event-based resets AFTER the event
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
        
        if (isTRUE(df$possession_end[i] == 1L)) {
          prev_ended_made_fg <- isTRUE(df$fgm[i] == 1L)
          if (prev_ended_made_fg) {
            made_fg_gc     <- gc
            made_fg_period <- df$period[i]
          }
        }
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

## link_shooting_fouls() links every shooting foul in the PBP to the FGA that
## caused it, then adds three columns to each FGA row:
##
##   drew_and1          — 1 if the shot was made AND drew a shooting foul
##   and1_ft_made       — number of and-1 free throws made
##   and1_fouler_id     — player_id of the defender who committed the foul
##   had_shooting_foul  — 1 if the shot was MISSED but drew a shooting foul
##   shooting_foul_fouler_id — defender who committed the non-and1 foul
##   is_contact_shot    — unified flag: block OR and-1 OR shooting foul

link_shooting_fouls <- function(pbp) {
  pbp <- pbp %>% mutate(row_id = row_number())

  foul_rows <- which(pbp$is_shootingfoul == 1)

  foul_trips <- lapply(foul_rows, function(fi) {
    foul_evt <- pbp[fi, ]
    drawn_by <- foul_evt$foul_drawn_person_id
    gid      <- foul_evt$game_id
    if (is.na(drawn_by) || drawn_by %in% c("", "NA")) return(NULL)

    # The player who committed the foul = closest defender
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
## Computes two fatigue proxies:
##
##   shooter_min_game   — cumulative minutes the shooter has been on court
##                        in the current game
##   shooter_min_stint  — minutes since the shooter's most recent sub-in
##
## ═════════════════════════════════════════════════════════════════════════════

compute_fatigue_features <- function(pbp, verbose = TRUE) {

  pbp$shooter_min_game  <- NA_real_
  pbp$shooter_min_stint <- NA_real_

  games <- unique(pbp$game_id)

  for (gid in games) {
    game_mask <- which(pbp$game_id == gid)
    if (length(game_mask) == 0) next
    game_pbp <- pbp[game_mask, ]

    on_court <- list()
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

## pull_game_matchups() fetches the NBA boxscore matchup data for a single game.
## Each row describes how much time and how many FGA one offensive player faced
## a specific defender. This data drives Phase 2 of defender imputation.

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
## each defensive player shared the floor. These counts are used in Phase 2
## of impute_closest_defender() to modulate matchup FGA weights: defenders
## who were on the floor with the shooter more in the relevant period receive
## proportionally higher sampling weight.
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
##   partial_possessions              = fractional possession time
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
  
  # Standardize matchup data: rename to uppercase keys and compute
  # shot-type-specific weights (TPA = three-pointer attempts, TWOPA = 2PA).
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
  # defender shared the floor. Used to modulate matchup FGA weights in
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
  
  # Helper: get opposing team's on-court players at the time of the shot
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
  
  # Counters for the source breakdown reported in verbose output
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
    # Use 3PA weights for three-pointers, 2PA weights for twos, since the
    # matchup data is more precise when filtered to the relevant shot type.
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
## "mFGA" = matchup sum (overcounted, shown for reference only).
##
## Reports:
##   - Per-player overcount ratio (matchup sum vs PBP FGA)
##   - Per-player proportion MAE (avg |imputed_share - matchup_share|)
##   - Volume-weighted correlation between imputed and matchup FGA shares
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
              overcount_pct   = round((matchup_fga_sum / max(pbp_fga, 1) - 1) * 100, 
                                      1),
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
  overcount_pct     <- round((total_matchup_fga / max(total_pbp_fga, 1) - 1) * 100, 
                             1)
  
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

## get_biometrics() fetches player height from nba_leaguedashplayerbiostats
## and wingspan from the NBA Draft Combine anthropometric database (all years
## from 2000 to season_year). The wingspan fallback of height × 1.06 applies
## the typical NBA ratio for players with no combine measurement.
##
## Note: biometric differentials (height_diff, wingspan_diff) were evaluated
## as XGBoost features and removed after SHAP analysis revealed they acted as
## partial player fingerprints — continuous differentials can uniquely identify
## specific matchups, causing Stage 2 random effects to capture "skill relative
## to size" rather than absolute skill. Height/wingspan columns are retained as
## reference columns in the output but are NOT used as model features.

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
## Each unique combination of the four filter parameters requires a separate
## API call because the NBA Stats API does not support OR-filtering. The full
## combo grid per date is:
##   periods × shot_clock_ranges × dribble_ranges × def_dist_ranges × general_ranges
##   = 7 × 6 × 5 × 4 × 2 = 1,680 calls (up to, in OT games)
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

## Helper: derive max period count from game_status_text.
## "Final" → 4, "Final/OT" → 5, "Final/2OT" → 6, etc.
## Caps at MAX_PERIOD to avoid requesting unnecessary combos.
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

## Helper: group game dates into ranges where no team plays more than once.
## When no team appears twice in a date range, the NBA Stats API date-range
## query returns player-level data that is effectively game-level — each
## player's rows come from exactly one game. This allows multiple dates to be
## scraped in a single pass through the combo grid, cutting total API calls
## roughly in half vs. scraping each date independently.
##
## Returns a list of character vectors, each vector being a group of dates.
.build_date_groups <- function(schedule) {

  date_teams <- schedule %>%
    mutate(game_date = as.character(game_date)) %>%
    group_by(game_date) %>%
    summarise(teams = list(unique(c(as.character(home_team_id),
                                    as.character(away_team_id)))),
              .groups = "drop") %>%
    arrange(game_date)

  groups    <- list()
  cur_dates <- character()
  cur_teams <- character()

  for (i in seq_len(nrow(date_teams))) {
    d <- date_teams$game_date[i]
    t <- date_teams$teams[[i]]

    if (any(t %in% cur_teams)) {
      # Conflict — close current group, start new
      if (length(cur_dates) > 0) groups <- c(groups, list(cur_dates))
      cur_dates <- d
      cur_teams <- t
    } else {
      cur_dates <- c(cur_dates, d)
      cur_teams <- c(cur_teams, t)
    }
  }
  if (length(cur_dates) > 0) groups <- c(groups, list(cur_dates))

  groups
}


## get_game_level_tracking() is the main tracking scrape function. For each
## game date (or group of dates), it:
##   1. Checks the per-date .rds cache — loads and skips if present
##   2. Validates cached FGA against PBP FGA (if pbp is supplied) — deletes
##      and re-scrapes if tracking is more than 2% short (likely rate-limited)
##   3. Scrapes the full combo grid for uncached dates
##   4. Normalizes column types and recovers game_date via TEAM_ID join
##   5. De-overlaps: subtracts "Less Than 10 ft" counts from "Overall" to
##      produce non-overlapping distance slices per player per game date
##   6. Saves per-date .rds caches and optionally retries with fewer workers
##
## The pbp argument enables PBP-vs-tracking reconciliation. Discrepancies
## > 2% indicate the scrape was incomplete (some combos returned empty due
## to rate limiting). The retry schedule reduces workers to improve reliability.

get_game_level_tracking <- function(schedule, season_str, cache_dir = "cache",
                                    pbp = NULL) {

  season_tag     <- gsub("-", "_20", substr(season_str, 1, 7))
  date_cache_dir <- file.path(cache_dir, paste0("shot_tracking_", season_tag))
  dir.create(date_cache_dir, showWarnings = FALSE, recursive = TRUE)

  game_dates <- sort(unique(as.character(schedule$game_date)))
  n_dates    <- length(game_dates)
  combos_per_period <- length(SHOT_CLOCK_RANGES) *
                       length(DRIBBLE_RANGES) *
                       length(DEF_DIST_RANGES) *
                       length(GENERAL_RANGES)          # 6 × 5 × 4 × 2 = 240

  # ── PBP FGA by date (for reconciliation when pbp is supplied) ───────────
  #    Compare tracking Overall FGA against PBP FGA per date.  If the
  #    tracking scrape is short (rate-limited / dropped calls), re-scrape
  #    that date solo with fewer workers.
  has_pbp_check <- !is.null(pbp) && "fga" %in% names(pbp) &&
                   "game_id" %in% names(pbp)
  if (has_pbp_check) {
    pbp_date_fga <- pbp %>%
      filter(fga == 1L) %>%
      left_join(schedule %>%
                  mutate(game_date = as.character(game_date)) %>%
                  select(game_id, game_date),
                by = "game_id") %>%
      group_by(game_date) %>%
      summarise(pbp_fga = n(), .groups = "drop")
    pbp_fga_map <- setNames(pbp_date_fga$pbp_fga, pbp_date_fga$game_date)
    message("  PBP reconciliation enabled (",
            sum(pbp_fga_map), " total FGA across ",
            length(pbp_fga_map), " dates)")
  }

  # ── Pre-compute max period per date from game_status_text ───────────────
  has_status <- "game_status_text" %in% names(schedule)
  if (has_status) {
    date_max_period <- schedule %>%
      mutate(game_date = as.character(game_date)) %>%
      group_by(game_date) %>%
      summarise(.max_per = .max_period_from_status(game_status_text),
                .groups = "drop")
    date_max_per <- setNames(pmin(date_max_period$.max_per, MAX_PERIOD),
                             date_max_period$game_date)
  } else {
    date_max_per <- setNames(rep(MAX_PERIOD, n_dates), game_dates)
  }

  # ── Build date groups (ranges where no team plays twice) ────────────────
  date_groups <- .build_date_groups(schedule)
  n_groups    <- length(date_groups)

  # ── Team → game_date mapping for date recovery after range scraping ─────
  #    Since no team plays twice in a group, TEAM_ID uniquely identifies
  #    the game_date within each group. This lets us recover game_date from
  #    the PLAYER_LAST_TEAM_ID returned by the API for each player row.
  team_date_lookup <- bind_rows(
    schedule %>% mutate(game_date = as.character(game_date)) %>%
      transmute(TEAM_ID = as.character(home_team_id), game_date),
    schedule %>% mutate(game_date = as.character(game_date)) %>%
      transmute(TEAM_ID = as.character(away_team_id), game_date)) %>% 
    distinct()

  # ── Estimate total calls (grouped vs ungrouped) ────────────────────────

  # ── Player → game_date mapping (fallback for traded players) ────────────
  #    The API returns PLAYER_LAST_TEAM_ID which is the player's CURRENT
  #    team, not the team they were on at game time.  Traded players get
  #    their new team's ID, which won't join to the old game_date.
  #    When PBP is available, build a direct PLAYER_ID → game_date lookup
  #    as a fallback for rows where the TEAM_ID join fails.
  if (has_pbp_check) {
    player_date_lookup <- pbp %>%
      filter(!is.na(player1_id), player1_id != "" & player1_id != "0") %>%
      left_join(schedule %>%
                  mutate(game_date = as.character(game_date)) %>%
                  select(game_id, game_date),
                by = "game_id") %>%
      transmute(PLAYER_ID = as.character(player1_id), game_date) %>%
      distinct()
  } else {
    player_date_lookup <- NULL
  }

  total_calls_grouped <- sum(vapply(date_groups, function(grp)
    as.integer(max(date_max_per[grp])) * combos_per_period, integer(1L)))
  total_calls_ungrouped <- sum(vapply(game_dates, function(d)
    as.integer(date_max_per[d]) * combos_per_period, integer(1L)))

  ot_dates     <- sum(date_max_per[game_dates] > 4L)
  use_parallel <- N_WORKERS > 1L

  message("  Game-level tracking: ", n_dates, " game-date(s) in ",
          n_groups, " groups, ", ot_dates, " with OT, ",
          N_WORKERS, " worker(s)")
  message("  Total API calls: ", total_calls_grouped,
          "  (vs ", total_calls_ungrouped, " ungrouped → ",
          round((1 - total_calls_grouped / total_calls_ungrouped) * 100, 0),
          "% reduction)")
  message("  Est. ", round(total_calls_grouped * 5.5 / N_WORKERS / 60, 0),
          " min @ ~5.5s/call ÷ ", N_WORKERS, " workers")
  message("  Per-date cache dir: ", date_cache_dir)

  all_records <- vector("list", n_dates)
  names(all_records) <- game_dates

  # ── Empty-row template ─────────────────────────────────────────────────
  empty_date_df <- tibble(PLAYER_ID = character(), 
                          period = integer(),
                          shot_clock_range = character(), 
                          dribble_range = character(),
                          def_dist_range = character(), 
                          general_range = character(),
                          fg2a = integer(), 
                          fg3a = integer(),
                          fg2m = integer(), 
                          fg3m = integer(),
                          fgm = integer(), 
                          fga = integer(),
                          game_date = character())

  for (g_idx in seq_along(date_groups)) {

    grp_dates <- date_groups[[g_idx]]
    n_grp     <- length(grp_dates)
    date_from <- min(grp_dates)
    date_to   <- max(grp_dates)

    # ── Check cache: if ALL dates in group are cached, load and skip ─────
    cache_files <- file.path(date_cache_dir, paste0(grp_dates, ".rds"))
    all_cached  <- all(file.exists(cache_files))

    if (all_cached) {
      for (d in grp_dates) {
        all_records[[d]] <- readRDS(file.path(date_cache_dir,
                                              paste0(d, ".rds")))
      }

      # ── Validate cached dates against PBP FGA ───────────────────────────
      #    Catches stale bad caches from previous runs with rate-limiting.
      if (has_pbp_check) {
        for (d in grp_dates) {
          if (!(d %in% names(pbp_fga_map))) next
          expected_fga <- pbp_fga_map[[d]]
          if (is.na(expected_fga) || expected_fga == 0L) next

          cached_rec <- all_records[[d]]
          cached_fga <- if (!is.null(cached_rec)) sum(cached_rec$fga) else 0L
          shortfall  <- (expected_fga - cached_fga) / expected_fga * 100

          if (shortfall > 2.0) {
            message("  [grp ", g_idx, "/", n_groups, "] ",
                    d, " cached but short: tracking FGA=", cached_fga,
                    " vs PBP FGA=", expected_fga,
                    " (", 
                    round(shortfall, 1), "%) — deleting cache to force re-scrape")
            file.remove(file.path(date_cache_dir, paste0(d, ".rds")))
            all_records[[d]] <- NULL
            all_cached <- FALSE   # force this group to re-scrape
          }
        }
      }

      if (all_cached) {
        if (g_idx %% 10 == 0 || g_idx == n_groups)
          message("  [grp ", g_idx, "/", n_groups, "] ",
                  date_from, " → ", date_to,
                  " (", n_grp, " date", if (n_grp > 1) "s", ") — all cached")
        next
      }
      # If any caches were invalidated, fall through to re-scrape the group
    }

    # ── Max period for group = max across all dates in group ─────────────
    grp_max_per  <- max(as.integer(date_max_per[grp_dates]))
    grp_n_combos <- grp_max_per * combos_per_period

    message("  [grp ", g_idx, "/", n_groups, "] ",
            date_from, " → ", date_to,
            " (", n_grp, " date", if (n_grp > 1) "s",
            ", ", grp_max_per, " per, ",
            grp_n_combos, " calls, ",
            N_WORKERS, " workers) scraping…")

    # ── Build full combo grid for this group ─────────────────────────────
    combos <- expand.grid(period            = seq_len(grp_max_per),
                          shot_clock_range  = SHOT_CLOCK_RANGES,
                          dribble_range     = DRIBBLE_RANGES,
                          close_def_dist_range = DEF_DIST_RANGES,
                          general_range     = GENERAL_RANGES,
                          stringsAsFactors  = FALSE,
                          KEEP.OUT.ATTRS    = FALSE)

    date_from_mdy <- format(as.Date(date_from), "%m/%d/%Y")
    date_to_mdy   <- format(as.Date(date_to),   "%m/%d/%Y")

    t_start <- Sys.time()

    if (use_parallel) {
      # ── PARALLEL: wave-based dispatch with progress ────────────────────
      grp_df <- .scrape_combos_parallel(combos, 
                                        N_WORKERS,
                                        date_mdy    = date_from_mdy,
                                        season_str  = season_str,
                                        date_to_mdy = date_to_mdy,
                                        call_delay  = CALL_DELAY,
                                        label = paste0(date_from, 
                                                       " → ", 
                                                       date_to, 
                                                       "  "))
      } else {
      # ── SEQUENTIAL: single-threaded with progress ──────────────────────
      combo_results <- vector("list", nrow(combos))

      for (i in seq_len(nrow(combos))) {
        if (i %% 30 == 1 || i == nrow(combos)) {
          remaining <- grp_n_combos - i
          elapsed   <- as.numeric(difftime(Sys.time(), t_start, units = "secs"))
          rate      <- if (i > 1) elapsed / (i - 1) else 5.5
          message("    combo ", i, "/", grp_n_combos,
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
          date_from            = date_from_mdy,
          date_to              = date_to_mdy,
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
          combo_results[[i]]   <- df
        }
      }
      grp_df <- bind_rows(combo_results)
    }

    # ── Normalise columns and recover game_date via TEAM_ID ──────────────
    if (!is.null(grp_df) && nrow(grp_df) > 0) {

      # Identify team column (API returns PLAYER_LAST_TEAM_ID or TEAM_ID)
      tid_col <- intersect(names(grp_df),
                           c("PLAYER_LAST_TEAM_ID", "TEAM_ID"))
      if (length(tid_col) == 0)
        stop("No team ID column found in tracking response.  Columns: ",
             paste(names(grp_df), collapse = ", "))

      grp_df <- grp_df %>%
        transmute(PLAYER_ID        = as.character(PLAYER_ID),
                  .TEAM_ID         = as.character(.data[[tid_col[1]]]),
                  period           = as.integer(period),
                  shot_clock_range = shot_clock_range,
                  dribble_range    = dribble_range,
                  def_dist_range   = def_dist_range,
                  general_range    = general_range,
                  fg2a             = as.integer(FG2A),
                  fg3a             = as.integer(FG3A),
                  fg2m             = as.integer(FG2M),
                  fg3m             = as.integer(FG3M),
                  fgm              = as.integer(FGM),
                  fga              = as.integer(FGA))

      # Map TEAM_ID → game_date (within this group's dates only)
      grp_team_dates <- team_date_lookup %>% filter(game_date %in% grp_dates)

      grp_df <- grp_df %>%
        left_join(grp_team_dates,
                  by = c(".TEAM_ID" = "TEAM_ID")) %>%
        select(-.TEAM_ID)

      # Fallback for traded players: use PBP player→date lookup
      n_unmapped <- sum(is.na(grp_df$game_date))
      if (n_unmapped > 0 && !is.null(player_date_lookup)) {
        grp_player_dates <- player_date_lookup %>%
          filter(game_date %in% grp_dates)

        unmapped_idx <- is.na(grp_df$game_date)
        fallback <- grp_df[unmapped_idx, ] %>%
          select(-game_date) %>%
          left_join(grp_player_dates, by = "PLAYER_ID")

        grp_df$game_date[unmapped_idx] <- fallback$game_date

        n_still_unmapped <- sum(is.na(grp_df$game_date))
        n_recovered <- n_unmapped - n_still_unmapped
        if (n_recovered > 0)
          message("      ℹ ", n_recovered, "/", n_unmapped,
                  " unmapped rows recovered via PBP player-date lookup",
                  if (n_still_unmapped > 0)
                    paste0(" (", n_still_unmapped, " still unmapped)"))
        n_unmapped <- n_still_unmapped
      }

      # Drop any rows that couldn't be mapped by either method
      if (n_unmapped > 0) {
        warning("  ", n_unmapped,
                " tracking rows could not be mapped to a game_date — dropping")
        grp_df <- grp_df %>% filter(!is.na(game_date))
      }

      # ── Split by game_date, de-overlap, and cache per date ─────────────
      # De-overlap: the "Overall" slice includes close-range shots. The
      # "Less Than 10 ft" slice is a sub-set of Overall. To get non-overlapping
      # slices we subtract LT10 counts from Overall, yielding:
      #   general_range == ""        → shots ≥ 10 ft
      #   general_range == "LT10 ft" → shots < 10 ft
      for (d in grp_dates) {
        date_df <- grp_df %>% filter(game_date == d)

        if (nrow(date_df) > 0 &&
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

        if (nrow(date_df) == 0) date_df <- empty_date_df

        cache_file <- file.path(date_cache_dir, paste0(d, ".rds"))
        saveRDS(date_df, cache_file)
        all_records[[d]] <- date_df
      }

    } else {
      # Empty group — save empty caches for all dates
      for (d in grp_dates) {
        cache_file <- file.path(date_cache_dir, paste0(d, ".rds"))
        saveRDS(empty_date_df, cache_file)
        all_records[[d]] <- empty_date_df
      }
    }

    elapsed <- round(as.numeric(difftime(Sys.time(), t_start, units = "secs")), 1)
    grp_rows <- sum(vapply(grp_dates, function(d) {
      r <- all_records[[d]]; if (is.null(r)) 0L else nrow(r)
    }, integer(1L)))
    message("    ✓ ", n_grp, " date", if (n_grp > 1) "s",
            "  |  ", grp_rows, " rows  |  ",
            elapsed, "s (", round(elapsed / 60, 1), " min)")

    # ── PBP FGA reconciliation + progressive retry ───────────────────────────
    #    For each date in the group, compare tracking Overall FGA (sum of
    #    both general_range slices after de-overlap) against PBP FGA.
    #    If tracking is short by > 2%, retry with progressively fewer
    #    workers (the primary lever on API reliability):
    #      Attempt 1: 4 workers, CALL_DELAY
    #      Attempt 2: 2 workers, 0.5s delay
    if (has_pbp_check) {
      retry_schedule <- list(list(w = 4L, d = CALL_DELAY),
                             list(w = 2L, d = 0.5))

      for (d in grp_dates) {
        if (!(d %in% names(pbp_fga_map))) next
        expected_fga <- pbp_fga_map[[d]]
        if (is.na(expected_fga) || expected_fga == 0L) next

        best_rec <- all_records[[d]]
        best_fga <- if (!is.null(best_rec) && nrow(best_rec) > 0) {
          sum(best_rec$fga)
        } else 0L

        for (att in seq_along(retry_schedule)) {
          shortfall_pct <- (expected_fga - best_fga) / expected_fga * 100
          if (shortfall_pct <= 2.0) break   # close enough

          rw <- retry_schedule[[att]]$w
          rd <- retry_schedule[[att]]$d

          message("    ⚠ ", d, ": tracking FGA=", best_fga,
                  " vs PBP FGA=", expected_fga,
                  " (", round(shortfall_pct, 1), "% short) — retry ",
                  att, "/", length(retry_schedule),
                  " with ", rw, " worker(s), ",
                  rd, "s delay…")

          d_max_per <- as.integer(date_max_per[d])
          d_combos <- expand.grid(
            period               = seq_len(d_max_per),
            shot_clock_range     = SHOT_CLOCK_RANGES,
            dribble_range        = DRIBBLE_RANGES,
            close_def_dist_range = DEF_DIST_RANGES,
            general_range        = GENERAL_RANGES,
            stringsAsFactors     = FALSE,
            KEEP.OUT.ATTRS       = FALSE
          )

          d_mdy    <- format(as.Date(d), "%m/%d/%Y")
          t_retry  <- Sys.time()

          retry_raw <- .scrape_combos_parallel(d_combos, rw,
                                               date_mdy    = d_mdy,
                                               season_str  = season_str,
                                               date_to_mdy = d_mdy,
                                               call_delay  = rd,
                                               label       = paste0("retry ", 
                                                                    att, 
                                                                    "  "))

          # Normalise retry results (single date — no TEAM_ID recovery needed)
          if (!is.null(retry_raw) && nrow(retry_raw) > 0) {
            retry_df <- retry_raw %>%
              transmute(PLAYER_ID        = as.character(PLAYER_ID),
                        period           = as.integer(period),
                        shot_clock_range = shot_clock_range,
                        dribble_range    = dribble_range,
                        def_dist_range   = def_dist_range,
                        general_range    = general_range,
                        fg2a             = as.integer(FG2A),
                        fg3a             = as.integer(FG3A),
                        fg2m             = as.integer(FG2M),
                        fg3m             = as.integer(FG3M),
                        fgm              = as.integer(FGM),
                        fga              = as.integer(FGA))
            retry_df$game_date <- d

            # De-overlap
            if (any(retry_df$general_range == "Less Than 10 ft")) {
              r_join <- c("PLAYER_ID", "game_date", "period",
                          "shot_clock_range", "dribble_range", "def_dist_range")
              r_overall <- retry_df %>% filter(general_range == "")
              r_lt10    <- retry_df %>% filter(general_range == "Less Than 10 ft")
              r_gt10 <- r_overall %>%
                left_join(r_lt10 %>% select(all_of(r_join),
                                            fg2a_lt = fg2a, fg3a_lt = fg3a,
                                            fg2m_lt = fg2m, fg3m_lt = fg3m,
                                            fgm_lt  = fgm,  fga_lt  = fga),
                          by = r_join) %>%
                mutate(across(ends_with("_lt"), ~ coalesce(.x, 0L)),
                       fg2a = fg2a - fg2a_lt, fg3a = fg3a - fg3a_lt,
                       fg2m = fg2m - fg2m_lt, fg3m = fg3m - fg3m_lt,
                       fgm  = fgm  - fgm_lt,  fga  = fga  - fga_lt) %>%
                select(-ends_with("_lt")) %>%
                filter(fga > 0L)
              retry_df <- bind_rows(r_lt10, r_gt10)
            }

            retry_fga <- sum(retry_df$fga)
            retry_elapsed <- round(as.numeric(
              difftime(Sys.time(), t_retry, units = "secs")), 1)

            # Keep the better result
            if (retry_fga >= best_fga) {
              cache_file <- file.path(date_cache_dir, paste0(d, ".rds"))
              saveRDS(retry_df, cache_file)
              all_records[[d]] <- retry_df
              best_fga <- retry_fga
              best_rec <- retry_df

              new_pct <- (expected_fga - retry_fga) / expected_fga * 100
              message("      ✓ attempt ", att, ": FGA=", retry_fga,
                      " (PBP=", expected_fga,
                      ", ", round(abs(new_pct), 1), "% ",
                      if (new_pct > 0) "short" else "over",
                      ")  |  ", retry_elapsed, "s")
            } else {
              message("      ✗ attempt ", att, " worse (",
                      retry_fga, " < ", best_fga,
                      ") — keeping previous  |  ", retry_elapsed, "s")
            }
          }
        }  # end retry attempts
      }  # end date loop
    }  # end if (has_pbp_check)

    # ── Pause between groups to avoid IP-level throttling ──────────────────
    if (g_idx < n_groups) Sys.sleep(GROUP_PAUSE)

  }  # end group loop
  

  out <- bind_rows(all_records)
  message("  Total tracking rows: ", nrow(out),
          "  |  unique players: ", n_distinct(out$PLAYER_ID),
          "  |  dates: ", n_distinct(out$game_date))

  # ── Final reconciliation summary ──────────────────────────────────────────
  if (has_pbp_check) {
    still_short <- 0L
    for (d in names(pbp_fga_map)) {
      if (!(d %in% names(all_records)) || is.null(all_records[[d]])) next
      trk_fga <- sum(all_records[[d]]$fga)
      pbp_fga <- pbp_fga_map[[d]]
      if (pbp_fga > 0 && (pbp_fga - trk_fga) / pbp_fga > 0.02)
        still_short <- still_short + 1L
    }
    if (still_short > 0L) {
      warning("  ", still_short, " date(s) still >2% short after retry. ",
              "Consider reducing N_WORKERS or re-running those dates.")
    } else {
      message("  ✓ All dates within 2% of PBP FGA")
    }
  }

  out
}


## ════════════════════════════════════════════════════════════════════════════
## get_lt10_tracking()  — Retrofit existing tracking caches with LT10 split
##
## Reads each existing date-level .rds cache (which has no general_range
## column — scraped before the distance-split was introduced), scrapes ONLY
## the "Less Than 10 ft" slice from the NBA Stats API, then merges and
## de-overlaps so each cached file has non-overlapping distance slices:
##   ""                 = shots ≥ 10 ft  (Overall minus LT10)
##   "Less Than 10 ft"  = shots < 10 ft   (scraped directly)
##
## Uses the same date-grouping optimisation as get_game_level_tracking():
## dates are grouped into ranges where no team plays twice, and each group
## is scraped in a single pass through the combo grid.
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

  # Combo grid for LT10 only (same dimensions, but general_range fixed)
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

  # ── Build date groups + team → date mapping ──────────────────────────────
  date_groups <- .build_date_groups(schedule)
  n_groups    <- length(date_groups)

  team_date_lookup <- bind_rows(
    schedule %>% mutate(game_date = as.character(game_date)) %>%
      transmute(TEAM_ID = as.character(home_team_id), game_date),
    schedule %>% mutate(game_date = as.character(game_date)) %>%
      transmute(TEAM_ID = as.character(away_team_id), game_date)
  ) %>% distinct()

  total_calls_grouped <- sum(vapply(date_groups, function(grp)
    as.integer(max(date_max_per[grp])) * combos_per_period, integer(1L)))

  use_parallel <- N_WORKERS > 1L
  message("  LT10 retrofit: ", n_dates, " dates in ", n_groups, " groups, ",
          N_WORKERS, " worker(s), ", total_calls_grouped, " total calls")
  message("  Est. ", round(total_calls_grouped * 5.5 / N_WORKERS / 60, 0),
          " min @ ~5.5s/call")

  all_records <- vector("list", n_dates)
  names(all_records) <- game_dates

  for (g_idx in seq_along(date_groups)) {

    grp_dates <- date_groups[[g_idx]]
    n_grp     <- length(grp_dates)
    date_from <- min(grp_dates)
    date_to   <- max(grp_dates)

    # ── Check: do ALL dates in group already have general_range? ─────────
    # If a cache already contains the general_range column, it has already
    # been retrofitted and can be loaded without re-scraping.
    all_retrofitted <- TRUE
    for (d in grp_dates) {
      cf <- file.path(date_cache_dir, paste0(d, ".rds"))
      if (!file.exists(cf)) {
        message("  [grp ", g_idx, "/", n_groups, "] ", d, " — no cache, skipping")
        all_retrofitted <- FALSE
        next
      }
      existing <- readRDS(cf)
      if (!"general_range" %in% names(existing)) {
        all_retrofitted <- FALSE
        break
      }
    }

    if (all_retrofitted) {
      for (d in grp_dates) {
        cf <- file.path(date_cache_dir, paste0(d, ".rds"))
        if (file.exists(cf)) all_records[[d]] <- readRDS(cf)
      }
      if (g_idx %% 10 == 0 || g_idx == n_groups)
        message("  [grp ", g_idx, "/", n_groups, "] ",
                date_from, " → ", date_to,
                " — already retrofitted, skipping")
      next
    }

    grp_max_per  <- max(as.integer(date_max_per[grp_dates]))
    grp_n_combos <- grp_max_per * combos_per_period

    message("  [grp ", g_idx, "/", n_groups, "] ",
            date_from, " → ", date_to,
            " (", n_grp, " date", if (n_grp > 1) "s",
            ", ", grp_max_per, " per, ",
            grp_n_combos, " LT10 calls) scraping…")

    # ── Build LT10-only combo grid ────────────────────────────────────────
    combos <- expand.grid(period               = seq_len(grp_max_per),
                          shot_clock_range     = SHOT_CLOCK_RANGES,
                          dribble_range        = DRIBBLE_RANGES,
                          close_def_dist_range = DEF_DIST_RANGES,
                          stringsAsFactors     = FALSE,
                          KEEP.OUT.ATTRS       = FALSE)
    combos$general_range <- "Less Than 10 ft"

    date_from_mdy <- format(as.Date(date_from), "%m/%d/%Y")
    date_to_mdy   <- format(as.Date(date_to),   "%m/%d/%Y")

    t_start <- Sys.time()

    if (use_parallel) {
      lt10_raw <- .scrape_combos_parallel(
        combos, N_WORKERS,
        date_mdy    = date_from_mdy,
        season_str  = season_str,
        date_to_mdy = date_to_mdy,
        call_delay  = CALL_DELAY,
        label       = paste0("LT10 ", date_from, "  "))
    } else {
      combo_results <- vector("list", nrow(combos))
      for (i in seq_len(nrow(combos))) {
        df <- .nba_stats_ptshot_direct(
          date_from            = date_from_mdy,
          date_to              = date_to_mdy,
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
      lt10_raw <- bind_rows(combo_results)
    }

    # ── Normalise LT10 columns and recover game_date via TEAM_ID ─────────
    if (!is.null(lt10_raw) && nrow(lt10_raw) > 0) {

      tid_col <- intersect(names(lt10_raw),
                           c("PLAYER_LAST_TEAM_ID", "TEAM_ID"))
      if (length(tid_col) == 0)
        stop("No team ID column in LT10 tracking response.  Columns: ",
             paste(names(lt10_raw), collapse = ", "))

      lt10_df <- lt10_raw %>%
        transmute(
          PLAYER_ID         = as.character(PLAYER_ID),
          .TEAM_ID          = as.character(.data[[tid_col[1]]]),
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

      grp_team_dates <- team_date_lookup %>% filter(game_date %in% grp_dates)

      lt10_df <- lt10_df %>%
        left_join(grp_team_dates, by = c(".TEAM_ID" = "TEAM_ID")) %>%
        select(-.TEAM_ID)

      n_unmapped <- sum(is.na(lt10_df$game_date))
      if (n_unmapped > 0) {
        warning("  ", n_unmapped, " LT10 rows unmapped — dropping")
        lt10_df <- lt10_df %>% filter(!is.na(game_date))
      }
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

    # ── Per-date: load existing, tag as Overall, de-overlap with LT10 ────
    for (d in grp_dates) {
      cf <- file.path(date_cache_dir, paste0(d, ".rds"))
      if (!file.exists(cf)) next

      existing <- readRDS(cf)
      if ("general_range" %in% names(existing)) {
        all_records[[d]] <- existing
        next
      }

      existing$general_range <- ""
      existing$game_date     <- d

      date_lt10 <- lt10_df %>% filter(game_date == d)

      join_cols <- c("PLAYER_ID", "game_date", "period",
                     "shot_clock_range", "dribble_range", "def_dist_range")

      complement <- existing %>%
        left_join(date_lt10 %>% select(all_of(join_cols),
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

      date_df <- bind_rows(date_lt10, complement)

      saveRDS(date_df, cf)
      all_records[[d]] <- date_df
    }

    elapsed <- round(as.numeric(difftime(Sys.time(), t_start, units = "secs")), 1)
    grp_lt10 <- if (nrow(lt10_df) > 0) {
      sum(lt10_df$game_date %in% grp_dates)
    } else 0L
    message("    ✓ ", n_grp, " date", if (n_grp > 1) "s",
            "  |  ", grp_lt10, " LT10 rows  |  ", elapsed, "s")

    # ── Pause between groups ──────────────────────────────────────────────
    if (g_idx < n_groups) Sys.sleep(GROUP_PAUSE)
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
## Per-shot imputation of dribble count and defender distance using the
## game-date-level joint tracking distribution from get_game_level_tracking().
##
## PURPOSE:
##   The NBA tracking API reports dribble_range and close_def_dist_range as
##   aggregate counts per player per game date. To assign a value to each
##   individual FGA, we sample from the joint frequency table conditioned on
##   (1) shot clock bucket, (2) period, and (3) shot distance bin.
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
##   1. pgd + specific_period + specific_sc    (player × game-date × period × sc)
##   2. pgd + period=0        + specific_sc    (player × game-date × all periods)
##   3. season + specific_period + specific_sc (player season × period × sc)
##   4. season + period=0        + specific_sc (player season × all periods)
##   5. league + specific_period + specific_sc (league × period × sc)
##   6. league + period=0        + specific_sc (league × all periods)
##   7. league + period=0 + ALL               (league overall — absolute last resort)
##
## If the distance-specific lookup set yields NULL at all 7 levels,
## the combined (all-distance) lookup set is tried as ultimate fallback.
##
## PRE-SAMPLING OVERRIDES (applied per shot before drawing):
##   Override A (definite_non_cs): descriptor contains "pullup", "step-back",
##              or "driving" → zero out "0 Dribbles" row (catch-and-shoot
##              is incompatible with these shot types)
##   Override C (force_zero_drib): tip, alley-oop, or putback descriptor →
##              force "0 Dribbles" (ball goes directly from passer to shooter)
##   Override B (is_contact):     contact shot (blocked, and-1, or fouled) →
##              force "0-2 Feet" column (defender must have been very close)
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
  # Maps a continuous shot clock value to the appropriate SHOT_CLOCK_RANGES
  # label. Shots within SC_BLEND_EPS of a boundary between buckets are
  # assigned a "blended" key (e.g. "15-7 Average||18-15 Early") so that both
  # adjacent matrices are summed when building the weight matrix, reducing
  # cliff-edge sensitivity to PBP timestamp imprecision.
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
  # Returns four matrices (w2 = 2PA, w3 = 3PA, w2m = 2PM, w3m = 3PM), each
  # with DRIBBLE_RANGES as rows and DEF_DIST_RANGES as columns, tallied from
  # the tracking aggregate rows in df.
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
  # Handles blended sc_keys (containing "||") by summing both adjacent buckets.
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
  #
  # Three levels of granularity (most → least specific):
  #   pgd:    player × game_date × period × sc_bucket
  #   ps:     player season × period × sc_bucket (season aggregate)
  #   league: league × period × sc_bucket (ultimate fallback)
  #
  # Within each level, four sub-variants: period-specific vs period=0 (all)
  # × sc-specific vs ALL-sc. That gives 4 × 3 = 12 sub-lookups per set.
  #
  # Returns list(pgd = ..., ps = ..., league = ..., pgd_keys = ...)
  # Each is a named list of 5×4 matrix-sets keyed by the usual scheme.

  build_tracking_lookups <- function(gt) {

    if (is.null(gt) || nrow(gt) == 0)
      return(list(pgd = list(), 
                  ps = list(), 
                  league = list(), 
                  pgd_keys = character()))

    pgd <- list()
    ps  <- list()
    lg  <- list()

    # ── Primary: player × game_date × period × sc_bucket ─────────────────

    # Period-specific × sc-specific
    gt_pgd_per_sc <- gt %>%
      filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
             period %in% seq_len(MAX_PERIOD)) %>%
      mutate(.key = paste0(PLAYER_ID, "|", 
                           game_date, "|", 
                           period, "|", 
                           shot_clock_range))
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
        filter(PLAYER_ID == pts[1], 
               game_date == pts[2], 
               period == as.integer(pts[3]))
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
      group_by(PLAYER_ID, period, shot_clock_range, 
               dribble_range, def_dist_range) %>%
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
  # Tries each level of the fallback chain in order (most → least specific)
  # and returns the first non-NULL result along with a src label.
  # ═══════════════════════════════════════════════════════════════════════════

  resolve_7level <- function(g_pid, g_date, g_per, g_sc, lkps) {
    pgd_per_base    <- paste0(g_pid, "|", g_date, "|", g_per, "|")
    pgd_0_base      <- paste0(g_pid, "|", g_date, "|0|")
    ps_per_base     <- paste0(g_pid, "|",           g_per, "|")
    ps_0_base       <- paste0(g_pid, "|0|")
    league_per_base <- paste0(g_per, "|")

    wl  <- resolve_wl(pgd_per_base, g_sc, lkps$pgd)
    src <- "pgd-period"

    if (is.null(wl)) { 
      wl <- resolve_wl(pgd_0_base, g_sc, lkps$pgd)
      src <- "pgd" 
      }
    if (is.null(wl)) { 
      wl <- resolve_wl(ps_per_base, g_sc, lkps$ps)
      src <- "season-period" 
      }
    if (is.null(wl)) { 
      wl <- resolve_wl(ps_0_base, g_sc, lkps$ps)
      src <- "season" 
      }
    if (is.null(wl)) { 
      wl <- resolve_wl(league_per_base, g_sc, lkps$league)
      src <- "league-period" 
      }
    if (is.null(wl)) { 
      wl <- resolve_wl("0|", g_sc, lkps$league)
      src <- "league"
      }
    if (is.null(wl)) { 
      wl <- lkps$league[["0|ALL"]]
      src <- "league-all" 
      }

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
  # When general_range is present we build three lookup sets:
  #   lookups_lt10:   shots < 10 ft only
  #   lookups_10plus: shots ≥ 10 ft only
  #   lookups_all:    both distances combined
  # Each shot uses the appropriate distance-specific set first, falling back
  # to the combined set if the specific set returns NULL at all 7 levels.
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
  # fga_meta collects per-shot metadata needed for constraint application and
  # group-key construction. Computed once and reused in the sampling loop.
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
  # Within each group, the UNCONSTRAINED weight matrix is resolved once and
  # cached. Then, for each individual shot, per-shot constraints are applied
  # to a copy of the matrix before drawing a single (dr, dd) pair.
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
    # Computed from the unconstrained group matrix (before any per-shot
    # overrides) so it reflects the player's genuine C&S tendency in this
    # context rather than the constrained value used for the actual draw.
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
  # Write sampled dribble and defender-distance values back to the PBP rows.
  # Also compute expected_def_dist by sampling uniformly within the imputed
  # bin's numeric bounds. Contact shots are fixed to [0, 1] ft.
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


  # SUMMARY
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
          "%-3s: n=%-4d  mean_def_dist=%4.2f  contact=%d  cs_rate=%5.3f  p_cs=%5.3f",
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
  
  # Ensure join keys are character
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

  # Defender biometrics
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
  pbp <- impute_shot_context(pbp, game_tracking, seed = seed)

  # Player index mapping
  all_player_ids <- unique(c(pbp$player1_id[pbp$fga == 1 & 
                                              pbp$player1_id != 0],
                             pbp$imputed_def_id[pbp$fga == 1 & 
                                                  !is.na(pbp$imputed_def_id)]))
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
  player_map <- tibble(player_idx = as.integer(pid_to_idx),
                       player_id  = names(pid_to_idx)) %>% 
    arrange(player_idx)

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
## 09 — MASTER FUNCTION   ======================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## build_season_pbp() orchestrates the full 10-step pipeline for a single
## season. Each step reads/writes through a cache layer so interrupted or
## partial runs restart from the last completed checkpoint.
##
## STEP BY STEP:
##   1. Schedule     — get_schedule(season_year)
##   2. PBP          — get_game_with_lineups() per game; saved to
##                     cache/pbp/pbp_<game_id>.rds
##   3. Features     — engineer_features(); shot clock state machine
##   4. Fouls        — link_shooting_fouls()
##   5. Fatigue      — compute_fatigue_features()
##   6. Matchups     — pull_game_matchups() per game; saved to
##                     cache/matchups/matchups_<game_id>.rds
##   7. Defender     — impute_closest_defender()
##   8. Biometrics   — get_biometrics()
##   9. Tracking     — get_game_level_tracking(); cached per date in
##                     cache/shot_tracking_<season>/
##  10. Assembly     — assemble_and_scale(); writes season CSV + maps
##

build_season_pbp <- function(season_year, 
                             game_ids  = NULL, 
                             output_dir = "output",
                             cache_dir  = "cache") {

  season_str <- year_to_season(season_year)

  dir.create(cache_dir,  showWarnings = FALSE, recursive = TRUE)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  message("\n═══════════════════════════════════════════════")
  message("  NBA EPAA data pipeline")
  message("  Season: ", season_str)
  message("═══════════════════════════════════════════════\n")


  # ── 1. Schedule ────────────────────────────────────────────────────────────
  message("  [1/10] Schedule…")
  schedule <- get_schedule(season_year)

  if (!is.null(game_ids)) {
    schedule <- schedule %>% filter(game_id %in% game_ids)
    message("        Filtered to ", nrow(schedule), " game(s)")
  }

  message("        ", nrow(schedule), " games from ",
          min(schedule$game_date), " to ", max(schedule$game_date))


  # ── 2. PBP with lineups ────────────────────────────────────────────────────
  message("\n  [2/10] PBP with lineups…")

  pbp_cache_dir <- file.path(cache_dir, "pbp")
  dir.create(pbp_cache_dir, showWarnings = FALSE)

  pbp_list <- lapply(schedule$game_id, function(gid) {
    cf <- file.path(pbp_cache_dir, paste0("pbp_", gid, ".rds"))
    if (file.exists(cf)) {
      readRDS(cf)
    } else {
      df <- get_game_with_lineups(as.character(gid))
      if (!is.null(df)) saveRDS(df, cf)
      df
    }
  })

  pbp_raw <- bind_rows(pbp_list[!sapply(pbp_list, is.null)])
  message("        ", nrow(pbp_raw), " events from ",
          n_distinct(pbp_raw$game_id), " games")


  # ── 3. Feature engineering ────────────────────────────────────────────────
  message("\n  [3/10] Feature engineering…")
  pbp <- engineer_features(pbp_raw)

  fga_total <- sum(pbp$fga == 1, na.rm = TRUE)
  message("        ", fga_total, " FGA  |  ",
          sum(pbp$fgm == 1, na.rm = TRUE), " FGM  |  FG% = ",
          round(sum(pbp$fgm == 1, na.rm = TRUE) / fga_total, 3))


  # ── 4. Foul linkage ───────────────────────────────────────────────────────
  message("\n  [4/10] Foul-drawing linkage…")
  pbp <- link_shooting_fouls(pbp)
  message(" and-1 FGA: ", sum(pbp$drew_and1 == 1, na.rm = TRUE))
  message(" Shooting foul FGA: ", sum(pbp$had_shooting_foul == 1, na.rm = TRUE))
  message(" Contact shots: ", sum(pbp$is_contact_shot == 1, na.rm = TRUE))


  # ── 5. Fatigue ────────────────────────────────────────────────────────────
  message("\n  [5/10] Fatigue features…")
  pbp <- compute_fatigue_features(pbp, verbose = TRUE)


  # ── 6. Matchup data ───────────────────────────────────────────────────────
  message("\n  [6/10] Matchup data…")

  matchup_cache_dir <- file.path(cache_dir, "matchups")
  dir.create(matchup_cache_dir, showWarnings = FALSE)

  matchup_list <- lapply(schedule$game_id, function(gid) {
    cf <- file.path(matchup_cache_dir, paste0("matchups_", gid, ".rds"))
    if (file.exists(cf)) {
      readRDS(cf)
    } else {
      df <- pull_game_matchups(as.character(gid))
      if (!is.null(df)) saveRDS(df, cf)
      df
    }
  })

  matchups <- bind_rows(matchup_list[!sapply(matchup_list, is.null)])
  message("        ", nrow(matchups), " matchup rows from ",
          n_distinct(matchups$game_id), " games")


  # ── 7. Defender imputation ────────────────────────────────────────────────
  message("\n  [7/10] Defender imputation…")
  pbp <- impute_closest_defender(pbp, matchups, schedule, verbose = TRUE)

  fga_idx <- which(pbp$fga == 1)
  coverage <- round(100 * mean(!is.na(pbp$imputed_def_id[fga_idx])), 1)
  message("        Coverage: ", coverage, "%")


  # ── 8. Biometrics ─────────────────────────────────────────────────────────
  message("\n  [8/10] Biometrics…")
  bio <- get_biometrics(season_str, season_year)
  message("        ", nrow(bio), " players with height/wingspan data")


  # ── 9. Tracking aggregates ────────────────────────────────────────────────
  message("\n  [9/10] Game-level tracking (dribble × def-dist)…")
  game_tracking <- get_game_level_tracking(schedule, season_str,
                                           cache_dir = cache_dir, pbp = pbp)

  message("        ", nrow(game_tracking), " tracking rows  |  ",
          n_distinct(game_tracking$PLAYER_ID), " players")


  # ── 10. Assembly + scaling ────────────────────────────────────────────────
  message("\n  [10/10] Assembly and scaling…")
  pbp_final <- assemble_and_scale(pbp, schedule, bio, game_tracking)

  # Write output
  player_map    <- attr(pbp_final, "player_map")
  defteam_map   <- attr(pbp_final, "defteam_map")
  scaling_params <- attr(pbp_final, "scaling_params")

  write_csv(player_map,     file.path(output_dir, "player_map.csv"))
  write_csv(defteam_map,    file.path(output_dir, "defteam_map.csv"))
  write_csv(scaling_params, file.path(output_dir, "scaling_params.csv"))

  message("\n  ✓ Season ", season_str, " complete")
  message("    FGA rows: ", sum(pbp_final$fga == 1, na.rm = TRUE))
  message("    Total rows: ", nrow(pbp_final))

  pbp_final
}





## ═════════════════════════════════════════════════════════════════════════════
## 10 — SEASON BUILDER  ========================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## build_season_pbp_inChunks() processes a full season in date chunks,
## writing each chunk to a CSV file before proceeding to the next.
##

build_season_pbp_inChunks <- function(season_year, 
                                      chunk_size  = 5,
                                      output_dir  = "output", 
                                      overwrite   = FALSE,
                                      ...) {

  season_str <- year_to_season(season_year)
  season_tag <- gsub("-", "_", season_str)
  chunk_dir  <- file.path(output_dir, paste0("pbp_", season_tag))

  dir.create(chunk_dir, showWarnings = FALSE, recursive = TRUE)

  message("\n═══════════════════════════════════════════════════════")
  message("  Chunked season builder: ", season_str,
          "  (chunk_size = ", chunk_size, ")")
  message("  Output dir: ", chunk_dir)
  message("═══════════════════════════════════════════════════════\n")

  schedule   <- get_schedule(season_year)
  game_ids   <- schedule$game_id
  n_games    <- length(game_ids)
  n_chunks   <- ceiling(n_games / chunk_size)

  # Identify existing chunks
  existing_csvs <- list.files(chunk_dir,
                              pattern = "^pbp_\\d{8}_\\d{8}\\.csv$",
                              full.names = FALSE)
  message("  ", n_games, " games → ", n_chunks, " chunks  |  ",
          length(existing_csvs), " chunks already on disk")

  all_paths <- character(n_chunks)

  for (k in seq_len(n_chunks)) {

    chunk_ids  <- game_ids[((k - 1) * chunk_size + 1):min(k * chunk_size, n_games)]

    chunk_dates <- schedule %>%
      filter(game_id %in% chunk_ids) %>%
      pull(game_date) %>%
      as.Date() %>%
      sort()

    date_from   <- format(min(chunk_dates), "%Y%m%d")
    date_to     <- format(max(chunk_dates), "%Y%m%d")
    chunk_fname <- paste0("pbp_", date_from, "_", date_to, ".csv")
    chunk_path  <- file.path(chunk_dir, chunk_fname)
    all_paths[k] <- chunk_path

    if (!overwrite && file.exists(chunk_path)) {
      message("[", k, "/", n_chunks, "] ", chunk_fname, " already exists — skipping")
      next
    }

    message("\n  [", k, "/", n_chunks, "] Chunk ", k, "/", n_chunks,
            "  games ", chunk_ids[1], " → ", tail(chunk_ids, 1),
            "  dates ", date_from, "–", date_to)

    chunk_pbp <- build_season_pbp(season_year,
                                  game_ids   = chunk_ids,
                                  output_dir = file.path(chunk_dir, "meta"),
                                  ...)
    write_csv(chunk_pbp, chunk_path)

    message("  ✓ Written: ", chunk_fname,
            "  (", nrow(chunk_pbp), " rows)")

    rm(chunk_pbp); gc()
  }

  message("\n  Chunked build complete: ", length(all_paths), " chunks")
  invisible(all_paths)
}





## ═════════════════════════════════════════════════════════════════════════════
## 11 — MULTI-SEASON BUILDER   =================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## build_multi_season_pbp() stacks multiple seasons into a single consistent
## data frame, then reassigns globally consistent player_idx, defteam_idx,
## and defender_idx across all seasons.
##

build_multi_season_pbp <- function(season_years, 
                                   existing_player_map  = NULL,
                                   existing_defteam_map = NULL,
                                   chunk_dirs           = NULL,
                                   output_dir           = "output",
                                   ...) {

  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  message("\n═══════════════════════════════════════════════════════")
  message("  Multi-season build: ", paste(season_years, collapse = ", "))
  message("═══════════════════════════════════════════════════════\n")

  season_list <- lapply(season_years, function(yr) {

    if (!is.null(chunk_dirs) && as.character(yr) %in% names(chunk_dirs)) {
      # Load from pre-built chunk CSV files
      cdir <- chunk_dirs[[as.character(yr)]]
      csv_files <- sort(list.files(cdir, pattern = "^pbp_\\d{8}_\\d{8}\\.csv$",
                                   full.names = TRUE))
      if (length(csv_files) == 0) {
        warning("  No chunk CSVs found in ", cdir, " — running fresh build")
        return(build_season_pbp(yr, output_dir = output_dir, ...))
      }
      message("  Loading ", length(csv_files), " chunks for season ", yr, "…")
      bind_rows(lapply(csv_files, read_csv, show_col_types = FALSE))
    } else {
      build_season_pbp(yr, output_dir = output_dir, ...)
    }
  })

  pbp_all <- bind_rows(season_list)

  message("\n  Stacked: ", nrow(pbp_all), " rows across ",
          n_distinct(pbp_all$season), " seasons")


  # Reassign globally consistent indices

  valid_id <- function(x) !is.na(x) & x != "" & x != "NA" & x != "0"

  # All player_ids that appear as shooter or defender across all seasons
  all_pids <- unique(c(
    pbp_all$player1_id[pbp_all$fga == 1 & valid_id(pbp_all$player1_id)],
    pbp_all$imputed_def_id[pbp_all$fga == 1 & valid_id(pbp_all$imputed_def_id)]
  ))
  all_pids <- sort(all_pids[!is.na(all_pids)])

  # If an existing map is provided, preserve old indices and append new players
  if (!is.null(existing_player_map)) {
    old_pids     <- existing_player_map$player_id
    new_pids     <- setdiff(all_pids, old_pids)
    max_old_idx  <- max(existing_player_map$player_idx)
    new_entries  <- tibble(player_idx = seq(max_old_idx + 1,
                                            max_old_idx + length(new_pids)),
                           player_id  = new_pids)
    player_map   <- bind_rows(existing_player_map, new_entries)
    message("  Player idx: ", length(old_pids), " existing + ",
            length(new_pids), " new = ", nrow(player_map), " total")
  } else {
    player_map <- tibble(player_idx = seq_along(all_pids), player_id = all_pids)
    message("  Player idx: ", nrow(player_map), " unique player_ids")
  }

  pid_to_idx <- setNames(player_map$player_idx, player_map$player_id)

  # Defensive team index
  all_teams <- sort(unique(pbp_all$def_team_abbr[!is.na(pbp_all$def_team_abbr)]))

  if (!is.null(existing_defteam_map)) {
    old_teams    <- existing_defteam_map$def_team_abbr
    new_teams    <- setdiff(all_teams, old_teams)
    max_old_tidx <- max(existing_defteam_map$defteam_idx)
    new_team_entries <- tibble(
      defteam_idx  = seq(max_old_tidx + 1, max_old_tidx + length(new_teams)),
      def_team_abbr = new_teams
    )
    defteam_map <- bind_rows(existing_defteam_map, new_team_entries)
  } else {
    defteam_map <- tibble(defteam_idx   = seq_along(all_teams),
                          def_team_abbr = all_teams)
  }

  team_to_idx <- setNames(defteam_map$defteam_idx, defteam_map$def_team_abbr)

  # Apply globally consistent indices
  pbp_all <- pbp_all %>%
    mutate(player_idx   = pid_to_idx[as.character(player1_id)],
           defender_idx = pid_to_idx[as.character(imputed_def_id)],
           defteam_idx  = team_to_idx[def_team_abbr])

  # Write consolidated maps
  write_csv(player_map,  file.path(output_dir, "player_map.csv"))
  write_csv(defteam_map, file.path(output_dir, "defteam_map.csv"))

  attr(pbp_all, "player_map")  <- player_map
  attr(pbp_all, "defteam_map") <- defteam_map

  message("\n  ✓ Multi-season build complete")
  message("    Total rows: ", nrow(pbp_all))
  message("    FGA rows:   ", sum(pbp_all$fga == 1, na.rm = TRUE))
  message("    Players:    ", nrow(player_map))
  message("    Def teams:  ", nrow(defteam_map))

  pbp_all
}




## ═════════════════════════════════════════════════════════════════════════════
## 12 — USAGE   =================================================================
## ═════════════════════════════════════════════════════════════════════════════
##
## Example invocations. Uncomment the appropriate block to run.

## ─── Full 2024-25 season ────────────────────────────────────────────────────
# pbp_all <- build_season_pbp(2024, output_dir = "output/2024_25")

## ─── Chunked scrape (5 games per chunk, resumable) ─────────────────────────
# chunk_paths <- build_season_pbp_inChunks(2024, chunk_size = 5,
#                                          output_dir = "output/2024_25")

## ─── Load stacked chunks after chunked build ────────────────────────────────
# library(vroom)
# csv_files <- sort(list.files("output/2024_25/pbp_2024_2025",
#                              pattern = "^pbp_\\d{8}_\\d{8}\\.csv$",
#                              full.names = TRUE))
# pbp_all <- vroom(csv_files, show_col_types = FALSE) %>%
#   as.data.frame() %>%
#   mutate(across(where(is.character), ~ na_if(., "NA")))

## ─── Multi-season build ──────────────────────────────────────────────────────
# pbp_multi <- build_multi_season_pbp(c(2023, 2024),
#                                     output_dir = "output/multi_season",
#                                     chunk_dirs = list("2023" = "output/2023_24",
#                                                       "2024" = "output/2024_25"))

## ─── Single-game test (fast, for debugging) ─────────────────────────────────
# schedule_24 <- get_schedule(2024)
# pbp_test <- build_season_pbp(2024,
#                              game_ids   = schedule_24$game_id[1],
#                              output_dir = "output/test")

## ─── After build: apply column renames for Stage 1/2 ────────────────────────
# pbp_all <- pbp_all %>%
#   rename(
#     xLoc                = x_legacy,
#     yLoc                = y_legacy,
#     ...
#   )
## ─── Live calls ─────────────────────────────────────────────────────────────
## The lines below run as part of the pipeline. Comment out as needed.
## Test with 1 game
# schedule <- get_schedule(2024)
# test1 <- build_season_pbp(2024, game_ids = schedule$game_id[1])

## Test with first 5 games
# test5 <- build_season_pbp(2024, game_ids = schedule$game_id[1:5])

## Full season in chunks of 5 game-dates
build_season_pbp_inChunks(2024, 5)
build_season_pbp_inChunks(2023, 5)

## Full season in one go
# pbp_2024 <- build_season_pbp(2024)

## Multi-season (consistent indices across seasons)
# multi <- build_multi_season_pbp(c(2023, 2024), chunk_size = 5)

get_lt10_tracking(schedule_24, "2024-25")




## ─── Post-build: rename + relocate columns for Stage 1/2 consumption ────────
## After loading the stacked chunk CSVs, apply these renames so column names
## match the expected schema in 02_shotQuality.R and 03_shootingTalent.R.
## Internal pipeline names (e.g. imputed_def_id, shooter_min_game) are
## replaced with cleaner Stage 1/2 names (defender_id, player_min_game, etc.).
## Columns used only within the data pipeline are dropped entirely.
## The relocate() chain puts columns in a consistent viewer-friendly order.

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


## ─── Quick test: date grouping ────────────────────────────────────────────────
## Validates that .build_date_groups() produces non-overlapping groups (no team
## appears twice in a single group) and estimates the API call savings from
## grouping vs. scraping each date independently.
if (FALSE) {
  schedule_24 <- get_schedule(2024)
  groups <- .build_date_groups(schedule_24)

  cat("Total game dates:", n_distinct(schedule_24$game_date), "\n")
  cat("Number of groups:", length(groups), "\n")
  cat("Group sizes:\n")
  print(table(sapply(groups, length)))

  # Verify no team appears twice in any group
  all_ok <- TRUE
  for (i in seq_along(groups)) {
    grp_teams <- schedule_24 %>%
      filter(as.character(game_date) %in% groups[[i]]) %>%
      { c(.$home_team_id, .$away_team_id) }
    if (any(duplicated(grp_teams))) {
      cat("  ✗ Group", i, "has duplicate team(s)!\n")
      all_ok <- FALSE
    }
  }
  if (all_ok) cat("  ✓ All groups are team-disjoint\n")

  # Estimate call reduction
  combos_per_period <- length(SHOT_CLOCK_RANGES) * length(DRIBBLE_RANGES) *
                       length(DEF_DIST_RANGES) * length(GENERAL_RANGES)
  date_max_per <- schedule_24 %>%
    mutate(game_date = as.character(game_date)) %>%
    group_by(game_date) %>%
    summarise(.max_per = .max_period_from_status(game_status_text),
              .groups = "drop")
  dmp <- setNames(pmin(date_max_per$.max_per, MAX_PERIOD),
                  date_max_per$game_date)

  ungrouped <- sum(vapply(names(dmp), function(d)
    as.integer(dmp[d]) * combos_per_period, integer(1L)))
  grouped <- sum(vapply(groups, function(grp)
    as.integer(max(dmp[grp])) * combos_per_period, integer(1L)))

  cat("\nAPI calls — ungrouped:", ungrouped, "\n")
  cat("API calls — grouped: ", grouped, "\n")
  cat("Reduction:", round((1 - grouped / ungrouped) * 100, 1), "%\n")
}


## ─── Quick single-group test: grouped scrape with TEAM_ID recovery ───────────
## Scrapes a small date group with both General Ranges, recovers game_date
## via TEAM_ID join, de-overlaps per date, and verifies the result.
## Useful for confirming the team → date mapping works correctly before
## running the full season scrape.
if (FALSE) {
  schedule_24 <- get_schedule(2024)
  groups <- .build_date_groups(schedule_24)

  # Pick the first multi-date group
  multi_grps <- groups[sapply(groups, length) > 1]
  test_grp <- multi_grps[[1]]
  cat("Test group:", paste(test_grp, collapse = ", "), "\n")

  # Show which teams play on each date in this group
  for (d in test_grp) {
    teams <- schedule_24 %>%
      filter(as.character(game_date) == d) %>%
      { paste(.$home_team_tricode, "vs", .$away_team_tricode) }
    cat("  ", d, ":", paste(teams, collapse = "; "), "\n")
  }

  # Full combo grid: both "" and "Less Than 10 ft"
  test_max_per <- 4L
  test_combos <- expand.grid(
    period               = seq_len(test_max_per),
    shot_clock_range     = SHOT_CLOCK_RANGES,
    dribble_range        = DRIBBLE_RANGES,
    close_def_dist_range = DEF_DIST_RANGES,
    general_range        = GENERAL_RANGES,
    stringsAsFactors     = FALSE,
    KEEP.OUT.ATTRS       = FALSE
  )

  date_from_mdy <- format(as.Date(min(test_grp)), "%m/%d/%Y")
  date_to_mdy   <- format(as.Date(max(test_grp)), "%m/%d/%Y")
  cat("Scraping", date_from_mdy, "→", date_to_mdy,
      " (", nrow(test_combos), " combos, ", N_WORKERS, " workers)…\n")

  t0 <- Sys.time()
  grp_raw <- .scrape_combos_parallel(
    test_combos, N_WORKERS,
    date_mdy    = date_from_mdy,
    season_str  = "2024-25",
    date_to_mdy = date_to_mdy,
    call_delay  = CALL_DELAY,
    label       = "test  ")
  elapsed_scrape <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
  cat("Raw rows:", nrow(grp_raw), " |", elapsed_scrape, "s\n")

  # Identify team column
  tid_col <- intersect(names(grp_raw),
                       c("PLAYER_LAST_TEAM_ID", "TEAM_ID"))
  cat("Team ID column found:", tid_col[1], "\n")

  # Build team→date map and recover game_date
  team_date_lookup <- dplyr::bind_rows(
    schedule_24 %>% dplyr::mutate(game_date = as.character(game_date)) %>%
      dplyr::transmute(TEAM_ID = as.character(home_team_id), game_date),
    schedule_24 %>% dplyr::mutate(game_date = as.character(game_date)) %>%
      dplyr::transmute(TEAM_ID = as.character(away_team_id), game_date)
  ) %>% dplyr::distinct()

  grp_df <- grp_raw %>%
    dplyr::transmute(
      PLAYER_ID = as.character(PLAYER_ID),
      .TEAM_ID  = as.character(.data[[tid_col[1]]]),
      period    = as.integer(period),
      shot_clock_range, dribble_range,
      def_dist_range = def_dist_range,
      general_range,
      fg2a = as.integer(FG2A), fg3a = as.integer(FG3A),
      fg2m = as.integer(FG2M), fg3m = as.integer(FG3M),
      fgm  = as.integer(FGM),  fga  = as.integer(FGA)
    ) %>%
    dplyr::left_join(
      team_date_lookup %>% dplyr::filter(game_date %in% test_grp),
      by = c(".TEAM_ID" = "TEAM_ID")
    ) %>%
    dplyr::select(-.TEAM_ID)

  cat("\nUnmapped rows:", sum(is.na(grp_df$game_date)), "\n")

  cat("\nPre-deoverlap — rows by game_date × general_range:\n")
  print(grp_df %>%
          dplyr::group_by(game_date, general_range) %>%
          dplyr::summarise(rows = dplyr::n(), total_fga = sum(fga),
                           players = dplyr::n_distinct(PLAYER_ID),
                           .groups = "drop"))

  # ── De-overlap per date ──────────────────────────────────────────────────
  join_cols <- c("PLAYER_ID", "game_date", "period",
                 "shot_clock_range", "dribble_range", "def_dist_range")

  deoverlapped <- dplyr::bind_rows(lapply(test_grp, function(d) {
    date_df <- grp_df %>% dplyr::filter(game_date == d)
    if (nrow(date_df) == 0) return(date_df)

    overall <- date_df %>% dplyr::filter(general_range == "")
    lt10    <- date_df %>% dplyr::filter(general_range == "Less Than 10 ft")

    if (nrow(lt10) == 0) return(date_df)   # nothing to de-overlap

    gt10 <- overall %>%
      dplyr::left_join(
        lt10 %>% dplyr::select(dplyr::all_of(join_cols),
                               fg2a_lt = fg2a, fg3a_lt = fg3a,
                               fg2m_lt = fg2m, fg3m_lt = fg3m,
                               fgm_lt  = fgm,  fga_lt  = fga),
        by = join_cols) %>%
      dplyr::mutate(
        dplyr::across(dplyr::ends_with("_lt"), ~ dplyr::coalesce(.x, 0L)),
        fg2a = fg2a - fg2a_lt,
        fg3a = fg3a - fg3a_lt,
        fg2m = fg2m - fg2m_lt,
        fg3m = fg3m - fg3m_lt,
        fgm  = fgm  - fgm_lt,
        fga  = fga  - fga_lt) %>%
      dplyr::select(-dplyr::ends_with("_lt")) %>%
      dplyr::filter(fga > 0L)

    dplyr::bind_rows(lt10, gt10)
  }))

  cat("\n=== Post-deoverlap results ===\n")
  cat("Total rows:", nrow(deoverlapped), "\n")
  cat("\nRows by game_date × general_range:\n")
  print(deoverlapped %>%
          dplyr::group_by(game_date, general_range) %>%
          dplyr::summarise(rows = dplyr::n(), total_fga = sum(fga),
                           players = dplyr::n_distinct(PLAYER_ID),
                           .groups = "drop"))

  # Reconcile: per-date FGA should roughly match Overall pre-deoverlap
  cat("\nReconciliation per date (Overall FGA vs deoverlapped LT10 + complement):\n")
  for (d in test_grp) {
    pre_fga  <- grp_df %>%
      dplyr::filter(game_date == d, general_range == "") %>%
      dplyr::pull(fga) %>% sum()
    post_fga <- deoverlapped %>%
      dplyr::filter(game_date == d) %>%
      dplyr::pull(fga) %>% sum()
    delta <- post_fga - pre_fga
    cat("  ", d, ":  Overall FGA =", pre_fga,
        "  |  Deoverlapped FGA =", post_fga,
        "  |  Δ =", delta, "\n")
  }

  cat("\ngeneral_range distribution (all dates):\n")
  print(table(deoverlapped$general_range, useNA = "ifany"))
}




## Missing games diagnostic
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


## ─── CSV type-check with vroom ───────────────────────────────────────────────
## After a chunked build, stacks all chunk CSVs using vroom and calls
## problems() to identify any type-inference failures. Type mismatches
## occur when a column is all-NA in one chunk (inferred as logical) but
## contains data in another (inferred as character or integer). The affected
## column names and file paths help diagnose which chunks need fixing.
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
