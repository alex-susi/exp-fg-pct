## ═════════════════════════════════════════════════════════════════════════════
## 01_data_pipeline.R
## NBA xFG v3 — Full data pipeline (test-friendly)
##
## Entry points:
##   build_season_pbp(2024)                          → full season
##   build_season_pbp(2024, game_ids = "0022400061") → single game test
##   build_season_pbp(2024, game_ids = schedule$game_id[1:5]) → first 5 games
##
## All intermediate results are cached to cache/ so re-runs skip API calls.
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

options(dplyr.summarise.inform = FALSE)


## ─── Tracking constants ──────────────────────────────────────────────────────
## These are the only valid values accepted by the NBA Stats API for the
## dribble_range, close_def_dist_range, and shot_clock_range parameters.
## Order matters: DRIBBLE_RANGES and DEF_DIST_RANGES are the row/column
## dimensions of the 5×4 weight matrices; SHOT_CLOCK_RANGES is ordered
## from most to least time remaining (matches API ordering).

DRIBBLE_RANGES <- c("0 Dribbles", "1 Dribble",  "2 Dribbles",
                    "3-6 Dribbles", "7+ Dribbles")

DEF_DIST_RANGES <- c("0-2 Feet - Very Tight", "2-4 Feet - Tight",
                     "4-6 Feet - Open",        "6+ Feet - Wide Open")

SHOT_CLOCK_RANGES <- c("24-22", "22-18 Very Early", "18-15 Early",
                        "15-7 Average", "7-4 Late", "4-0 Very Late")

## Numeric upper bounds of each sc bucket (seconds remaining on shot clock).
## SC_BOUNDARIES[k] is the threshold between SHOT_CLOCK_RANGES[k] and [k+1].
## E.g., SC_BOUNDARIES[1] = 22 → shots with sc > 22 land in "24-22",
##                                shots with 18 < sc ≤ 22 land in "22-18 Very Early".
SC_BOUNDARIES <- c(22, 18, 15, 7, 4)   # length = length(SHOT_CLOCK_RANGES) - 1

## Half-width of the ambiguity window around each boundary (seconds).
## A shot whose shot_clock falls within SC_BLEND_EPS of a boundary gets
## the two adjacent buckets' matrices SUMMED rather than choosing a side.
## 0.5s is appropriate given that our shot_clock is estimated from PBP
## timestamps and may have sub-second imprecision.
SC_BLEND_EPS <- 0.5

## Maximum period to scrape (1–4 = regulation, 5–7 = OT1/OT2/OT3).
## Period 0 is a synthetic key meaning "all periods summed" and is used
## as a fallback when a specific-period lookup fails.
MAX_PERIOD <- 7L


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

.NON_RETRYABLE_PATTERNS <- c(
  "df_list",
  "object .* not found",
  "subscript out of bounds",
  "no applicable method"
)

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
  
  home_lineups <- vector("list", nrow(pbp))
  away_lineups <- vector("list", nrow(pbp))
  
  
  # Adjusts lineups for substitutions
  for (i in seq_len(nrow(pbp))) {
    play <- pbp[i, ]
    if (!is.na(play$action_type) &&
        tolower(play$action_type) == "substitution") {
      pid     <- as.character(play$player1_id)
      tid     <- as.character(play$team_id)
      sub_dir <- tolower(as.character(play$sub_type))
      lineup  <- if (tid == home_team_id) cur_home else cur_away
      if (grepl("out", sub_dir)) {
        idx <- which(lineup == pid)
        if (length(idx) > 0) lineup[idx[1]] <- NA_character_
      } else if (grepl("in", sub_dir)) {
        na_idx <- which(is.na(lineup))
        if (length(na_idx) > 0) lineup[na_idx[1]] <- pid
      }
      if (tid == home_team_id) cur_home <- lineup else cur_away <- lineup
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
           angle         = atan2(abs(x_legacy), pmax(y_legacy, 1e-1)) * (180 / pi),
    
           # shot classification
           is_three = as.integer(str_detect(action_type, "3pt")),
           is_dunk  = as.integer(str_detect(toupper(coalesce(sub_type, "")), "DUNK")),
           is_rim   = as.integer((area == "Restricted Area" | shot_distance < 4) &
                                   fga == 1),
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
           is_2ndchance    = as.integer(
             str_detect(coalesce(qualifiers, ""), "2ndchance")),
           is_fastbreak    = as.integer(
             str_detect(coalesce(qualifiers, ""), "fastbreak")),
           is_fromturnover = as.integer(
             str_detect(coalesce(qualifiers, ""), "fromturnover")),
           
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
                                         (action_type == "jumpball" & 
                                            # jump ball at period start => possession ends
                                            ((lag(action_type) == "period" & 
                                                lag(sub_type) == "start") |
                                               # otherwise: ends iff offense_team_id != team_id
                                               !coalesce(suppressWarnings(as.integer(offense_team_id)) ==
                                                           suppressWarnings(as.integer(team_id)),
                                                         FALSE)))),
           possession_id = cumsum(lag(possession_end, default = 0L) == 1L),
           # preserve block/foul IDs for deterministic defender assignment
           block_person_id      = as.character(block_person_id),
           foul_drawn_person_id = as.character(foul_drawn_person_id)) %>%
    group_by(game_id) %>%
    arrange(order, event_num, .by_group = TRUE) %>%
    mutate(time_since_last_event = pmax(coalesce(total_elapsed - lag(total_elapsed), 0), 0),
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
      
      shot_clock <- rep(NA_real_, n)
      
      # state = shot clock "after" previous event (post-event)
      sc_state <- NA_real_
      
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
        
        bump14_if_under <- orb || 
          def3sec || 
          loose_ball_def_foul || 
          nonshoot_def_notpen
        
        made_last_ft <- isTRUE(df$is_last_freethrow[i] == 1L && df$ftm[i] == 1L)
        
        # 1) Initialize at possession start (BETWEEN rows), THEN decrement by dt
        if (period_start) {
          sc_pre  <- 0
          sc_post <- 0
          
        } else if (jump_after_period_start) {
          # shot clock starts on the tip outcome, not 2 seconds earlier
          sc_pre  <- 24
          sc_post <- 24
          
        } else {
          # start-of-possession reset happens BEFORE dt elapses to this event
          # if (new_poss) {
          #   sc_state <- if (!is.na(gc) && gc < 24) NA_real_ else 24
          # }
          
          if (new_poss) {
            
            # game clock at the moment the new possession began:
            # prefer previous-row game clock (possession-end event),
            # otherwise fall back to gc + dt (same idea if prev is missing)
            gc_poss_start <- if (prev_poss_end && !is.na(gc_prev)) gc_prev else {
              if (!is.na(gc)) gc + dt else NA_real_
            }
            
            # your re-phrased rule:
            # if the possession ended with <=24s left, shot clock = game clock
            # otherwise full 24
            sc_state <- if (!is.na(gc_poss_start) && prev_poss_end && gc_poss_start <= 24.0) {
              gc_poss_start
            } else {
              24
            }
          }
          
          sc_pre <- if (is.na(sc_state)) NA_real_ else max(sc_state - dt, 0)
          
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
          
          # 14-second rules that are "only if under 14" (incl ORB edge case)
          # if (bump14_if_under) {
          #   if (!is.na(gc) && gc < 14) {
          #     sc_post <- NA_real_
          #   } else {
          #     if (is.na(sc_post)) sc_post <- 14
          #     else sc_post <- if (sc_post < 14) 14 else sc_post
          #   }
          # }
          
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
            #if (!is.na(sc_pre)  && sc_pre  > gc) sc_pre  <- NA_real_
            if (!is.na(sc_pre)  && sc_pre  > gc) {
              sc_pre  <- gc
              }
            #if (!is.na(sc_post) && sc_post > gc) sc_post <- NA_real_
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
      }
      
      df$shot_clock <- shot_clock
      df
    }) %>%
    ungroup()
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
##   PHASE 2 — PROPORTIONAL SAMPLING
##     For each remaining FGA by shooter S in game G:
##       1. Get on-court opposing defenders
##       2. Look up matchup weights for S in game G:
##          a. Shot-type-specific: 3PA weights for threes, 2PA weights for twos
##          b. Total FGA weights (if no type-specific data for on-court defenders)
##          c. partial_possessions weights (if no FGA data at all)
##          d. Uniform random (no matchup data for any on-court defender)
##       3. Sample one defender proportional to weights
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
  # PHASE 2: PROPORTIONAL SAMPLING
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
  n_fga_type  <- 0L   # shot-type-specific FGA weights
  n_fga_total <- 0L   # total FGA weights (cross-type fallback)
  n_poss      <- 0L   # possession-time weights
  n_uniform   <- 0L   # no matchup data
  
  for (i in remaining_idx) {
    oc <- get_on_court_def(i)
    if (length(oc) == 0) next
    
    gid  <- pbp$game_id[i]
    pid  <- pbp$player1_id[i]
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
    
    # ── Tier 1: Shot-type-specific FGA weights ──
    type_wts <- if (is_3) wl$tpa_weights[mu_idx] else wl$twopa_weights[mu_idx]
    
    if (sum(type_wts) > 0) {
      pbp$imputed_def_id[i] <- wsample(oc_with_data, type_wts)
      pbp$def_source[i]     <- "matchup_fga"
      n_fga_type <- n_fga_type + 1L
      next
    }
    
    # ── Tier 2: Total FGA weights ──
    fga_wts <- wl$fga_weights[mu_idx]
    
    if (sum(fga_wts) > 0) {
      pbp$imputed_def_id[i] <- wsample(oc_with_data, fga_wts)
      pbp$def_source[i]     <- "matchup_fga"
      n_fga_total <- n_fga_total + 1L
      next
    }
    
    # ── Tier 3: Partial possessions weights ──
    poss_wts <- wl$poss_weights[mu_idx]
    
    if (sum(poss_wts) > 0) {
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
            "fga_type=", n_fga_type,
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
    
    big_players <- player_check %>% filter(pbp_fga >= 5)
    if (nrow(big_players) > 0) {
      message("\n    Per-player summary (5+ FGA):")
      message("    ", sprintf("%-20s %5s %5s %7s %8s %5s",
                              "Player", "FGA", "mFGA", "Over%", "PropMAE", "Cor"))
      for (i in seq_len(nrow(big_players))) {
        p <- big_players[i, ]
        cor_str <- if (is.na(p$cor_shares)) {
          "  N/A"
          } else sprintf("%5.2f", p$cor_shares)
        message("    ", sprintf("%-20s %5d %5d  %+5.1f%% %7.3f %s",
                                coalesce(p$off_name, p$off_id),
                                p$pbp_fga, p$matchup_fga_sum,
                                p$overcount_pct,
                                p$proportion_mae, cor_str))
      }
    }
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
## v3: Instead of season-level marginal distributions, we pull the JOINT
## distribution of dribble_range × def_dist_range per player per game date.
##
## Approach:
##   For each unique game date in the schedule subset, call
##   nba_leaguedashplayerptshot() for all combinations of:
##     period              : 1–N where N = max periods on that date
##     shot_clock_range    : 6 bins
##     dribble_range       : 5 bins (0 / 1 / 2 / 3-6 / 7+)
##     close_def_dist_range: 4 bins (0-2 / 2-4 / 4-6 / 6+)
##
##   The API with date_from = date_to = "MM/DD/YYYY" returns every player who
##   attempted a shot matching that filter on that date, across ALL games played
##   that day.  Since each player plays at most one game per date, this gives
##   genuine game-level joint frequencies.
##
##   FG2A and FG3A in the response are raw counts (not frequencies), making
##   them directly usable as sampling weights conditioned on shot type.
##
## Per-date period optimisation:
##   Instead of always scraping 7 periods (4 regulation + 3 OT), we use
##   game_status_text from the schedule to determine the actual max period
##   needed for each game date.  If all games on a date ended in regulation,
##   max_period = 4 → 4 × 6 × 5 × 4 = 480 calls.  If one game went to OT,
##   max_period = 5 → 600 calls.  OT2 → 720, OT3 → 840.  Since ~90% of
##   game dates have no OT games, this saves substantial call volume.
##
## Caching:
##   Each game date is cached individually in cache/trk_dates_v3_<season>/.
##   If the scrape is interrupted, re-running skips any date already on disk.
## ════════════════════════════════════════════════════════════════════════════

## Helper: derive max period count from game_status_text
## "Final" → 4, "Final/OT" → 5, "Final/OT2" → 6, "Final/OT3" → 7
.max_period_from_status <- function(status_texts) {
  ot_numbers <- sapply(status_texts, function(s) {
    if (is.na(s) || !grepl("OT", s, fixed = TRUE)) return(0L)
    ot_part <- sub(".*OT", "", s)            # "" for "Final/OT", "2" for "Final/OT2"
    if (ot_part == "") return(1L)
    n <- suppressWarnings(as.integer(ot_part))
    if (is.na(n)) return(1L)
    n
  }, USE.NAMES = FALSE)
  4L + max(ot_numbers, na.rm = TRUE)         # 4 regulation + max OT periods
}

get_game_level_tracking <- function(schedule, season_str, cache_dir = "cache") {

  # Subdirectory for per-date caches.
  # "v3" distinguishes from v2 caches (120 combos, no period).
  # If you have stale v2 caches, they are ignored here automatically.
  season_tag     <- gsub("-", "", substr(season_str, 1, 7))   # "2024-25" → "202425"
  date_cache_dir <- file.path(cache_dir, paste0("trk_dates_v3_", season_tag))
  dir.create(date_cache_dir, showWarnings = FALSE, recursive = TRUE)

  game_dates <- sort(unique(as.character(schedule$game_date)))
  n_dates    <- length(game_dates)

  # ── Pre-compute max period per date from game_status_text ───────────────
  # If game_status_text is missing (old schedule objects), fall back to MAX_PERIOD.
  has_status <- "game_status_text" %in% names(schedule)
  if (has_status) {
    date_max_period <- schedule %>%
      mutate(game_date = as.character(game_date)) %>%
      group_by(game_date) %>%
      summarise(.max_per = .max_period_from_status(game_status_text),
                .groups = "drop")
    # Named vector for fast lookup:  date_max_per["2024-10-23"] → 4L
    date_max_per <- setNames(
      pmin(date_max_period$.max_per, MAX_PERIOD),   # cap at MAX_PERIOD (7)
      date_max_period$game_date
    )
    total_calls <- sum(vapply(game_dates, function(d) {
      as.integer(date_max_per[d]) *
        length(SHOT_CLOCK_RANGES) * length(DRIBBLE_RANGES) * length(DEF_DIST_RANGES)
    }, integer(1L)))
    ot_dates <- sum(date_max_per[game_dates] > 4L)
    message("  Game-level tracking (v3): ", n_dates, " game-date(s), ",
            ot_dates, " with OT")
    message("  Total API calls: ", total_calls,
            "  (est. ", round(total_calls * 0.7 / 60, 1), " min)")
  } else {
    # Fallback: no game_status_text available, use global MAX_PERIOD
    date_max_per <- setNames(rep(MAX_PERIOD, n_dates), game_dates)
    n_combos <- MAX_PERIOD * length(SHOT_CLOCK_RANGES) * length(DRIBBLE_RANGES) * length(DEF_DIST_RANGES)
    message("  Game-level tracking (v3): ", n_dates, " game-date(s) × ",
            n_combos, " combos (no game_status_text; using MAX_PERIOD=", MAX_PERIOD, ")")
  }
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

    max_per  <- as.integer(date_max_per[date_ymd])
    n_combos <- max_per * length(SHOT_CLOCK_RANGES) * length(DRIBBLE_RANGES) * length(DEF_DIST_RANGES)

    message("  [", d_idx, "/", n_dates, "] ", date_ymd,
            " (", max_per, " periods, ", n_combos, " calls) scraping…",
            appendLF = FALSE)

    combo_results <- vector("list", n_combos)
    k <- 1L

    for (per in seq_len(max_per)) {
      for (sc in SHOT_CLOCK_RANGES) {
        for (dr in DRIBBLE_RANGES) {
          for (dd in DEF_DIST_RANGES) {

            res <- safe_call(
              nba_leaguedashplayerptshot,
              date_from            = date_mdy,
              date_to              = date_mdy,
              season               = season_str,
              season_type          = "Regular Season",
              period               = per,
              shot_clock_range     = sc,
              dribble_range        = dr,
              close_def_dist_range = dd,
              per_mode             = "Totals"
            )

            if (!is.null(res) && !is.null(res$LeagueDashPTShots)) {
              df <- tryCatch(as.data.frame(res$LeagueDashPTShots),
                             error = function(e) NULL)
              if (!is.null(df) && nrow(df) > 0) {
                combo_results[[k]] <- df %>%
                  transmute(
                    PLAYER_ID         = as.character(PLAYER_ID),
                    period            = as.integer(per),
                    shot_clock_range  = sc,
                    dribble_range     = dr,
                    def_dist_range    = dd,
                    fg2a              = as.integer(FG2A),
                    fg3a              = as.integer(FG3A),
                    fg2m              = as.integer(FG2M),
                    fg3m              = as.integer(FG3M),
                    fgm               = as.integer(FGM),
                    fga               = as.integer(FGA)
                  )
              }
            }
            k <- k + 1L
          }
        }
      }
    }

    date_df <- bind_rows(combo_results)
    if (nrow(date_df) > 0) date_df$game_date <- date_ymd

    saveRDS(date_df, cache_file)
    all_records[[d_idx]] <- date_df

    message(" ", nrow(date_df), " rows  |  ",
            n_distinct(date_df$PLAYER_ID), " players")
  }

  out <- bind_rows(all_records)
  message("  Total tracking rows: ", nrow(out),
          "  |  unique players: ", n_distinct(out$PLAYER_ID),
          "  |  dates: ", n_distinct(out$game_date))
  out
}


## ════════════════════════════════════════════════════════════════════════════
## impute_shot_context()  — v3.3
##
## Per-shot imputation using the game-date-level joint tracking distribution,
## additionally conditioned on (1) the shot clock bucket and (2) the period
## in which the shot occurred.
##
## PERIOD CONDITIONING
## ─────────────────────────────────────────────────────────────────────────
## Dribble patterns and defensive positioning can differ by period: Q4
## late-game possessions tend to be more isolation-heavy, OT has fresher
## legs, etc.  Adding period as a dimension gives finer-grained priors at
## the cost of 7× more API calls (840 vs 120 per game date).
##
## Period 0 is a synthetic key meaning "all periods summed" and is used as
## a fallback when a specific-period entry is absent (e.g. a player only
## appeared in one quarter on a given date).
##
## SHOT CLOCK CONDITIONING
## ─────────────────────────────────────────────────────────────────────────
## Each FGA has an imputed shot_clock (seconds remaining) from the earlier
## shot-clock computation step.  We use that value to select which of the
## 6 NBA shot-clock buckets ("24-22", "22-18 Very Early", "18-15 Early",
## "15-7 Average", "7-4 Late", "4-0 Very Late") the shot falls into, and
## condition the (dribble_range, def_dist_range) weight matrix on that
## bucket.  This is a strict improvement: dribble patterns and defensive
## distance differ substantially by shot-clock epoch.
##
## Edge-case handling at bucket boundaries (sc = 22, 18, 15, 7, 4 ± 0.5s):
##   Our shot_clock estimate has sub-second imprecision, so assigning a
##   boundary shot to one side arbitrarily would be wrong.  Instead, within
##   SC_BLEND_EPS (= 0.5s) of any boundary we SUM the two adjacent buckets'
##   weight matrices before expanding the pool.
##
##   Shots with NA shot_clock fall back to the "ALL" matrix (sum over all
##   sc buckets), which is equivalent to v3.1 behaviour with no sc conditioning.
##
## LOOKUP KEY STRUCTURE
## ─────────────────────────────────────────────────────────────────────────
## All three lookup levels use the same key scheme with period as a prefix:
##
##   "<pid>|<gdate>|<period>|<sc_key>"   (primary)
##   "<pid>|<period>|<sc_key>"           (season)
##   "<period>|<sc_key>"                 (league)
##
## where <period> is 1–7 for specific quarters/OT, or 0 for all-periods-summed,
## and <sc_key> is one of:
##   - A single SHOT_CLOCK_RANGES value, e.g. "15-7 Average"
##   - A blend of two adjacent values joined with "||", e.g.
##     "18-15 Early||15-7 Average"  (boundary shots only)
##   - "ALL"  (NA shot_clock fallback)
##
## FALLBACK CHAIN (7 levels, most to least specific)
## ─────────────────────────────────────────────────────────────────────────
##   1. pgd + specific_period + specific_sc   (player × game × period × sc)
##   2. pgd + period=0        + specific_sc   (player × game × all-periods × sc)
##   3. season + specific_period + specific_sc
##   4. season + period=0        + specific_sc
##   5. league + specific_period + specific_sc
##   6. league + period=0        + specific_sc
##   7. league + period=0 + ALL              (absolute last resort)
##
## GROUP-LEVEL BLOCK ASSIGNMENT
## ─────────────────────────────────────────────────────────────────────────
## Grouping key is (pid, gdate, period, is_3, sc_key).  Within each
## sub-group, pool expansion and permutation produce exact cell-count
## matching when pool_total == n_shots.  Proportional sampling is the
## fallback when sizes differ.
##
## POST-HOC OVERRIDES (unchanged from v3.1)
## ─────────────────────────────────────────────────────────────────────────
## Override A (descriptor): if assigned "0 Dribbles" but descriptor is
##   definitively non-C&S, re-sample dribble from the sc-conditioned
##   non-zero-dribble marginal.  def_dist from the pool is preserved.
## Override B (contact):  force def_dist = "0-2 Feet - Very Tight",
##   expected_def_dist ~ U(0, 1).  Dribble from the pool is preserved.
##
## Output columns added per FGA row (same as v3.1):
##   imputed_dribble_range   — character, one of DRIBBLE_RANGES
##   imputed_def_dist_range  — character, one of DEF_DIST_RANGES
##   is_catch_shoot          — integer, 1 iff imputed_dribble_range == "0 Dribbles"
##   p_catch_shoot           — numeric, "0 Dribbles" marginal mass from the
##                             sc-conditioned matrix (pre-override)
##   expected_def_dist       — numeric, U(lo, hi) within the imputed dist bin
## ════════════════════════════════════════════════════════════════════════════

impute_shot_context <- function(pbp, game_tracking, seed = 42, verbose = TRUE) {

  set.seed(seed)

  n_dr <- length(DRIBBLE_RANGES)
  n_dd <- length(DEF_DIST_RANGES)
  n_sc <- length(SHOT_CLOCK_RANGES)

  bin_defs <- list(
    "0-2 Feet - Very Tight" = c(0.0, 2.0),
    "2-4 Feet - Tight"      = c(2.0, 4.0),
    "4-6 Feet - Open"       = c(4.0, 6.0),
    "6+ Feet - Wide Open"   = c(6.0, 10.0)
  )

  non_cs_pattern <- "pullup|step.?back"


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: shot_clock → sc_key
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Returns one of:
  #   - A single SHOT_CLOCK_RANGES string (deterministic assignment)
  #   - Two adjacent strings joined with "||" (boundary blend)
  #   - "ALL" (NA shot_clock)

  sc_to_key <- function(sc) {
    if (is.na(sc) || sc < 0) return("ALL")

    for (b in seq_along(SC_BOUNDARIES)) {
      if (abs(sc - SC_BOUNDARIES[b]) <= SC_BLEND_EPS)
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
    m2m <- matrix(0L, nrow = n_dr, ncol = n_dd,   # fg2m — made 2PA per cell
                  dimnames = list(DRIBBLE_RANGES, DEF_DIST_RANGES))
    m3m <- matrix(0L, nrow = n_dr, ncol = n_dd,   # fg3m — made 3PA per cell
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

  # NULL-safe matrix addition — propagates all four fields
  add_mats <- function(a, b) {
    if (is.null(a)) return(b)
    if (is.null(b)) return(a)
    list(w2  = a$w2  + b$w2,
         w3  = a$w3  + b$w3,
         w2m = a$w2m + b$w2m,
         w3m = a$w3m + b$w3m)
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: resolve weight matrices from a lookup, given a (possibly blend) key
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # For single and "ALL" keys: direct lookup.
  # For blend keys ("A||B"): sum the two adjacent sc-bucket entries.
  # Returns NULL if nothing found (caller falls through to next level).

  resolve_wl <- function(base_key, sc_key, lookup) {
    if (!grepl("||", sc_key, fixed = TRUE)) {
      # Single bucket or "ALL"
      return(lookup[[paste0(base_key, sc_key)]])
    }
    # Blend
    parts <- strsplit(sc_key, "||", fixed = TRUE)[[1]]
    a <- lookup[[paste0(base_key, parts[1])]]
    b <- lookup[[paste0(base_key, parts[2])]]
    add_mats(a, b)
  }
  # resolve_wl handles all three lookup levels uniformly (pgd, season, league).
  # league_lookup keys now use "period|sc_key" format so the same function
  # works for all levels — resolve_wl_league is no longer needed.


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
  # BUILD LOOKUP TABLES
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Each lookup entry is a list with four 5×4 integer matrices:
  #   w2  = fg2a counts  (2PA attempts)     w3  = fg3a counts  (3PA attempts)
  #   w2m = fg2m counts  (2PA makes)        w3m = fg3m counts  (3PA makes)
  #
  # Missed-shot matrices are computed at assignment time as (w_att - w_made),
  # floored to 0 to guard against any tracking vs PBP inconsistencies.
  #
  # Keys use a separator-free prefix scheme:
  #   pgd_lookup[["<pid>|<gdate>|<sc_range>"]]  — single sc bucket or "ALL"
  #   ps_lookup[["<pid>|<sc_range>"]]
  #   league_lookup[["<sc_range>"]]
  #
  # Note the trailing "|" in base_key is included so resolve_wl() can
  # concatenate sc_key directly: paste0(base_key, sc_key).
  # "ALL" entries are the sum over all sc buckets for that (pid, gdate) pair.

  message("  Building period- and sc-conditioned tracking lookup tables…")

  # ── Primary: player × game_date × period × sc_bucket ───────────────────
  #
  # Keys: "pid|gdate|period|sc_key"   (period 1–7, specific sc)
  #       "pid|gdate|period|ALL"      (period 1–7, sc summed)
  #       "pid|gdate|0|sc_key"        (period=0: all periods summed, specific sc)
  #       "pid|gdate|0|ALL"           (period=0: all periods + sc summed)
  pgd_lookup <- list()

  # Period-specific × sc-specific
  gt_pgd_per_sc <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
           period %in% seq_len(MAX_PERIOD)) %>%
    mutate(.key = paste0(PLAYER_ID, "|", game_date, "|", period, "|", shot_clock_range))
  for (k in unique(gt_pgd_per_sc$.key))
    pgd_lookup[[k]] <- build_matrices(gt_pgd_per_sc[gt_pgd_per_sc$.key == k, ])

  # Period-specific × ALL-sc
  gt_pgd_per_all <- game_tracking %>%
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
    pgd_lookup[[paste0(k, "|ALL")]] <- build_matrices(sl)
  }

  # Period=0 × sc-specific (all periods summed, same as previous v3.2 behavior)
  gt_pgd_0_sc <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
    mutate(.key = paste0(PLAYER_ID, "|", game_date, "|0|", shot_clock_range))
  for (k in unique(gt_pgd_0_sc$.key))
    pgd_lookup[[k]] <- build_matrices(gt_pgd_0_sc[gt_pgd_0_sc$.key == k, ])

  # Period=0 × ALL-sc
  gt_pgd_all <- game_tracking %>%
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
    pgd_lookup[[paste0(pgd_k, "|0|ALL")]] <- build_matrices(slice)
  }

  # ── Secondary: player season × period × sc_bucket ──────────────────────
  #
  # Keys: "pid|period|sc_key"   (period 1–7, specific sc)
  #       "pid|period|ALL"      (period 1–7, sc summed)
  #       "pid|0|sc_key"        (period=0: all periods, specific sc)
  #       "pid|0|ALL"           (period=0: all periods + sc summed)
  ps_lookup <- list()

  # Period-specific × sc-specific
  gt_ps_per_sc <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
           period %in% seq_len(MAX_PERIOD)) %>%
    group_by(PLAYER_ID, period, shot_clock_range, dribble_range, def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
              fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
              .groups = "drop") %>%
    mutate(.key = paste0(PLAYER_ID, "|", period, "|", shot_clock_range))
  for (k in unique(gt_ps_per_sc$.key))
    ps_lookup[[k]] <- build_matrices(gt_ps_per_sc[gt_ps_per_sc$.key == k, ])

  # Period-specific × ALL-sc
  gt_ps_per_all <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
           period %in% seq_len(MAX_PERIOD)) %>%
    group_by(PLAYER_ID, period, dribble_range, def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
              fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
              .groups = "drop")
  for (k in unique(paste0(gt_ps_per_all$PLAYER_ID, "|", gt_ps_per_all$period))) {
    pts <- strsplit(k, "\\|")[[1]]
    sl  <- gt_ps_per_all %>%
      filter(PLAYER_ID == pts[1], period == as.integer(pts[2]))
    ps_lookup[[paste0(k, "|ALL")]] <- build_matrices(sl)
  }

  # Period=0 × sc-specific
  gt_ps_0_sc <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
    group_by(PLAYER_ID, shot_clock_range, dribble_range, def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
              fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
              .groups = "drop") %>%
    mutate(.key = paste0(PLAYER_ID, "|0|", shot_clock_range))
  for (k in unique(gt_ps_0_sc$.key))
    ps_lookup[[k]] <- build_matrices(gt_ps_0_sc[gt_ps_0_sc$.key == k, ])

  # Period=0 × ALL-sc
  gt_ps_all <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
    group_by(PLAYER_ID, dribble_range, def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
              fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
              .groups = "drop")
  for (pid in unique(gt_ps_all$PLAYER_ID))
    ps_lookup[[paste0(pid, "|0|ALL")]] <-
      build_matrices(gt_ps_all[gt_ps_all$PLAYER_ID == pid, ])

  # ── Tertiary: league × period × sc_bucket ───────────────────────────────
  #
  # Keys: "period|sc_key"   (period 1–7, specific sc)
  #       "period|ALL"      (period 1–7, sc summed)
  #       "0|sc_key"        (period=0: all periods, specific sc)
  #       "0|ALL"           (absolute last-resort fallback)
  league_lookup <- list()

  # Period-specific × sc-specific
  gt_league_per_sc <- game_tracking %>%
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
        league_lookup[[paste0(per, "|", sc)]] <- build_matrices(sl)
    }
  }

  # Period-specific × ALL-sc
  gt_league_per_all <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES,
           period %in% seq_len(MAX_PERIOD)) %>%
    group_by(period, dribble_range, def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
              fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
              .groups = "drop")
  for (per in seq_len(MAX_PERIOD)) {
    sl <- gt_league_per_all %>% filter(period == per)
    if (nrow(sl) > 0)
      league_lookup[[paste0(per, "|ALL")]] <- build_matrices(sl)
  }

  # Period=0 × sc-specific
  gt_league_0_sc <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
    group_by(shot_clock_range, dribble_range, def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
              fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
              .groups = "drop")
  for (sc in SHOT_CLOCK_RANGES)
    league_lookup[[paste0("0|", sc)]] <-
      build_matrices(gt_league_0_sc[gt_league_0_sc$shot_clock_range == sc, ])

  # Period=0 × ALL-sc (absolute last resort)
  gt_league_all <- game_tracking %>%
    filter(shot_clock_range %in% SHOT_CLOCK_RANGES) %>%
    group_by(dribble_range, def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), fg3a = sum(fg3a, na.rm = TRUE),
              fg2m = sum(fg2m, na.rm = TRUE), fg3m = sum(fg3m, na.rm = TRUE),
              .groups = "drop")
  league_lookup[["0|ALL"]] <- build_matrices(gt_league_all)

  message("  Lookup built: ", length(pgd_all_keys), " player-game-dates  |  ",
          n_distinct(gt_ps_all$PLAYER_ID), " players with season data")


  # ═══════════════════════════════════════════════════════════════════════════
  # ANNOTATE ALL FGA ROWS WITH PER-SHOT METADATA
  # ═══════════════════════════════════════════════════════════════════════════

  # Retrieve shot_clock: prefer imputed shot_clock, use game_clock_sec as fallback
  # (both are in seconds remaining; shot_clock is more specific for our purposes)
  shot_clocks <- coalesce(pbp$shot_clock[fga_idx], pbp$game_clock_sec[fga_idx])

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
    stringsAsFactors = FALSE
  )
  fga_meta$sc_key <- vapply(fga_meta$shot_clock, sc_to_key, character(1L))

  fga_meta$definite_non_cs <-
    #(fga_meta$is_rim == 1L) |
    (!is.na(fga_meta$descriptor) &
       grepl(non_cs_pattern, fga_meta$descriptor, ignore.case = TRUE))

  n_fga        <- nrow(fga_meta)
  res_dr       <- rep(NA_character_, n_fga)
  res_dd       <- rep(NA_character_, n_fga)
  res_p_cs     <- rep(NA_real_,      n_fga)
  res_src      <- rep(NA_character_, n_fga)
  res_assign   <- rep(NA_character_, n_fga)


  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER: expand integer weight matrix into flat pool of (dr, dd) pairs
  # ═══════════════════════════════════════════════════════════════════════════

  expand_pool <- function(w_mat) {
    pool_dr <- character(0)
    pool_dd <- character(0)
    for (ri in seq_len(n_dr)) {
      for (ci in seq_len(n_dd)) {
        cnt <- w_mat[ri, ci]
        if (cnt > 0L) {
          pool_dr <- c(pool_dr, rep(DRIBBLE_RANGES[ri],  cnt))
          pool_dd <- c(pool_dd, rep(DEF_DIST_RANGES[ci], cnt))
        }
      }
    }
    list(dr = pool_dr, dd = pool_dd, total = length(pool_dr))
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # STAGE 1: GROUP-LEVEL BLOCK ASSIGNMENT (FGA-only, no outcome conditioning)
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Group key: (pid, gdate, period, is_3, sc_key)
  #
  # Pools are built from the ATTEMPT matrix only (fg2a / fg3a counts).
  # fgm is intentionally excluded — dribble counts and defender distances
  # are causally prior to the shot outcome; using fgm would introduce
  # data leakage into the XGBoost and Stan models.
  #
  # The wl_cache deduplicates the lookup per group key.

  group_keys    <- paste0(fga_meta$pid,    "|", fga_meta$gdate, "|",
                          fga_meta$period, "|", fga_meta$is_3,  "|",
                          fga_meta$sc_key)
  unique_groups <- unique(group_keys)

  wl_cache <- list()

  for (gk in unique_groups) {

    grp_mask <- which(group_keys == gk)
    n_shots  <- length(grp_mask)

    # Parse: "pid|gdate|period|is_3|sc_key"
    # sc_key may contain "|" (from "||" blend); period and is_3 are integers.
    parts  <- strsplit(gk, "\\|")[[1]]
    g_pid  <- parts[1]
    g_date <- parts[2]
    g_per  <- as.integer(parts[3])
    g_is3  <- as.integer(parts[4])
    g_sc   <- paste(parts[-(1:4)], collapse = "|")

    # ── 7-level fallback: period-specific → period=0, pgd → season → league
    if (is.null(wl_cache[[gk]])) {
      pgd_per_base    <- paste0(g_pid, "|", g_date, "|", g_per, "|")
      pgd_0_base      <- paste0(g_pid, "|", g_date, "|0|")
      ps_per_base     <- paste0(g_pid, "|",           g_per, "|")
      ps_0_base       <- paste0(g_pid, "|0|")
      league_per_base <- paste0(g_per, "|")

      wl  <- resolve_wl(pgd_per_base,    g_sc, pgd_lookup);    src <- "pgd-period"
      if (is.null(wl)) { wl <- resolve_wl(pgd_0_base,      g_sc, pgd_lookup);    src <- "pgd"           }
      if (is.null(wl)) { wl <- resolve_wl(ps_per_base,     g_sc, ps_lookup);     src <- "season-period" }
      if (is.null(wl)) { wl <- resolve_wl(ps_0_base,       g_sc, ps_lookup);     src <- "season"        }
      if (is.null(wl)) { wl <- resolve_wl(league_per_base, g_sc, league_lookup); src <- "league-period" }
      if (is.null(wl)) { wl <- resolve_wl("0|",            g_sc, league_lookup); src <- "league"        }
      if (is.null(wl)) { wl <- league_lookup[["0|ALL"]];                           src <- "league-all"    }

      wl_cache[[gk]] <- list(wl = wl, src = src)
    }

    wl  <- wl_cache[[gk]]$wl
    src <- wl_cache[[gk]]$src

    # ── Shot-type-specific attempt matrix; fall back to combined if empty ──
    w_att <- if (g_is3 == 1L) wl$w3 else wl$w2
    if (sum(w_att) == 0L) w_att <- wl$w2 + wl$w3

    # ── p_catch_shoot: marginal "0 Dribbles" mass from attempt matrix ──
    #all_is_rim <- all(fga_meta$is_rim[grp_mask] == 1L)
    #p_cs_group <- if (sum(w_att) > 0L && !all_is_rim) {
    p_cs_group <- if (sum(w_att) > 0L) {
      sum(w_att["0 Dribbles", ]) / sum(w_att)
    } else 0.0
    res_p_cs[grp_mask] <- p_cs_group
    res_src[grp_mask]  <- src

    # ── Pool from attempt matrix and assign ────────────────────────────
    pool <- expand_pool(w_att)

    if (pool$total == n_shots) {
      perm             <- sample(pool$total)
      res_dr[grp_mask] <- pool$dr[perm]
      res_dd[grp_mask] <- pool$dd[perm]
      res_assign[grp_mask] <- "exact"

    } else if (pool$total > 0L) {
      flat_probs <- as.vector(w_att) / sum(w_att)
      flat_draws <- sample(n_dr * n_dd, n_shots, replace = TRUE, prob = flat_probs)
      r_idx <- ((flat_draws - 1L) %% n_dr) + 1L
      c_idx <- ((flat_draws - 1L) %/% n_dr) + 1L
      res_dr[grp_mask] <- DRIBBLE_RANGES[r_idx]
      res_dd[grp_mask] <- DEF_DIST_RANGES[c_idx]
      res_assign[grp_mask] <- "proportional"

    } else {
      res_dr[grp_mask] <- sample(DRIBBLE_RANGES,  n_shots, replace = TRUE)
      res_dd[grp_mask] <- sample(DEF_DIST_RANGES, n_shots, replace = TRUE)
      res_assign[grp_mask] <- "uniform"
    }
  }


  # ── Diagnostic: tracking vs PBP FGM discrepancy ────────────────────────
  # Count groups where the tracking total (pool size) differs from PBP total.
  # This is a data quality signal, not something the imputation should "fix".
  n_fgm_mismatch <- 0L
  pbp_fgm <- as.integer(pbp$fgm[fga_idx])
  for (gk in unique_groups) {
    grp_mask  <- which(group_keys == gk)
    parts     <- strsplit(gk, "\\|")[[1]]
    g_is3     <- as.integer(parts[4])
    g_sc      <- paste(parts[-(1:4)], collapse = "|")
    wl        <- wl_cache[[gk]]$wl
    w_att     <- if (g_is3 == 1L) wl$w3 else wl$w2
    if (sum(w_att) == 0L) w_att <- wl$w2 + wl$w3
    trk_total <- sum(w_att)
    pbp_total <- length(grp_mask)
    if (trk_total != pbp_total) n_fgm_mismatch <- n_fgm_mismatch + 1L
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # STAGE 2: PER-SHOT POST-HOC OVERRIDES
  # ═══════════════════════════════════════════════════════════════════════════

  n_desc_conflict <- 0L
  n_contact_ovr   <- 0L

  for (j in seq_len(n_fga)) {

    # ── Override A: descriptor conflict ──────────────────────────────────
    if (fga_meta$definite_non_cs[j] && !is.na(res_dr[j]) && res_dr[j] == "0 Dribbles") {

      n_desc_conflict <- n_desc_conflict + 1L

      # Re-sample dribble from period- and sc-conditioned, FGA-based
      # non-zero-dribble marginal.  Use wl_cache (populated in Stage 1)
      # where possible; the else-branch is a defensive fallback only.
      gk_j <- group_keys[j]
      wl_j <- if (!is.null(wl_cache[[gk_j]])) {
        wl_cache[[gk_j]]$wl
      } else {
        g_per_j <- fga_meta$period[j]
        g_sc_j  <- fga_meta$sc_key[j]
        pgd_per_j    <- paste0(fga_meta$pid[j], "|", fga_meta$gdate[j], "|", g_per_j, "|")
        pgd_0_j      <- paste0(fga_meta$pid[j], "|", fga_meta$gdate[j], "|0|")
        ps_per_j     <- paste0(fga_meta$pid[j], "|", g_per_j, "|")
        ps_0_j       <- paste0(fga_meta$pid[j], "|0|")
        wl_jj <- resolve_wl(pgd_per_j, g_sc_j, pgd_lookup)
        if (is.null(wl_jj)) wl_jj <- resolve_wl(pgd_0_j,           g_sc_j, pgd_lookup)
        if (is.null(wl_jj)) wl_jj <- resolve_wl(ps_per_j,          g_sc_j, ps_lookup)
        if (is.null(wl_jj)) wl_jj <- resolve_wl(ps_0_j,            g_sc_j, ps_lookup)
        if (is.null(wl_jj)) wl_jj <- resolve_wl(paste0(g_per_j, "|"), g_sc_j, league_lookup)
        if (is.null(wl_jj)) wl_jj <- resolve_wl("0|",               g_sc_j, league_lookup)
        if (is.null(wl_jj)) wl_jj <- league_lookup[["0|ALL"]]
        wl_jj
      }

      w_att_j <- if (fga_meta$is_3[j] == 1L) wl_j$w3 else wl_j$w2
      if (sum(w_att_j) == 0L) w_att_j <- wl_j$w2 + wl_j$w3

      dr_marginal                <- rowSums(w_att_j)
      dr_marginal["0 Dribbles"] <- 0L

      res_dr[j] <- if (sum(dr_marginal) > 0L) {
        sample(DRIBBLE_RANGES, 1L, prob = dr_marginal / sum(dr_marginal))
      } else "1 Dribble"
    }

    # ── Override B: contact shot ─────────────────────────────────────────
    if (fga_meta$is_contact[j] == 1L) {
      res_dd[j]     <- "0-2 Feet - Very Tight"
      n_contact_ovr <- n_contact_ovr + 1L
    }
  }


  # ═══════════════════════════════════════════════════════════════════════════
  # STAGE 3: WRITE RESULTS TO PBP
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

    # Shot-clock key distribution: single bucket / blend / ALL
    sc_type <- ifelse(fga_meta$sc_key == "ALL", "no-sc",
               ifelse(grepl("||", fga_meta$sc_key, fixed = TRUE), "blend", "single"))
    sc_type_tab <- table(sc_type)
    message("    Shot-clock key type (shots):  ",
            paste(names(sc_type_tab), sc_type_tab, sep = "=", collapse = "  "))

    message("    Post-hoc overrides:  descriptor-conflict=", n_desc_conflict,
            "  contact-def-dist=", n_contact_ovr)

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

    exact_groups <- sum(tapply(res_assign, group_keys, function(x) x[1] == "exact"),
                        na.rm = TRUE)
    total_groups <- length(unique_groups)
    message("    Exact-count groups: ", exact_groups, "/", total_groups,
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
    left_join(bio %>% select(PLAYER_ID, shooter_ht = PLAYER_HEIGHT_INCHES,
                             shooter_ws = wingspan),
              by = c("player1_id" = "PLAYER_ID"))

  # Defender biometrics (using imputed defender)
  pbp <- pbp %>%
    left_join(bio %>% select(PLAYER_ID, defender_ht = PLAYER_HEIGHT_INCHES,
                             defender_ws = wingspan),
              by = c("imputed_def_id" = "PLAYER_ID"))

  league_avg_ht <- median(bio$PLAYER_HEIGHT_INCHES, na.rm = TRUE)
  pbp <- pbp %>%
    mutate(shooter_ht     = coalesce(shooter_ht, league_avg_ht),
           defender_ht    = coalesce(defender_ht, league_avg_ht))#,
           #height_matchup = shooter_ht - defender_ht)


  # Tracking aggregates — per-shot context imputation from game-level joint distributions
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
           A               = safe_scale(angle),
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
              mu_ang  = mean(angle, na.rm = TRUE),
              sd_ang  = sd(angle, na.rm = TRUE),
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
  name_lookup <- fga_rows %>%
    distinct(player1_id, player_name) %>%
    filter(!is.na(player_name))
  player_map <- player_map %>%
    left_join(name_lookup, by = c("player_id" = "player1_id")) %>%
    rename(name = player_name)

  attr(pbp, "player_map") <- player_map
  attr(pbp, "defteam_map")    <- fga_rows %>%
    filter(!is.na(defteam_idx)) %>%
    distinct(defteam_idx, def_team_abbr) %>% arrange(defteam_idx)

  pbp
}





## ═════════════════════════════════════════════════════════════════════════════
## 09 - MASTER FUNCTION   ======================================================
## ═════════════════════════════════════════════════════════════════════════════

build_season_pbp <- function(season_year, game_ids = NULL, cache_dir = "cache") {
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
  tag        <- if (is_test) {
    paste0(season_year, "_test")
    } else as.character(season_year)

  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  cp <- function(name) file.path(cache_dir, paste0(name, "_", tag, ".rds"))

  message("\n══════════════════════════════════════════════════════")
  if (is_test) {
    message("  TEST RUN: ", length(game_ids), " game(s) from ", season_str)
  } else {
    message("  FULL SEASON: ", season_str)
  }
  message("══════════════════════════════════════════════════════\n")


  ## ── 1. Schedule ──────────────────────────────────────────────────────────
  message("─── Step 1: Schedule ───")
  #schedule <- get_schedule(season_year)
  message("  Full season: ", nrow(schedule), " games")

  if (is_test) {
    # Validate game_ids exist in schedule
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
  message("  Scraping PBP for ", nrow(schedule_subset), " game(s)…")
  pbp_raw <- map(schedule_subset$game_id, function(gid) {
    tryCatch(get_game_with_lineups(gid), error = function(e) {
      message("    Error on ", gid, ": ", e$message); NULL
    })
  }, .progress = !is_test) %>% bind_rows()
  saveRDS(pbp_raw, cp("pbp_raw"))
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
  # if (file.exists(cp("matchups"))) {
  #   message("  Loading cached matchups…")
  #   matchups <- readRDS(cp("matchups"))
  # } else {
  #   gids_to_scrape <- unique(pbp$game_id)
  #   message("  Scraping matchups for ", length(gids_to_scrape), " game(s)…")
  #   matchups <- map(gids_to_scrape, pull_game_matchups,
  #                   .progress = !is_test) %>% bind_rows()
  #   saveRDS(matchups, cp("matchups"))
  # }
  gids_to_scrape <- unique(pbp$game_id)
  message("  Scraping matchups for ", length(gids_to_scrape), " game(s)…")
  matchups <- map(gids_to_scrape, pull_game_matchups,
                  .progress = !is_test) %>% bind_rows()
  saveRDS(matchups, cp("matchups"))
  message("  Matchup rows: ", nrow(matchups))

  pbp <- impute_closest_defender(pbp, matchups, schedule_subset)

  coverage <- round(100 * mean(!is.na(pbp$imputed_def_id[pbp$fga == 1])), 1)
  message("  Overall defender coverage: ", coverage, "%")


  ## ── 7. Biometrics ──────────────────────────────────────────────────────
  # Biometrics are season-wide (not game-specific), so use season-level cache
  message("─── Step 7: Biometrics ───")
  bio_cache <- file.path(cache_dir, paste0("bio_", season_year, ".rds"))
  if (file.exists(bio_cache)) {
    message("  Loading cached biometrics…")
    bio <- readRDS(bio_cache)
  } else {
    bio <- get_biometrics(season_str, season_year)
    saveRDS(bio, bio_cache)
  }
  bio <- get_biometrics(season_str, season_year)
  saveRDS(bio, bio_cache)
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

  write_csv(pbp %>% select(-any_of("on_court_defenders")),
            paste0("pbp_", tag, ".csv"))
  write_csv(scaling_params, paste0("scaling_params_", tag, ".csv"))
  write_csv(player_map,     paste0("player_map_", tag, ".csv"))
  write_csv(defteam_map,    paste0("defteam_map_", tag, ".csv"))
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
## 10 - USAGE   ================================================================
## ═════════════════════════════════════════════════════════════════════════════

## ── Test with 1 game ──
schedule <- get_schedule(2024)
test1 <- build_season_pbp(2024, game_ids = schedule$game_id[1])
test2 <- build_season_pbp(2024, game_ids = schedule$game_id[123])

## ── Test with first 5 games ──
test5 <- build_season_pbp(2024, game_ids = schedule$game_id[1:5])

## ── Full season (once tests pass) ──
## pbp_2024 <- build_season_pbp(2024)

## ── Multiple seasons ──
## pbp_2023 <- build_season_pbp(2023)
## pbp_2022 <- build_season_pbp(2022)

## ── To clear test caches and re-run ──
## unlink("cache/*test*")



tracking_20241022 %>% group_by(PLAYER_ID, period, game_date) %>% 
  summarise(fg2a = sum(fg2a),
            fg2m = sum(fg2m),
            fg3a = sum(fg3a),
            fg3m = sum(fg3m),
            fga = sum(fga),
            fgm = sum(fgm)) %>% 
  filter(PLAYER_ID == "1628369")

tracking_20241022 %>% group_by(PLAYER_ID, shot_clock_range, game_date) %>% 
  summarise(fg2a = sum(fg2a),
            fg2m = sum(fg2m),
            fg3a = sum(fg3a),
            fg3m = sum(fg3m),
            fga = sum(fga),
            fgm = sum(fgm)) %>% 
  filter(PLAYER_ID == "1628369")

tracking_20241022 %>% group_by(PLAYER_ID, dribble_range, game_date) %>% 
  summarise(fg2a = sum(fg2a),
            fg2m = sum(fg2m),
            fg3a = sum(fg3a),
            fg3m = sum(fg3m),
            fga = sum(fga),
            fgm = sum(fgm)) %>% 
  filter(PLAYER_ID == "1628369")

tracking_20241022 %>% group_by(PLAYER_ID, def_dist_range, game_date) %>% 
  summarise(fg2a = sum(fg2a),
            fg2m = sum(fg2m),
            fg3a = sum(fg3a),
            fg3m = sum(fg3m),
            fga = sum(fga),
            fgm = sum(fgm)) %>% 
  filter(PLAYER_ID == "1628369")

tracking_20241022 %>% group_by(PLAYER_ID, period, dribble_range, def_dist_range, game_date) %>% 
  summarise(fg2a = sum(fg2a),
            fg2m = sum(fg2m),
            fg3a = sum(fg3a),
            fg3m = sum(fg3m),
            fga = sum(fga),
            fgm = sum(fgm)) %>% 
  filter(PLAYER_ID == "1628369")


