## ═════════════════════════════════════════════════════════════════════════════
## 01_data_pipeline.R
## NBA xFG v2 — Full data pipeline (test-friendly)
##
## Entry points:
##   build_season_pbp(2024)                          → full season
##   build_season_pbp(2024, game_ids = "0022400061") → single game test
##   build_season_pbp(2024, game_ids = schedule$game_id[1:5]) → first 5 games
##
## All intermediate results are cached to cache/ so re-runs skip API calls.
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


## ─── Helpers ────────────────────────────────────────────────────────────────

safe_call <- function(fn, ..., delay = 0.6) {
  Sys.sleep(delay)
  tryCatch(fn(...), error = function(e) {
    message("  API error: ", e$message, " — retrying in 3s…")
    Sys.sleep(3)
    tryCatch(fn(...), error = function(e2) {
      message("  Retry failed: ", e2$message)
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
           season, season_type_description)
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
          if (new_poss) {
            sc_state <- if (!is.na(gc) && gc < 24) NA_real_ else 24
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
          if (bump14_if_under) {
            if (!is.na(gc) && gc < 14) {
              sc_post <- NA_real_
            } else {
              if (is.na(sc_post)) sc_post <- 14
              else sc_post <- if (sc_post < 14) 14 else sc_post
            }
          }
          
          # game clock vs shot clock: if shot clock > game clock, turn off
          if (!is.na(gc)) {
            #if (!is.na(sc_pre)  && sc_pre  > gc) sc_pre  <- NA_real_
            if (!is.na(sc_pre)  && sc_pre  > gc) sc_pre  <- gc
            #if (!is.na(sc_post) && sc_post > gc) sc_post <- NA_real_
            if (!is.na(sc_post) && sc_post > gc) sc_post <- gc
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
## 07 — TRACKING AGGREGATES ===================================================
##
## Key change: the API already returns FG2A and FG3A per player in each call.
## Instead of collapsing to a single player-level average, we preserve the
## full conditional distribution:
##
##   P(def_dist_bin | shot_type, player)
##   P(catch_and_shoot | shot_type, player)
##
## This lets us assign DIFFERENT defender distances and C&S probabilities
## to rim shots vs threes by the same player with no additional API calls.
##
## The old code computed one expected_def_dist per player (e.g. Tatum = 4.16ft
## for ALL shots). The new code samples per shot from:
##   - Tatum rim attempt → mostly 0-2ft and 2-4ft bins → ~1.5-3ft
##   - Tatum three → mostly 4-6ft and 6+ft bins → ~4.5-7ft
## ════════════════════════════════════════════════════════════════════════════

get_tracking_aggregates <- function(season_str) {
  
  dist_ranges <- c("0-2 Feet - Very Tight", "2-4 Feet - Tight",
                   "4-6 Feet - Open", "6+ Feet - Wide Open")
  
  # ── 4 calls: defender distance profiles (same as before, but keep FG2A/FG3A) ──
  def_list <- lapply(dist_ranges, function(dr) {
    res <- safe_call(nba_leaguedashplayerptshot,
                     season = season_str, season_type = "Regular Season",
                     close_def_dist_range = dr, per_mode = "Totals")
    if (is.null(res)) return(NULL)
    as.data.frame(res$LeagueDashPTShots) %>% mutate(def_dist_range = dr)
  })
  def_raw <- bind_rows(def_list)
  
  def_dist_profiles <- def_raw %>%
    transmute(PLAYER_ID = as.character(PLAYER_ID),
              def_dist_range,
              fg2a = as.numeric(FG2A),
              fg3a = as.numeric(FG3A))
  
  # ── 2 calls: catch-and-shoot / pullup profiles (same as before, but keep FG2A/FG3A) ──
  cs_list <- lapply(c("Catch and Shoot", "Pullups"), function(gr) {
    res <- safe_call(nba_leaguedashplayerptshot,
                     season = season_str, season_type = "Regular Season",
                     general_range = gr, per_mode = "Totals")
    if (is.null(res)) return(NULL)
    as.data.frame(res$LeagueDashPTShots) %>% mutate(general_range = gr)
  })
  cs_raw <- bind_rows(cs_list)
  
  cs_profiles <- cs_raw %>%
    transmute(PLAYER_ID = as.character(PLAYER_ID),
              type = case_when(general_range == "Catch and Shoot" ~ "cs",
                               general_range == "Pullups"         ~ "pu",
                               TRUE ~ NA_character_),
              fg2a = as.numeric(FG2A),
              fg3a = as.numeric(FG3A)) %>%
    filter(!is.na(type)) %>%
    pivot_wider(names_from = type,
                values_from = c(fg2a, fg3a),
                values_fill = 0)
  # → columns: PLAYER_ID, fg2a_cs, fg2a_pu, fg3a_cs, fg3a_pu
  
  list(def_dist_profiles = def_dist_profiles, cs_profiles = cs_profiles)
}


## ════════════════════════════════════════════════════════════════════════════
## impute_shot_context()
##
## Per-shot probabilistic imputation of:
##   1. is_catch_shoot  — Bernoulli draw from shot-type-specific C&S rate
##   2. p_catch_shoot   — the rate itself (for reference / diagnostics)
##   3. expected_def_dist — sampled from player's defender-distance distribution
##                          conditioned on shot type (2PA vs 3PA)
##
## Conditioning logic:
##   - Rim shots (shot_family == "rim"):
##       is_catch_shoot = 0 (always)
##       def_dist sampled from player's FG2A distribution (tighter)
##
##   - Jump shots (j2, j3):
##       is_catch_shoot ~ Bernoulli(p_cs for shot type)
##       def_dist sampled from player's FG2A or FG3A distribution
##
##   - Fastbreak modifier:
##       Shifts defender distance distribution toward more open bins.
##       Fastbreak shots are typically in transition with less defensive setup.
##
##
## Defender distance logic (three-tier, mirrors C&S approach):
##
##   Tier 1 — DETERMINISTIC: contact shots → def_dist ~ Uniform(0, 1)
##     Blocks, and-1s, and shooting fouls all involve physical contact
##     between defender and shooter. The defender was effectively at 0 ft.
##     These are tagged by is_contact_shot from link_shooting_fouls().
##
##   Tier 2 — ADJUSTED PROBABILISTIC: non-contact shots
##     The tracking API's "0-2 Feet - Very Tight" bin includes BOTH contact
##     and non-contact tight defense. To avoid double-counting:
##       - Subtract contact count from 0-2ft bin per player × shot type
##       - Re-normalise remaining counts for non-contact shots
##     This is analogous to the C&S descriptor adjustment: known events get
##     deterministic assignment, then the probabilistic distribution shifts
##     so per-player totals still match tracking data.
##
##     Example: Tatum has 100 2PA with tracking showing 40 in 0-2ft bin.
##       - 12 of those 2PA are blocks/fouls (known contact → Tier 1)
##       - Adjusted 0-2ft count for remaining 88 non-contact shots: 40-12=28
##       - Adjusted weight: 28/88 = 0.318 (was 40/100 = 0.400)
##       - Combined: 12 deterministic + 88 × 0.318 ≈ 40 total 0-2ft, matching
##
##   Tier 3 — LEAGUE FALLBACK: no tracking data for the player
##
##
## Catch-and-shoot logic (three-tier):
##
##   Tier 1 — DETERMINISTIC: rim shots → is_catch_shoot = 0 (always)
##
##   Tier 2 — DETERMINISTIC from PBP descriptor:
##     Shots whose descriptor contains "driving", "pullup", "fadeaway",
##     "step back", "turnaround", or "running" are DEFINITIVELY not C&S.
##     These are all self-created / off-the-dribble shot types.
##
##   Tier 3 — ADJUSTED PROBABILISTIC for ambiguous shots:
##     Shots with NA or other descriptors ("cutting", "bank", "floating",
##     "putback") are ambiguous. For these, we compute an adjusted p_cs:
##
##       adjusted_p_cs = (n_total × base_p_cs) / n_ambiguous
##
##     This keeps the player's overall C&S rate aligned with their season
##     tracking profile, but concentrates C&S probability on shots where
##     it is actually plausible (i.e., not known self-created shots).
##
##     Example: Tatum has 10 threes, base_p_cs = 0.50 from tracking.
##       - 3 are "step back" or "pullup" → definite non-C&S
##       - 7 are ambiguous
##       - Expected C&S = 10 × 0.50 = 5; must be among the 7 ambiguous
##       - adjusted_p_cs = 5/7 = 0.714
##       - ~5 of 7 ambiguous get tagged C&S → matches 50% season rate
##
##
## Fallback: league-average distributions for unknown players.
## ════════════════════════════════════════════════════════════════════════════

impute_shot_context <- function(pbp, tracking, seed = 42, verbose = TRUE) {
  
  set.seed(seed)
  
  # Bin definitions: lower bound, upper bound for sampling within each bin
  bin_defs <- list("0-2 Feet - Very Tight" = c(0.5, 2.0),
                   "2-4 Feet - Tight"      = c(2.0, 4.0),
                   "4-6 Feet - Open"       = c(4.0, 6.0),
                   "6+ Feet - Wide Open"   = c(6.0, 10.0))
  bin_names <- names(bin_defs)
  
  # ── Descriptor patterns that DEFINITIVELY indicate non-catch-and-shoot ──
  # These are all off-the-dribble or self-created shot types. A catch-and-shoot
  # shot by definition cannot be a pullup, driving, fadeaway, step back, etc.
  non_cs_pattern <- "driving|pullup|fadeaway|step.?back|turnaround|running"
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # BUILD DEFENDER DISTANCE LOOKUP
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # For each player: 4-bin probability vector for 2PA, 4-bin vector for 3PA.
  # If a player has no 3PA tracking data, fall back to their overall distribution.
  
  ddp <- tracking$def_dist_profiles
  
  # League-average distributions (computed from all players)
  league_2 <- ddp %>%
    group_by(def_dist_range) %>%
    summarise(fg2a = sum(fg2a, na.rm = TRUE), 
              .groups = "drop")
  league_2$w <- league_2$fg2a / sum(league_2$fg2a)
  league_2_w <- setNames(league_2$w, league_2$def_dist_range)[bin_names]
  
  league_3 <- ddp %>%
    group_by(def_dist_range) %>%
    summarise(fg3a = sum(fg3a, na.rm = TRUE), 
              .groups = "drop")
  league_3$w <- league_3$fg3a / sum(league_3$fg3a)
  league_3_w <- setNames(league_3$w, league_3$def_dist_range)[bin_names]
  
  # Per-player lookup
  players <- unique(ddp$PLAYER_ID)
  def_lookup <- list()
  
  for (pid in players) {
    pd <- ddp %>% filter(PLAYER_ID == pid)
    
    w2 <- setNames(rep(0, 4), bin_names)
    w3 <- setNames(rep(0, 4), bin_names)
    
    for (bn in bin_names) {
      row <- pd %>% filter(def_dist_range == bn)
      if (nrow(row) > 0) {
        w2[bn] <- row$fg2a[1]
        w3[bn] <- row$fg3a[1]
      }
    }
    
    # Normalise; fall back to league average if no data for a shot type
    s2 <- sum(w2)
    s3 <- sum(w3)
    w2 <- if (s2 > 0) w2 / s2 else league_2_w
    w3 <- if (s3 > 0) w3 / s3 else league_3_w
    
    def_lookup[[pid]] <- list(w2 = w2, w3 = w3)
  }
  
  def_fallback <- list(w2 = league_2_w, w3 = league_3_w)
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # BUILD CATCH-AND-SHOOT LOOKUP
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # For each player: p_catch_shoot conditioned on shot type (2PA vs 3PA).
  # This captures that players shoot more C&S threes than C&S twos.
  
  csp <- tracking$cs_profiles
  
  # League-average C&S rates by shot type
  tot_cs_2 <- sum(csp$fg2a_cs, na.rm = TRUE)
  tot_pu_2 <- sum(csp$fg2a_pu, na.rm = TRUE)
  tot_cs_3 <- sum(csp$fg3a_cs, na.rm = TRUE)
  tot_pu_3 <- sum(csp$fg3a_pu, na.rm = TRUE)
  
  league_p_cs_2 <- if ((tot_cs_2 + tot_pu_2) > 0) {
    tot_cs_2 / (tot_cs_2 + tot_pu_2)
  } else 0.30
  league_p_cs_3 <- if ((tot_cs_3 + tot_pu_3) > 0) {
    tot_cs_3 / (tot_cs_3 + tot_pu_3)
  } else 0.50
  
  cs_lookup <- list()
  
  for (i in seq_len(nrow(csp))) {
    pid   <- csp$PLAYER_ID[i]
    cs_2  <- csp$fg2a_cs[i]
    pu_2  <- csp$fg2a_pu[i]
    cs_3  <- csp$fg3a_cs[i]
    pu_3  <- csp$fg3a_pu[i]
    
    t2 <- cs_2 + pu_2
    t3 <- cs_3 + pu_3
    
    cs_lookup[[pid]] <- list(
      p_cs_2 = if (t2 > 0) cs_2 / t2 else league_p_cs_2,
      p_cs_3 = if (t3 > 0) cs_3 / t3 else league_p_cs_3
    )
  }
  
  cs_fallback <- list(p_cs_2 = league_p_cs_2, p_cs_3 = league_p_cs_3)
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # DESCRIPTOR-ADJUSTED C&S RATES
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Shots with descriptors like "pullup", "driving", "fadeaway", etc. are
  # DEFINITIVELY not catch-and-shoot. For ambiguous shots (NA, "cutting",
  # "bank", "floating", "putback"), we adjust p_cs upward to compensate:
  #
  #   adjusted_p_cs = (n_total * base_p_cs) / n_ambiguous
  #
  # This ensures the overall C&S count for the player still matches their
  # season tracking profile, but concentrates C&S probability on shots where
  # it is actually plausible. Capped at 1.0 for edge cases where more shots
  # have definite non-C&S descriptors than the tracking data would predict.
  #
  # Example: Tatum has 10 threes, base_p_cs = 0.50.
  #   - 3 are "step back" or "pullup" → definite non-C&S
  #   - 7 are ambiguous
  #   - Expected C&S = 10 * 0.50 = 5
  #   - adjusted_p_cs = 5 / 7 = 0.714
  #   - ~5 of 7 ambiguous shots get tagged C&S, matching the 50% season rate
  
  fga_idx <- which(pbp$fga == 1)
  if (length(fga_idx) == 0) {
    pbp$expected_def_dist <- NA_real_
    pbp$p_catch_shoot     <- NA_real_
    pbp$is_catch_shoot    <- NA_integer_
    return(pbp)
  }
  
  # Ensure descriptor column exists (some PBP sources may not have it)
  #if (!"descriptor" %in% names(pbp)) pbp$descriptor <- NA_character_
  
  # Tag definite non-C&S on jump shots across full PBP
  jump_shots <- pbp[fga_idx, ] %>%
    filter(shot_family %in% c("j2", "j3")) %>%
    mutate(pid        = as.character(player1_id),
           is_3_flag  = as.integer(is_three == 1),
           definite_non_cs = grepl(non_cs_pattern, descriptor, 
                                   ignore.case = TRUE) &
             !is.na(descriptor))
  
  # Per player × shot type: count definite non-C&S and total
  cs_adj_counts <- jump_shots %>%
    group_by(pid, is_3_flag) %>%
    summarise(n_total           = n(),
              n_definite_non_cs = sum(definite_non_cs),
              n_ambiguous       = n_total - n_definite_non_cs,
              .groups = "drop")
  
  # Build adjusted p_cs lookup: key = "pid|is_3" → adjusted_p_cs
  adj_cs_lookup <- list()
  
  for (r in seq_len(nrow(cs_adj_counts))) {
    row <- cs_adj_counts[r, ]
    pid  <- row$pid
    is_3 <- row$is_3_flag
    key  <- paste0(pid, "|", is_3)
    
    # Get base rate from tracking
    cs <- cs_lookup[[pid]]
    if (!is.null(cs)) {
      base_p_cs <- if (is_3 == 1) cs$p_cs_3 else cs$p_cs_2
    } else {
      base_p_cs <- if (is_3 == 1) league_p_cs_3 else league_p_cs_2
    }
    
    if (row$n_ambiguous > 0) {
      # Expected C&S shots = total * base_rate; all must be among ambiguous shots
      expected_cs <- row$n_total * base_p_cs
      adj_p <- min(expected_cs / row$n_ambiguous, 1.0)
    } else {
      # Every shot has a definite non-C&S descriptor → no C&S possible
      adj_p <- 0.0
    }
    
    adj_cs_lookup[[key]] <- adj_p
  }
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # PER-SHOT IMPUTATION
  # ═══════════════════════════════════════════════════════════════════════════
  
  pbp$expected_def_dist <- NA_real_
  pbp$p_catch_shoot     <- NA_real_
  pbp$is_catch_shoot    <- NA_integer_
  
  fga_idx <- which(pbp$fga == 1)
  if (length(fga_idx) == 0) return(pbp)
  
  
  # ═══════════════════════════════════════════════════════════════════════════
  # CONTACT-ADJUSTED DEFENDER DISTANCE DISTRIBUTIONS
  # ═══════════════════════════════════════════════════════════════════════════
  #
  # Contact shots (blocks, and-1s, shooting fouls on misses) get deterministic
  # def_dist ~ Uniform(0, 1). The tracking API's 0-2ft bin includes both
  # contact and non-contact tight defense. We subtract contact counts from
  # the 0-2ft bin per player × shot type, then re-normalise for non-contact
  # shots. This mirrors the descriptor-adjusted C&S approach.
  
  # Count contact shots per player × shot type from PBP
  contact_counts <- pbp[fga_idx, ] %>%
    filter(is_contact_shot == 1) %>%
    mutate(pid       = as.character(player1_id),
           is_3_flag = as.integer(is_three == 1)) %>%
    group_by(pid, is_3_flag) %>%
    summarise(n_contact = n(), .groups = "drop")
  
  # Total FGA per player × shot type (for converting proportions → pseudo-counts)
  fga_totals <- pbp[fga_idx, ] %>%
    mutate(pid       = as.character(player1_id),
           is_3_flag = as.integer(is_three == 1)) %>%
    group_by(pid, is_3_flag) %>%
    summarise(n_total = n(), .groups = "drop")
  
  # Build adjusted distributions: subtract contacts from 0-2ft bin, re-normalise
  adj_def_lookup <- list()
  
  for (r in seq_len(nrow(contact_counts))) {
    pid  <- contact_counts$pid[r]
    is_3 <- contact_counts$is_3_flag[r]
    n_c  <- contact_counts$n_contact[r]
    key  <- paste0(pid, "|", is_3)
    
    dl <- def_lookup[[pid]]
    if (is.null(dl)) dl <- def_fallback
    weights <- if (is_3 == 1) dl$w3 else dl$w2
    
    # Get PBP total for this player × shot type
    total_row <- fga_totals %>% filter(pid == !!pid, is_3_flag == !!is_3)
    n_tot <- if (nrow(total_row) > 0) total_row$n_total[1] else 1L
    
    # Convert proportions to pseudo-counts, subtract contacts from 0-2ft bin
    counts <- weights * n_tot
    counts[1] <- max(counts[1] - n_c, 0)
    
    # Re-normalise for the non-contact shots
    n_remaining <- n_tot - n_c
    if (n_remaining > 0 && sum(counts) > 0) {
      adj_def_lookup[[key]] <- counts / sum(counts)
    }
    # else: all shots are contact (edge case) → no adjusted dist needed
  }
  
  if (verbose) {
    n_contact_total <- sum(contact_counts$n_contact)
    n_players_adj   <- nrow(contact_counts)
    message("    Contact-shot adjustment: ", n_contact_total,
            " contact FGA across ", n_players_adj, " player x type combos")
  }
  
  # Counters
  n_cs_deterministic <- 0L  # definitely not C&S (from descriptor)
  n_cs_adjusted <- 0L       # ambiguous, used adjusted p_cs
  n_cs_fallback <- 0L       # no tracking data for player
  n_def_type <- 0L
  n_def_fallback <- 0L
  n_fb_boost <- 0L
  n_contact_det <- 0L       # contact shots → deterministic tight def_dist
  
  for (i in fga_idx) {
    pid    <- as.character(pbp$player1_id[i])
    is_3   <- isTRUE(pbp$is_three[i] == 1)
    is_rim <- isTRUE(pbp$shot_family[i] == "rim")
    is_fb  <- isTRUE(pbp$is_fastbreak[i] == 1)
    descrip   <- pbp$descriptor[i]
    
    # ── 1. Catch-and-shoot ──
    if (is_rim) {
      # Rim shots are never catch-and-shoot
      p_cs <- 0
      pbp$is_catch_shoot[i] <- 0L
      
    } else if (!is.na(descrip) && grepl(non_cs_pattern, descrip, ignore.case = TRUE)) {
      # Descriptor definitively indicates NOT catch-and-shoot
      p_cs <- 0
      pbp$is_catch_shoot[i] <- 0L
      n_cs_deterministic <- n_cs_deterministic + 1L
      
    } else {
      # Ambiguous descriptor → use adjusted p_cs
      is_3_int <- as.integer(is_3)
      key <- paste0(pid, "|", is_3_int)
      
      adj_p <- adj_cs_lookup[[key]]
      if (!is.null(adj_p)) {
        p_cs <- adj_p
        n_cs_adjusted <- n_cs_adjusted + 1L
      } else {
        # Player not in our PBP jump shots (shouldn't happen, but guard)
        cs <- cs_lookup[[pid]]
        if (!is.null(cs)) {
          p_cs <- if (is_3) cs$p_cs_3 else cs$p_cs_2
        } else {
          p_cs <- if (is_3) cs_fallback$p_cs_3 else cs_fallback$p_cs_2
        }
        n_cs_fallback <- n_cs_fallback + 1L
      }
      pbp$is_catch_shoot[i] <- as.integer(runif(1) < p_cs)
    }
    pbp$p_catch_shoot[i] <- p_cs
    
    # ── 2. Defender distance ──
    is_contact <- isTRUE(pbp$is_contact_shot[i] == 1)
    
    if (is_contact) {
      # Contact shot (block, and-1, shooting foul): defender was physically
      # touching the shooter. Deterministic very-tight distance.
      pbp$expected_def_dist[i] <- runif(1, 0, 1.0)
      n_contact_det <- n_contact_det + 1L
      
    } else {
      # Non-contact shot: use adjusted distribution if available
      # (0-2ft bin reduced by this player's contact shot count)
      adj_key <- paste0(pid, "|", as.integer(is_3))
      adj_w   <- adj_def_lookup[[adj_key]]
      
      if (!is.null(adj_w)) {
        weights <- adj_w
        n_def_type <- n_def_type + 1L
      } else {
        # No contact adjustment needed → use original distribution
        dl <- def_lookup[[pid]]
        if (!is.null(dl)) {
          weights <- if (is_3) dl$w3 else dl$w2
          n_def_type <- n_def_type + 1L
        } else {
          weights <- if (is_3) def_fallback$w3 else def_fallback$w2
          n_def_fallback <- n_def_fallback + 1L
        }
      }
      
      # Fastbreak modifier: shift distribution toward open
      if (is_fb) {
        weights[1] <- weights[1] * 0.3
        weights[2] <- weights[2] * 0.5
        weights[3] <- weights[3] * 1.2
        weights[4] <- weights[4] * 2.0
        weights <- weights / sum(weights)
        n_fb_boost <- n_fb_boost + 1L
      }
      
      # Sample bin then sample continuously within the bin
      u <- runif(1)
      cum <- cumsum(weights)
      bin_idx <- which(u <= cum)[1]
      if (is.na(bin_idx)) bin_idx <- 4L
      bounds <- bin_defs[[bin_idx]]
      pbp$expected_def_dist[i] <- runif(1, bounds[1], bounds[2])
    }
  }
  
  if (verbose) {
    message("    Shot context imputation:")
    message("      C&S — descriptor-ruled-out=", n_cs_deterministic,
            "  adjusted-sampling=", n_cs_adjusted,
            "  fallback=", n_cs_fallback)
    message("      Def dist — contact-deterministic=", n_contact_det,
            "  type-specific=", n_def_type,
            "  league-fallback=", n_def_fallback,
            "  fastbreak-boosted=", n_fb_boost)
    
    # Quick sanity summary
    fga <- pbp[fga_idx, ]
    for (fam in c("rim", "j2", "j3")) {
      sub <- fga %>% filter(shot_family == fam)
      if (nrow(sub) > 0) {
        n_contact <- sum(sub$is_contact_shot == 1, na.rm = TRUE)
        message(
          "      ",
          sprintf("%-3s: n=%-4d  mean_def_dist=%4.2f  contact=%d  mean_p_cs=%4.3f  cs_rate=%4.3f",
                  fam, nrow(sub),
                  mean(sub$expected_def_dist, na.rm = TRUE),
                  n_contact,
                  mean(sub$p_catch_shoot, na.rm = TRUE),
                  mean(sub$is_catch_shoot, na.rm = TRUE)))
      }
    }
    # Descriptor breakdown
    n_jump <- nrow(fga %>% filter(shot_family %in% c("j2", "j3")))
    pct_ruled <- round(100 * n_cs_deterministic / max(n_jump, 1), 1)
    message("      Descriptor-based non-C&S: ", n_cs_deterministic, "/",
            n_jump, " jump shots (", pct_ruled, "%)")
  }
  
  pbp
}





## ════════════════════════════════════════════════════════════════════════════
## 08 — ASSEMBLY + SCALING   ==================================================
## ════════════════════════════════════════════════════════════════════════════

assemble_and_scale <- function(pbp, schedule, bio, tracking, seed = 42) {
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


  # Tracking aggregates — per-shot context imputation
  # (replaces old player-level averages with shot-type-specific sampling)
  pbp <- impute_shot_context(pbp, tracking, seed = seed)

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
  # if (file.exists(cp("pbp_raw"))) {
  #   message("  Loading cached raw PBP…")
  #   pbp_raw <- readRDS(cp("pbp_raw"))
  # } else {
  #   message("  Scraping PBP for ", nrow(schedule_subset), " game(s)…")
  #   pbp_raw <- map(schedule_subset$game_id, function(gid) {
  #     tryCatch(get_game_with_lineups(gid), error = function(e) {
  #       message("    Error on ", gid, ": ", e$message); NULL
  #     })
  #   }, .progress = !is_test) %>% bind_rows()
  #   saveRDS(pbp_raw, cp("pbp_raw"))
  # }
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
  # if (file.exists(bio_cache)) {
  #   message("  Loading cached biometrics…")
  #   bio <- readRDS(bio_cache)
  # } else {
  #   bio <- get_biometrics(season_str, season_year)
  #   saveRDS(bio, bio_cache)
  # }
  bio <- get_biometrics(season_str, season_year)
  saveRDS(bio, bio_cache)
  message("  Players with height: ", nrow(bio))


  ## ── 8. Tracking aggregates ─────────────────────────────────────────────
  # Also season-wide
  message("─── Step 8: Tracking aggregates ───")
  trk_cache <- file.path(cache_dir, paste0("tracking_", season_year, ".rds"))
  # if (file.exists(trk_cache)) {
  #   message("  Loading cached tracking data…")
  #   tracking <- readRDS(trk_cache)
  # } else {
  #   tracking <- get_tracking_aggregates(season_str)
  #   saveRDS(tracking, trk_cache)
  # }
  tracking <- get_tracking_aggregates(season_str)
  saveRDS(tracking, trk_cache)
  message("  Def-dist profiles: ",
          n_distinct(tracking$def_dist_profiles$PLAYER_ID), " players",
          "  |  C&S profiles: ", nrow(tracking$cs_profiles), " players")

  ## ── 9. Assembly + scaling ──────────────────────────────────────────────
  message("─── Step 9: Assembly & scaling ───")
  pbp <- assemble_and_scale(pbp, schedule_subset, bio, tracking)

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
## test1 <- build_season_pbp(2024, game_ids = schedule$game_id[1])

## ── Test with first 5 games ──
test5 <- build_season_pbp(2024, game_ids = schedule$game_id[1:5])

## ── Full season (once tests pass) ──
## pbp_2024 <- build_season_pbp(2024)

## ── Multiple seasons ──
## pbp_2023 <- build_season_pbp(2023)
## pbp_2022 <- build_season_pbp(2022)

## ── To clear test caches and re-run ──
## unlink("cache/*test*")
