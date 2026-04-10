
## 0 - Configure ================================================================

# Load libraries
library(hoopR)
library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(ggplot2)
library(cmdstanr)
library(posterior)
library(loo)
library(readr)
library(scales)
library(stringr)
library(ggrepel)
library(patchwork)
library(janitor)
library(httr)
library(jsonlite)


# Stan options
set_cmdstan_path(path = cmdstanr::cmdstan_path())
options(mc.cores = parallel::detectCores())





## 1 - Scraping Shot Attempts ===================================================

# Scrape all shot attempts
pull_shots_league <- function(season, season_type, measure = "FGA") {
  res <- nba_shotchartdetail(player_id = 0,   # 0 means "all players" (league)
                             team_id   = 0,   # 0 means "all teams" (league)
                             season    = season,
                             season_type = season_type,
                             context_measure = measure)
  
  as.data.frame(res$Shot_Chart_Detail)
}



# Data cleaning
shots <- pull_shots_league(year_to_season(2024), "Regular Season") %>%
#shots <- read.csv("shots.csv") %>%
  mutate(PLAYER_ID = as.character(PLAYER_ID),
         TEAM_ID   = as.character(TEAM_ID),
         SHOT_ATTEMPTED_FLAG = as.integer(SHOT_ATTEMPTED_FLAG),
         SHOT_MADE_FLAG = as.integer(SHOT_MADE_FLAG),
         loc_x = as.numeric(LOC_X),
         loc_y = as.numeric(LOC_Y),
         shot_dist = as.numeric(SHOT_DISTANCE),
         period = as.integer(PERIOD),
         min_rem = as.integer(MINUTES_REMAINING),
         sec_rem = as.integer(SECONDS_REMAINING)) %>%
  filter(!is.na(SHOT_MADE_FLAG), !is.na(loc_x), 
         !is.na(loc_y), !is.na(shot_dist)) %>% 
  filter(SHOT_ZONE_BASIC != c("Backcourt")) %>%
  select(-LOC_X, -LOC_Y, -SHOT_DISTANCE, -PERIOD, 
         -MINUTES_REMAINING, -SECONDS_REMAINING) %>% 
  distinct(GAME_ID, GAME_EVENT_ID, PLAYER_ID, .keep_all = TRUE) %>%
  
  # Feature engineering
  mutate(angle = atan2(abs(loc_x), pmax(loc_y, 1e-6)),
         angle2 = abs(loc_x) / pmax(sqrt(loc_x^2 + loc_y^2), 1e-6),
         # radians, 0=straight, larger=more baseline/corner-ish
         
         is_three = as.integer(str_detect(SHOT_TYPE, "3PT")),
         is_corner3 = as.integer(str_detect(SHOT_ZONE_BASIC, "Corner 3")),
         is_dunk = as.integer(str_detect(ACTION_TYPE, "Dunk")),
         is_rim = as.integer(SHOT_ZONE_BASIC == "Restricted Area" |
                               shot_dist <= 4),
         
         # crude jumper flag
         is_jump = as.integer(!is_rim),

         # game clock within period 
         sec_in_period = (min_rem * 60 + sec_rem),
         T = as.numeric(scale(sec_in_period)),
         
         # Indexing for hierarchical components
         player_idx = as.integer(factor(PLAYER_ID)),
         zone_idx   = as.integer(factor(SHOT_ZONE_BASIC)),
         
         # Shot types
         shot_family = case_when(is_jump == 0 ~ "rim", 
                                 is_jump == 1 & is_three == 0 ~ "j2", 
                                 TRUE ~ "j3"),
         shot_type = case_when(shot_family == "rim" ~ 1L,
                               shot_family == "j2"  ~ 2L,
                               shot_family == "j3"  ~ 3L)) %>%
  group_by(shot_family) %>%
  mutate(D = as.numeric(scale(shot_dist)),  # center/scale within family
         A = as.numeric(scale(angle))) %>%
  ungroup() %>% 
  as.data.frame()


glimpse(shots)



shots %>% 
  group_by(SHOT_ZONE_BASIC, SHOT_ZONE_RANGE) %>% 
  summarise(n = n(), 
            min_dist = min(shot_dist), 
            mean_dist = round(mean(shot_dist), 1), 
            max_dist = max(shot_dist)) %>% 
  arrange(SHOT_ZONE_BASIC) %>% 
  as.data.frame()


nba_boxscorematchupsv3(game_id = "0022400001")$home_team_player_matchups %>% 
  as.data.frame() %>% 
  filter(person_id == "1627759")

nba_boxscorematchupsv3(game_id = "0022400001")$away_team_player_matchups %>% 
  as.data.frame() %>% 
  filter(matchups_person_id == "1627759")

shots %>% 
  filter(GAME_ID == "0022400001", PLAYER_ID == "1627759") %>% 
  group_by(GAME_ID, PLAYER_ID, PLAYER_NAME) %>% 
  summarise(fga = sum(SHOT_ATTEMPTED_FLAG),
            fgm = sum(SHOT_MADE_FLAG),
            `2pa` = sum(SHOT_ATTEMPTED_FLAG[is_three == 0]),
            `2pm` = sum(SHOT_MADE_FLAG[is_three == 0]),
            `3pa` = sum(SHOT_ATTEMPTED_FLAG[is_three == 1]),
            `3pm` = sum(SHOT_MADE_FLAG[is_three == 1]),
            points = sum((SHOT_MADE_FLAG * (2 + is_three))),
            .groups = "drop_last") %>% 
            #points = (SHOT_MADE_FLAG * (2 + is_three))) %>% 
  as.data.frame()




# Function to convert clock string to seconds elapsed in period
# Vectorized clock conversion
clock_to_seconds <- function(clock_str) {
  # Format: "PT11M43.00S" means 11:43 remaining
  # We want seconds ELAPSED, so 12:00 - 11:43 = 0:17 = 17 seconds
  
  sapply(clock_str, function(cs) {
    if (is.na(cs)) return(NA)
    
    minutes <- as.numeric(gsub("PT(\\d+)M.*", "\\1", cs))
    seconds <- as.numeric(gsub(".*M([0-9.]+)S", "\\1", cs))
    
    remaining <- minutes * 60 + seconds
    elapsed <- 720 - remaining  # 12 minutes = 720 seconds per period
    return(elapsed)
  }, USE.NAMES = FALSE)
}

# Function to get lineups for one game with substitution tracking
get_game_with_lineups <- function(game_id) {
  message("Processing game ", game_id)
  
  # Get play-by-play
  pbp <- nba_live_pbp(game_id)
  if (is.null(pbp) || nrow(pbp) == 0) return(NULL)
  
  # Get rotation data for initial lineups
  rot <- nba_gamerotation(game_id = game_id)
  
  # Get home/away team IDs
  home_team_id <- unique(rot$HomeTeam$TEAM_ID)[1]
  away_team_id <- unique(rot$AwayTeam$TEAM_ID)[1]
  
  # Initialize lineups with starters (players who checked in at time 0)
  home_starters <- rot$HomeTeam %>%
    filter(IN_TIME_REAL == "0") %>%
    pull(PERSON_ID)
  
  away_starters <- rot$AwayTeam %>%
    filter(IN_TIME_REAL == "0") %>%
    pull(PERSON_ID)
  
  # Initialize lineup vectors (pad to 5 if needed)
  current_home_lineup <- c(home_starters, rep(NA_character_, 5 - length(home_starters)))[1:5]
  current_away_lineup <- c(away_starters, rep(NA_character_, 5 - length(away_starters)))[1:5]
  
  #message("Home starters: ", paste(current_home_lineup, collapse = ", "))
  #message("Away starters: ", paste(current_away_lineup, collapse = ", "))
  
  # Create storage for lineups at each play
  home_lineups <- list()
  away_lineups <- list()
  
  # Track number of substitutions processed
  sub_count <- 0
  
  # Iterate through plays and track substitutions
  for (i in 1:nrow(pbp)) {
    play <- pbp[i, ]
    
    # Check if this is a substitution - check both action_type values
    if (!is.na(play$action_type) && 
        (play$action_type == "substitution" || play$action_type == "Substitution")) {
      
      sub_count <- sub_count + 1
      player_id <- as.character(play$player1_id)
      team_id <- as.character(play$team_id)
      sub_type <- tolower(as.character(play$sub_type))
      
      # Determine which team's lineup to modify
      if (team_id == home_team_id) {
        # Home team substitution
        if (grepl("out", sub_type)) {
          # Remove player from lineup
          idx <- which(current_home_lineup == player_id)
          if (length(idx) > 0) {
            current_home_lineup[idx[1]] <- NA_character_
          }
        } else if (grepl("in", sub_type)) {
          # Add player to first available NA slot
          na_idx <- which(is.na(current_home_lineup))
          if (length(na_idx) > 0) {
            current_home_lineup[na_idx[1]] <- player_id
          }
        }
      } else if (team_id == away_team_id) {
        # Away team substitution
        if (grepl("out", sub_type)) {
          # Remove player from lineup
          idx <- which(current_away_lineup == player_id)
          if (length(idx) > 0) {
            current_away_lineup[idx[1]] <- NA_character_
          }
        } else if (grepl("in", sub_type)) {
          # Add player to first available NA slot
          na_idx <- which(is.na(current_away_lineup))
          if (length(na_idx) > 0) {
            current_away_lineup[na_idx[1]] <- player_id
          }
        }
      }
    }
    
    # Store current lineups for this play
    home_lineups[[i]] <- current_home_lineup
    away_lineups[[i]] <- current_away_lineup
  }
  
  #message("Total substitutions processed: ", sub_count)
  
  # Add lineup columns to pbp
  pbp_with_lineups <- pbp %>%
    mutate(
      game_id = game_id,
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
      # Convert list columns to character strings
      qualifiers = map_chr(qualifiers, ~ if (length(.x) > 0) {
        paste(.x, collapse = "_")
        } else NA_character_),
      person_ids_filter = map_chr(person_ids_filter, ~ if (length(.x) > 0) {
        paste(.x, collapse = "_")
        } else NA_character_)
      
    )
  
  return(pbp_with_lineups)
}

# Test it
test_with_lineups <- get_game_with_lineups(game_ids[1])


write.csv(test_with_lineups, "test_lineup.csv")


nba_schedule(season = year_to_season(2024)) %>% as.data.frame()




## 2 - Stan Model ===============================================================

# Stan data
dat9 <- list(N = nrow(shots), 
             y = shots$y,
             D = shots$D,
             A = shots$A,
             is_dunk = shots$is_dunk,
             J_player = n_distinct(shots$player_idx),
             player = shots$player_idx,
             shot_type = shots$shot_type,
             grainsize = 200)          


# Compile
m9 <- cmdstan_model("stan/nba_xfg9.stan", 
                    cpp_options = list(stan_threads = TRUE))

# Sample
fit9 <- m9$sample(data = dat9,
                  chains = 4,
                  parallel_chains = 4,   # uses 4 cores
                  threads_per_chain = 4, # uses 4 cores per chain -> 16 total
                  iter_warmup = 600,
                  iter_sampling = 600,
                  refresh = 200)

# Diagnostic check
fit9$cmdstan_diagnose()

# Parameter Summaries
fit9$summary(variables = c('alpha_rim', 'alpha_j2', 'alpha_j3', 
                           'bD_rim', 'bA_rim', 'bDunk_rim',
                           'bD_j2', 'bA_j2', 'bD_j3', 'bA_j3'))
fit9$summary(variables = c('z_player', 'sigma_player_type'))

# Draws Matrix
draws_mat <- fit9$draws(variables = c("alpha_rim", "alpha_j2", "alpha_j3", 
                                      "bD_rim", "bA_rim", "bDunk_rim", 
                                      "bD_j2", "bA_j2", "bD_j3", "bA_j3", 
                                      "sigma_player_type", "z_player")) %>% 
  posterior::as_draws_matrix()


inv_logit <- function(x) 1 / (1 + exp(-x))


# Manually calculate xFG%
get_xfg <- function(fit, shots_df, S = 200, chunk = 50000, seed = 1,
                    draws_mat = get0("draws_mat", 
                                     envir = parent.frame(), 
                                     inherits = TRUE)) {
  
  if (is.null(draws_mat)) stop("`draws_mat` not found.")
  
  fam  <- as.character(shots_df$shot_family)
  dunk <- shots_df$is_dunk; dunk[is.na(dunk)] <- 0L
  
  set.seed(seed)
  dmS <- draws_mat[sample.int(nrow(draws_mat), min(S, nrow(draws_mat))), , drop = FALSE]
  
  B <- list(rim = dmS[, c("alpha_rim","bD_rim","bA_rim","bDunk_rim"), drop = FALSE],
            j2  = dmS[, c("alpha_j2","bD_j2","bA_j2"), drop = FALSE],
            j3  = dmS[, c("alpha_j3","bD_j3","bA_j3"), drop = FALSE])
  
  N <- nrow(shots_df); out <- numeric(N)
  
  for (s in seq.int(1, N, by = chunk)) {
    idx <- s:min(N, s + chunk - 1L)
    sp  <- split(idx, fam[idx])
    
    if (length(i <- sp[["rim"]])) 
      out[i] <- colMeans(plogis(B$rim %*% rbind(1, shots_df$D[i], shots_df$A[i], dunk[i])))
    if (length(i <- sp[["j2"]]))  
      out[i] <- colMeans(plogis(B$j2  %*% rbind(1, shots_df$D[i], shots_df$A[i])))
    if (length(i <- sp[["j3"]]))  
      out[i] <- colMeans(plogis(B$j3  %*% rbind(1, shots_df$D[i], shots_df$A[i])))
  }
  
  out
}


# Posterior mean xFG per shot (average-player context-only)
shots_out <- shots %>%
  mutate(xfg = get_xfg(fit9, shots, S = 200, chunk = 50000, draws_mat = draws_mat),
         xpoints = (xfg * (2 + is_three)),
         points = (SHOT_MADE_FLAG * (2 + is_three)))

# Player-level summary:
# - "Shot Difficulty" ~ average xFG (lower = harder shots)
# - "FG% over expected" ~ actual FG% - expected FG%
player_tbl <- shots_out %>%
  group_by(PLAYER_ID, PLAYER_NAME) %>%
  summarise(fga = n(),
            fg_pct = mean(SHOT_MADE_FLAG),
            xfg_pct = mean(xfg),
            fg_over_exp = fg_pct - xfg_pct,
            xpoints_ps = mean(xpoints),
            points_ps = mean(points),
            pps_over_exp = points_ps - xpoints_ps,
            xpoints = sum(xpoints),
            points = sum(points),
            points_over_exp = points - xpoints,
            exp_makes = sum(xfg),
            makes = sum(SHOT_MADE_FLAG),
            makes_over_exp = makes - exp_makes,
            .groups = "drop") %>%
  arrange(desc(fg_over_exp)) %>% 
  select(-PLAYER_ID)

player_tbl %>% 
  arrange((desc(points_over_exp))) %>% 
  filter(fga>500)





## 3 - Basketball Court Plots ===================================================

# Court geometry helpers (NBA shotchart coords)
circle_df <- function(center = c(0, 0), r = 1, start = 0, end = 2*pi, n = 200) {
  t <- seq(start, end, length.out = n)
  tibble(x = center[1] + r * cos(t),
         y = center[2] + r * sin(t))
}

nba_halfcourt_layers <- function(line_size = 0.4) {
  # Standard shotchart coords
  x_min <- -250
  x_max <- 250
  y_min <- -47.5
  y_max <- 422.5
  
  # Key measurements (same convention used in most public shotchart court drawings)
  hoop_center <- c(0, 0)
  rim_r <- 7.5
  backboard_y <- -7.5
  
  lane_x <- 80             # paint half-width
  lane_y_top <- 143.5      # free-throw line y
  
  ft_center <- c(0, lane_y_top)
  ft_r <- 60               # FT circle radius
  
  ra_r <- 40               # restricted area radius
  
  corner_x <- 220
  corner_y <- 92.5         # where 3pt arc meets corner line
  three_r <- 237.5
  theta0 <- acos(corner_x / three_r)   # angle where arc meets corner
  
  # Build arc paths
  ra_arc   <- circle_df(center = hoop_center, r = ra_r, 
                        start = 0, end = pi, n = 200)
  rim_circ <- circle_df(center = hoop_center, r = rim_r, 
                        start = 0, end = 2*pi, n = 200)
  
  # 3pt arc: from left corner meeting point to right corner meeting point
  # Use angles from (pi - theta0) down to theta0 for the "upper" arc
  three_arc <- circle_df(center = hoop_center, r = three_r,
                         start = theta0, end = pi - theta0, n = 400)
  
  # FT circle (top half solid, bottom half dashed is common;)
  ft_circ <- circle_df(center = ft_center, r = ft_r, start = 0, end = 2*pi, n = 300)
  
  list(
    # Court boundary (halfcourt)
    geom_rect(aes(xmin = x_min, xmax = x_max, ymin = y_min, ymax = y_max),
              fill = NA, linewidth = line_size, inherit.aes = FALSE),
    
    # Halfcourt line
    geom_segment(aes(x = x_min, y = y_max, xend = x_max, yend = y_max),
                 linewidth = line_size, inherit.aes = FALSE),
    
    # Backboard
    geom_segment(aes(x = -30, y = backboard_y, xend = 30, yend = backboard_y),
                 linewidth = line_size, inherit.aes = FALSE),
    
    # Rim
    geom_path(data = rim_circ, aes(x, y), 
              linewidth = line_size, inherit.aes = FALSE),
    
    # Paint (lane)
    geom_rect(aes(xmin = -lane_x, xmax = lane_x, ymin = y_min, ymax = lane_y_top),
              fill = NA, linewidth = line_size, inherit.aes = FALSE),
    
    # Free throw line
    geom_segment(aes(x = -lane_x, y = lane_y_top, xend = lane_x, yend = lane_y_top),
                 linewidth = line_size, inherit.aes = FALSE),
    
    # Free throw circle
    geom_path(data = ft_circ, aes(x, y), linewidth = line_size, inherit.aes = FALSE),
    
    # Restricted area arc + vertical stanchions
    geom_path(data = ra_arc, aes(x, y), linewidth = line_size, inherit.aes = FALSE),
    geom_segment(aes(x = -ra_r, y = y_min, xend = -ra_r, yend = 0),
                 linewidth = line_size, inherit.aes = FALSE),
    geom_segment(aes(x =  ra_r, y = y_min, xend =  ra_r, yend = 0),
                 linewidth = line_size, inherit.aes = FALSE),
    
    # 3pt corner lines
    geom_segment(aes(x = -corner_x, y = y_min, xend = -corner_x, yend = corner_y),
                 linewidth = line_size, inherit.aes = FALSE),
    geom_segment(aes(x =  corner_x, y = y_min, xend =  corner_x, yend = corner_y),
                 linewidth = line_size, inherit.aes = FALSE),
    
    # 3pt arc
    geom_path(data = three_arc, aes(x, y), 
              linewidth = line_size, inherit.aes = FALSE)
  )
}



# Posterior mean parameters (plug-in surface)
par_hat <- draws_mat %>%
  colMeans()


# Build a halfcourt grid and compute features from (x,y)
# Court coords are in "tenths of feet" (shotchart convention)
x_seq <- seq(-250, 250, by = 2.5)      # 0.25 ft resolution
y_seq <- seq(-47.5, 422.5, by = 2.5)

grid <- tidyr::expand_grid(loc_x = x_seq, loc_y = y_seq) %>%
  mutate(shot_dist = sqrt(loc_x^2 + loc_y^2) / 10, # distance in feet
         # your original angle feature (keep consistent with training)
         angle = atan2(abs(loc_x), pmax(loc_y, 1e-6)),
        
         # classify 3PT by geometry of the NBA 3pt line
         # - corners: |x| >= 220 up to y=92.5
         # - arc: radius >= 237.5 for y>92.5
         is_three = as.integer(if_else(loc_y <= 92.5,
                                       abs(loc_x) >= 220,
                                       sqrt(loc_x^2 + loc_y^2) >= 237.5)),
        
         # rim vs jumper (match your model definition: rim if <=4 ft)
         is_jump = as.integer(shot_dist > 4),
         
         # IMPORTANT: dunk is not a pure function of location.
         # For a clean "average non-dunk attempt" surface, set to 0 everywhere.
         is_dunk = 0L,
         
         shot_family = case_when(is_jump == 0 ~ "rim", 
                                 is_jump == 1 & is_three == 0 ~ "j2", 
                                 TRUE ~ "j3")) %>%
  # Capture the exact within-family scaling used in training
  # (since D/A were scaled inside shot_family groups)
  left_join(shots %>%
              mutate(shot_family = case_when(is_jump == 0 ~ "rim", 
                                             is_jump == 1 & is_three == 0 ~ "j2",
                                             TRUE ~ "j3")) %>%
              group_by(shot_family) %>%
              summarise(mu_dist = mean(shot_dist),
                        sd_dist = sd(shot_dist),
                        mu_ang  = mean(angle),
                        sd_ang  = sd(angle),
                        .groups = "drop"), by = "shot_family") %>%
  mutate(D = (shot_dist - mu_dist) / sd_dist,
         A = (angle    - mu_ang)  / sd_ang,
         eta = case_when(shot_family == "rim" ~ par_hat["alpha_rim"] +
                           par_hat["bD_rim"] * D +
                           par_hat["bA_rim"] * A +
                           par_hat["bDunk_rim"] * is_dunk,
                         shot_family == "j2" ~ par_hat["alpha_j2"] +
                           par_hat["bD_j2"] * D +
                           par_hat["bA_j2"] * A,
                         TRUE ~ par_hat["alpha_j3"] +
                           par_hat["bD_j3"] * D +
                           par_hat["bA_j3"] * A),
         xfg = inv_logit(eta),
         xpoints = xfg * (2 + is_three))

# Model-implied xFG% surface
p_surface_xfg <- ggplot(grid, aes(x = loc_x, y = loc_y, fill = xfg)) +
  geom_raster(interpolate = TRUE) +
  nba_halfcourt_layers(line_size = 0.35) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(labels = percent_format(accuracy = 1), name = "xFG%") +
  labs(title = "Model-implied xFG% surface (posterior mean parameters)") +
  theme_void() +
  theme(legend.position = "right",
        plot.title = element_text(hjust = 0.5))

# Model-implied xPoints surface
p_surface_xpts <- ggplot(grid, aes(x = loc_x, y = loc_y, fill = xpoints)) +
  geom_raster(interpolate = TRUE) +
  nba_halfcourt_layers(line_size = 0.35) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5), clip = "off") +
  scale_fill_viridis_c(labels = number_format(accuracy = 0.01), name = "xPts") +
  labs(title = "Model-implied xPoints surface (posterior mean parameters)") +
  theme_void() +
  theme(legend.position = "right",
        plot.title = element_text(hjust = 0.5))

p_surface_xfg
p_surface_xpts










# Player map (for readability)
player_map <- shots %>%
  distinct(player_idx, PLAYER_ID, PLAYER_NAME) %>%
  arrange(player_idx)

J_players <- n_distinct(shots$player_idx)

alpha_names <- c("alpha_rim", "alpha_j2", "alpha_j3")
sigma_names <- paste0("sigma_player_type[", 1:3, "]")


# Matrix of Player Effects
get_z_mat <- function(draws_mat, t, J_players) {
  pat  <- paste0("^z_player\\[(\\d+),\\s*", t, "\\]$")
  cols <- grep(pat, colnames(draws_mat), value = TRUE)
  if (length(cols) == 0) stop("No z_player columns found for type ", t)
  
  idx <- as.integer(sub(pat, "\\1", cols))
  ord <- order(idx)
  cols <- cols[ord]
  
  if (length(cols) != J_players) {
    stop("Expected ", J_players, " z_player cols for type ", t, " but got ", length(cols),
         ". Check player_idx construction (should be 1..J_players).")
  }
  
  draws_mat[, cols, drop = FALSE]  # S x J_players
}

out_list <- vector("list", 3)

for (t in 1:3) {
  
  # draw-wise player effect on probability scale
  p_mat <- plogis(get_z_mat(draws_mat, t, J_players) * draws_mat[, sigma_names[t], drop = TRUE] + 
                    draws_mat[, alpha_names[t], drop = TRUE])                     # S x J_players
  
  out_list[[t]] <- tibble(player_idx   = 1:J_players,
                          shot_type    = t,
                          shot_family  = recode(as.character(t), `1`="rim", `2`="j2", `3`="j3"),
                          p_avg_mean   = colMeans(p_mat),
                          p_avg_q05    = apply(p_mat, 2, quantile, probs = 0.05),
                          p_avg_q95    = apply(p_mat, 2, quantile, probs = 0.95),
                          a_logit_mean = colMeans(get_z_mat(draws_mat, t, J_players) * 
                                                    draws_mat[, sigma_names[t], drop = TRUE]),
                          a_logit_q05  = apply(get_z_mat(draws_mat, t, J_players) * 
                                                 draws_mat[, sigma_names[t], drop = TRUE], 
                                               2, quantile, probs = 0.05),
                          a_logit_q95  = apply(get_z_mat(draws_mat, t, J_players) * 
                                                 draws_mat[, sigma_names[t], drop = TRUE], 
                                               2, quantile, probs = 0.95))
}

player_skill_by_type <- bind_rows(out_list) %>%
  left_join(player_map, by = "player_idx") %>%
  select(PLAYER_ID, PLAYER_NAME, shot_family,
         p_avg_mean, p_avg_q05, p_avg_q95,
         a_logit_mean, a_logit_q05, a_logit_q95) %>%
  arrange(desc(p_avg_mean))

player_skill_by_type





## 5 - Points Above Average =====================================================

# fixed/context parameters for plug-in
par_hat <- colMeans(draws_mat[, c("alpha_rim", "alpha_j2", "alpha_j3",
                                  "bD_rim", "bA_rim", "bDunk_rim",
                                  "bD_j2", "bA_j2", "bD_j3", "bA_j3"), 
                              drop = FALSE])

# Build posterior-mean a_hat[player, type] = E[sigma_t * z_{player,t}]
player_pts_by_family <- shots %>%
  mutate(eta_context = case_when(shot_type == 1L ~ par_hat[["alpha_rim"]] + 
                                   par_hat[["bD_rim"]]*D + 
                                   par_hat[["bA_rim"]]*A + 
                                   par_hat[["bDunk_rim"]]*is_dunk,
                                 shot_type == 2L ~ par_hat[["alpha_j2"]] + 
                                   par_hat[["bD_j2"]]*D + 
                                   par_hat[["bA_j2"]]*A,
                                 TRUE ~ par_hat[["alpha_j3"]] + 
                                   par_hat[["bD_j3"]]*D  + 
                                   par_hat[["bA_j3"]]*A),
         p_context = plogis(eta_context),
         p_skill   = plogis(eta_context + sapply(1:3, function(t) {
           colMeans(sweep(get_z_mat(draws_mat, t, n_distinct(shots$player_idx)), 1,
                          draws_mat[, paste0("sigma_player_type[", t, "]"), drop = TRUE],
                          "*"))})[cbind(player_idx, shot_type)]),
         points    = dplyr::if_else(shot_type == 3L, 3, 2),
         pts_above_avg_model = (p_skill - p_context) * points,
         poe_actual          = (y - p_context) * points) %>%
  group_by(player_idx, PLAYER_ID, PLAYER_NAME, shot_family) %>%
  summarise(fga = n(),
            pts_aa_model = sum(pts_above_avg_model),
            pts_aa_per100 = 100 * pts_aa_model / fga,
            poe_actual = sum(poe_actual),
            poe_actual_per100 = 100 * poe_actual / fga,
            .groups = "drop") %>%
  select(-player_idx, -PLAYER_ID) %>%
  arrange(desc(pts_aa_model))

player_pts_by_family %>% 
  as.data.frame() %>% 
  #arrange(pts_aa_per100) %>% 
  arrange(desc(pts_aa_per100)) %>% 
  #filter(shot_family == "rim") %>% 
  #filter(shot_family == "j2") %>% 
  filter(shot_family == "j3") %>% 
  filter(PLAYER_NAME %in% c("Nikola Jokić", "Stephen Curry", "Kevin Durant")) %>% 
  head()






# Plotting posterior distributions

compute_player_epaa_draws <- function(fit,
                                      shots_df,
                                      players = NULL,
                                      player_ids = NULL,
                                      player_names = NULL,
                                      ndraws = NULL,
                                      seed = 1,
                                      draws_mat = draws_mat) {
  
  req  <- c("PLAYER_ID", "PLAYER_NAME", 
            "player_idx", "shot_family", 
            "D", "A", "is_dunk", "is_three")
  miss <- setdiff(req, names(shots_df))
  if (length(miss)) stop("shots_df is missing columns: ", paste(miss, collapse = ", "))
  
  keep_idx <- shots_df %>%
    dplyr::distinct(PLAYER_ID, PLAYER_NAME, player_idx) %>%
    { if (!is.null(players)) {
      p <- as.character(players)
      dplyr::filter(., PLAYER_ID %in% p | tolower(PLAYER_NAME) %in% tolower(p))
    } else {
      out <- .
      if (!is.null(player_ids))   out <- dplyr::filter(out, PLAYER_ID %in% as.character(player_ids))
      if (!is.null(player_names)) out <- dplyr::filter(out, tolower(PLAYER_NAME) %in% 
                                                         tolower(as.character(player_names)))
      out
    } }
  
  if (!nrow(keep_idx)) stop("No players matched your input.")
  
  # subset draws to what we need (no re-draw)
  draws_mat <- draws_mat[, intersect(c("alpha_rim","alpha_j2","alpha_j3",
                                       "bD_rim","bA_rim","bDunk_rim",
                                       "bD_j2","bA_j2","bD_j3","bA_j3",
                                       paste0("sigma_player_type[", 1:3, "]"),
                                       grep("^z_player\\[", colnames(draws_mat), value = TRUE)),
                                     colnames(draws_mat)), drop = FALSE]
  
  if (!is.null(ndraws) && ndraws < nrow(draws_mat)) {
    set.seed(seed)
    draws_mat <- draws_mat[sort(sample.int(nrow(draws_mat), ndraws)), , drop = FALSE]
  }
  S <- nrow(draws_mat)
  
  .onecol <- function(pat, what) {
    nm <- grep(pat, colnames(draws_mat), value = TRUE)
    if (length(nm) != 1) stop("Expected exactly 1 column for ", what, " matching: ", pat,
                              "\nFound: ", paste(nm, collapse = ", "))
    nm
  }
  
  dplyr::bind_rows(lapply(seq_len(nrow(keep_idx)), function(k) {
    pid  <- keep_idx$PLAYER_ID[k]
    pnm  <- keep_idx$PLAYER_NAME[k]
    pidx <- keep_idx$player_idx[k]
    
    dfp <- shots_df %>%
      dplyr::filter(player_idx == pidx) %>%
      dplyr::mutate(shot_family = as.character(shot_family))
    
    sum_overall <- rep(0, S)
    fam_out <- list(); j <- 0L
    
    for (fam in c("rim","j2","j3")) {
      dff <- dplyr::filter(dfp, shot_family == fam)
      n   <- nrow(dff)
      if (!n) next
      
      if (fam == "rim") {
        cols <- c("alpha_rim","bD_rim","bA_rim","bDunk_rim")
        X    <- rbind(1, dff$D, dff$A, dff$is_dunk)
        pts  <- 2
        tt   <- 1L
      } else if (fam == "j2") {
        cols <- c("alpha_j2","bD_j2","bA_j2")
        X    <- rbind(1, dff$D, dff$A)
        pts  <- 2
        tt   <- 2L
      } else {
        cols <- c("alpha_j3","bD_j3","bA_j3")
        X    <- rbind(1, dff$D, dff$A)
        pts  <- 3
        tt   <- 3L
      }
      
      eta <- as.matrix(draws_mat[, cols, drop = FALSE]) %*% X
      a   <- draws_mat[, .onecol(paste0("^z_player\\[", pidx, ",\\s*", tt, "\\]$"),
                                 paste0("z_player[", pidx, ",", tt, "]")), drop = TRUE] *
        draws_mat[, .onecol(paste0("^sigma_player_type\\[", tt, "\\]$"),
                            paste0("sigma_player_type[", tt, "]")), drop = TRUE]
      
      dpts <- (plogis(eta + a) - plogis(eta)) * pts
      sum_overall <- sum_overall + rowSums(dpts)
      
      j <- j + 1L
      fam_out[[j]] <- tibble::tibble(draw = seq_len(S),
                                     PLAYER_ID = pid,
                                     PLAYER_NAME = pnm,
                                     shot_family = fam,
                                     epaa_pts_per_shot = rowMeans(dpts),
                                     n_shots = n)
    }
    
    dplyr::bind_rows(tibble::tibble(draw = seq_len(S),
                                    PLAYER_ID = pid,
                                    PLAYER_NAME = pnm,
                                    shot_family = "overall",
                                    epaa_pts_per_shot = sum_overall / nrow(dfp),
                                    n_shots = nrow(dfp)),
                     dplyr::bind_rows(fam_out)
    )
  }))
}

plot_player_epaa <- function(fit, shots_df, players,
                             ndraws = NULL, seed = 1,
                             draws_mat = draws_mat) {
  epaa_draws <- compute_player_epaa_draws(
    fit = fit,
    shots_df = shots_df,
    players = players,
    ndraws = ndraws,
    seed = seed,
    draws_mat = draws_mat
  )
  list(
    draws = epaa_draws,
    overall_plot   = plot_epaa_overall(epaa_draws),
    by_family_plot = plot_epaa_by_family(epaa_draws)
  )
}



# Single player (name)
res1 <- plot_player_epaa(fit9, shots, players = "Kevin Durant", ndraws = 1200)
res1$overall_plot
res1$by_family_plot

# Multiple players (names)
res2 <- plot_player_epaa(fit9, shots,
                         players = c("Stephen Curry", "Kevin Durant"),
                         ndraws = 1200)
res2$overall_plot
res2$by_family_plot

# Using player IDs also works
res3 <- plot_player_epaa(fit9, shots, players = c("203999", "201939"), ndraws = 1200)
res3$overall_plot
res3$by_family_plot





## 6 - BBall Index Plots ================================================

# helpers
.normalize_shot_types <- function(shot_types) {
  if (is.null(shot_types)) return("overall")
  st <- tolower(trimws(shot_types))
  
  # keep canonical inputs as-is
  st[st %in% c("overall", "rim", "j2", "j3")] <- 
    st[st %in% c("overall", "rim", "j2", "j3")]
  
  # replacements (use word boundaries so "j3" is NOT matched)
  st <- gsub("non[- ]?rim 2s?|2pt jumpers?|mid[- ]?range", "j2", st)
  st <- gsub("\\b3pt(s)?\\b|\\b3s?\\b|\\bthrees?\\b", "j3", st)
  st <- gsub("\\brim shots?\\b|\\bat rim\\b", "rim", st)
  st[st %in% c("total", "all")] <- "overall"
  
  bad <- setdiff(st, c("overall", "rim", "j2", "j3"))
  if (length(bad) > 0) stop(
    "Unknown shot_types: ", paste(bad, collapse=", "),
    "\nUse: overall, rim, j2 (non-rim 2s), j3 (3s)"
  )
  
  unique(st)
}



.resolve_players <- function(shots_df, players = NULL) {
  map <- shots_df %>% distinct(PLAYER_ID, PLAYER_NAME, player_idx)
  
  if (is.null(players)) return(map)
  
  p <- as.character(players)
  out <- map %>%
    filter(PLAYER_ID %in% p | tolower(PLAYER_NAME) %in% tolower(p))
  
  if (nrow(out) == 0) stop("No players matched your `players` input.")
  out
}


.extract_a_player_draws <- function(ndraws = NULL, seed = 1,
                                    draws_mat = get0("draws_mat", 
                                                     envir = parent.frame(), 
                                                     inherits = TRUE)) {
  if (is.null(draws_mat)) stop("`draws_mat` not found.")
  
  sigma_cols <- paste0("sigma_player_type[", 1:3, "]")
  draws_mat  <- as.matrix(draws_mat[, intersect(c(sigma_cols, 
                                                  grep("^z_player\\[", colnames(draws_mat),
                                                       value = TRUE)),
                                                colnames(draws_mat)), 
                                    drop = FALSE])
  
  if (!is.null(ndraws) && ndraws < nrow(draws_mat)) {
    set.seed(seed)
    draws_mat <- draws_mat[sort(sample.int(nrow(draws_mat), ndraws)), , drop = FALSE]
  }
  
  get_z <- function(t) {
    pat  <- paste0("^z_player\\[(\\d+),\\s*", t, "\\]$")
    cols <- grep(pat, colnames(draws_mat), value = TRUE)
    if (!length(cols)) stop("No z_player columns found for type ", t)
    cols[order(as.integer(sub(pat, "\\1", cols)))]
  }
  
  z <- lapply(1:3, function(t) draws_mat[, get_z(t), drop = FALSE])
  s <- draws_mat[, sigma_cols, drop = FALSE]
  
  list(a_rim = sweep(z[[1]], 1, as.numeric(s[, 1]), "*"),
       a_j2  = sweep(z[[2]], 1, as.numeric(s[, 2]), "*"),
       a_j3  = sweep(z[[3]], 1, as.numeric(s[, 3]), "*"),
       S     = nrow(draws_mat),
       J_fit = ncol(z[[1]]))
}


.compute_player_baselines_vecs <- function(shots_df, J_fit) { 
  
  req <- c("player_idx", "shot_family", "xfg", "is_three") 
  miss <- setdiff(req, names(shots_df)) 
  if (length(miss) > 0) stop("shots_df missing: ", paste(miss, collapse=", "))
  
  base <- shots_df %>% 
    dplyr::mutate(shot_family = as.character(shot_family), 
                  pts_value = 2 + is_three, xpts = xfg * pts_value ) %>% 
    dplyr::group_by(player_idx, shot_family) %>% 
    dplyr::summarise(fga = dplyr::n(), 
                     p_avg = mean(xfg), 
                     xpts_avg = mean(xpts), 
                     .groups = "drop" ) 
  
  make_vec <- function(fam, col, default) { 
    v <- rep(default, J_fit) 
    tmp <- base %>% 
      dplyr::filter(shot_family == fam) # guard: player_idx must be within [1, J_fit] 
    tmp <- tmp %>% 
      dplyr::filter(player_idx >= 1, player_idx <= J_fit) 
    v[tmp$player_idx] <- tmp[[col]] 
    v
    } 
  
  list(n_rim = make_vec("rim", "fga", 0L), 
       n_j2 = make_vec("j2", "fga", 0L), 
       n_j3 = make_vec("j3", "fga", 0L), 
       p_rim = make_vec("rim", "p_avg", 0.5), 
       p_j2 = make_vec("j2", "p_avg", 0.5), 
       p_j3 = make_vec("j3", "p_avg", 0.5), 
       xpts_rim = make_vec("rim", "xpts_avg", 0.0), 
       xpts_j2 = make_vec("j2", "xpts_avg", 0.0), 
       xpts_j3 = make_vec("j3", "xpts_avg", 0.0) ) 
  }


compute_bball_scatter_summary <- function(fit, shots_df,
                                          players = NULL,
                                          shot_types = c("overall"),
                                          ci_level = 0.95,
                                          ndraws = 1200,
                                          seed = 1,
                                          draws_mat = get0("draws_mat", 
                                                           envir = parent.frame(), 
                                                           inherits = TRUE)) {
  
  if (is.null(draws_mat)) stop("`draws_mat` not found.")
  shot_types <- .normalize_shot_types(shot_types)
  
  if (!is.numeric(ci_level) || length(ci_level) != 1) stop("ci_level must be a single number.")
  if (ci_level < 0 || ci_level > 0.99) stop("ci_level must be between 0 and 0.99.")
  alpha <- if (ci_level > 0) (1 - ci_level) / 2 else NA_real_
  
  keep_players <- .resolve_players(shots_df, players)
  
  a <- .extract_a_player_draws(ndraws = ndraws, seed = seed, draws_mat = draws_mat)
  J <- a$J_fit
  
  player_map <- tibble::tibble(player_idx = 1:J) %>%
    dplyr::left_join(shots_df %>% 
                       dplyr::distinct(player_idx, PLAYER_ID, PLAYER_NAME), 
                     by = "player_idx")
  
  b <- .compute_player_baselines_vecs(shots_df, J_fit = J)
  n_tot <- (b$n_rim + b$n_j2 + b$n_j3); n_tot[n_tot == 0] <- 1L
  
  clamp01 <- function(p, eps = 1e-6) pmin(pmax(p, eps), 1 - eps)
  lp <- list(rim = qlogis(clamp01(b$p_rim)), 
             j2 = qlogis(clamp01(b$p_j2)), 
             j3 = qlogis(clamp01(b$p_j3)))
  
  pskill <- list(rim = plogis(sweep(a$a_rim, 2, lp$rim, "+")),
                 j2  = plogis(sweep(a$a_j2,  2, lp$j2,  "+")),
                 j3  = plogis(sweep(a$a_j3,  2, lp$j3,  "+")))
  
  dprob <- list(rim = sweep(pskill$rim, 2, b$p_rim, "-"),
                j2  = sweep(pskill$j2,  2, b$p_j2,  "-"),
                j3  = sweep(pskill$j3,  2, b$p_j3,  "-"))
  
  dpts <- list(rim = dprob$rim * 2, 
               j2 = dprob$j2 * 2, 
               j3 = dprob$j3 * 3)
  
  dprob$overall <- sweep(sweep(dprob$rim, 2, b$n_rim, "*") +
                           sweep(dprob$j2,  2, b$n_j2,  "*") +
                           sweep(dprob$j3,  2, b$n_j3,  "*"),
                         2, n_tot, "/")
  dpts$overall <- sweep(sweep(dpts$rim, 2, b$n_rim, "*") +
                          sweep(dpts$j2,  2, b$n_j2,  "*") +
                          sweep(dpts$j3,  2, b$n_j3,  "*"),
                        2, n_tot, "/")
  
  qual <- list(rim = list(n = b$n_rim, 
                          xfg = b$p_rim, 
                          xpts = b$xpts_rim),
               j2  = list(n = b$n_j2,  
                          xfg = b$p_j2,  
                          xpts = b$xpts_j2),
               j3  = list(n = b$n_j3,  
                          xfg = b$p_j3,  
                          xpts = b$xpts_j3),
               overall = list(n = n_tot,
                              xfg  = (b$n_rim*b$p_rim + 
                                        b$n_j2*b$p_j2 + 
                                        b$n_j3*b$p_j3) / n_tot,
                              xpts = (b$n_rim*b$xpts_rim + 
                                        b$n_j2*b$xpts_j2 + 
                                        b$n_j3*b$xpts_j3) / n_tot))
  
  summarise_one <- function(fam) {
    ypp  <- 100 * dprob[[fam]]
    ypts <- dpts[[fam]]
    tibble::tibble(player_idx = 1:J,
                   shot_family = fam,
                   fga = qual[[fam]]$n,
                   xfg_quality  = 100 * qual[[fam]]$xfg,
                   xpts_quality = qual[[fam]]$xpts,
                   talent_pp_mean = colMeans(ypp),
                   talent_pp_lo   = if (ci_level > 0) 
                     apply(ypp,  2, stats::quantile, probs = alpha) else NA_real_,
                   talent_pp_hi   = if (ci_level > 0) 
                     apply(ypp,  2, stats::quantile, probs = 1 - alpha) else NA_real_,
                   epaa_mean      = colMeans(ypts),
                   epaa_lo        = if (ci_level > 0) 
                     apply(ypts, 2, stats::quantile, probs = alpha) else NA_real_,
                   epaa_hi        = if (ci_level > 0) 
                     apply(ypts, 2, stats::quantile, probs = 1 - alpha) else NA_real_) %>%
      dplyr::mutate(epaa_total_mean = epaa_mean * fga) %>%
      dplyr::left_join(player_map, by = "player_idx")
              }
  
  dplyr::bind_rows(lapply(shot_types, summarise_one)) %>%
    dplyr::semi_join(keep_players, by = c("PLAYER_ID","PLAYER_NAME","player_idx")) %>%
    dplyr::mutate(shot_family = factor(shot_family, levels = c("overall","rim","j2","j3")))
}


plot_bball_index_map <- function(fit, shots_df,
                                 players = NULL,
                                 shot_types = c("overall"),
                                 axis_set = c("xfg_vs_talent", "xpts_vs_epaa"),
                                 ci_level = 0.95,
                                 ndraws = 1200,
                                 seed = 1,
                                 min_fga = 0,
                                 label_top_n = 10,
                                 label_all_if_small = TRUE,
                                 separate_panels = TRUE,
                                 draws_mat = get0("draws_mat", 
                                                  envir = parent.frame(), 
                                                  inherits = TRUE)) {
  
  if (is.null(draws_mat)) stop("`draws_mat` not found.")
  axis_set   <- match.arg(axis_set)
  shot_types <- .normalize_shot_types(shot_types)
  
  summ <- compute_bball_scatter_summary(fit = fit, 
                                        shots_df = shots_df, 
                                        players = players, 
                                        shot_types = shot_types,
                                        ci_level = ci_level, 
                                        ndraws = ndraws, 
                                        seed = seed, 
                                        draws_mat = draws_mat) %>% 
    dplyr::filter(fga >= min_fga)
  
  if (!nrow(summ)) stop("No rows left after min_fga filter. Lower min_fga.")
  
  ax <- if (axis_set == "xfg_vs_talent")
    list(x = "xfg_quality", 
         y = "talent_pp_mean", 
         ylo = "talent_pp_lo", 
         yhi = "talent_pp_hi",
         xlab = "Shot quality (avg-player xFG% on player's shot mix)",
         ylab = "Shooting talent (make% over expected, percentage points)")
  else
    list(x = "xpts_quality", 
         y = "epaa_mean", 
         ylo = "epaa_lo", 
         yhi = "epaa_hi",
         xlab = "Shot quality (avg-player xPoints per shot on player's shot mix)",
         ylab = "Shooting talent (expected points above average per shot)")
  
  want_all <- !is.null(players) && label_all_if_small &&
    (summ %>% dplyr::distinct(PLAYER_ID) %>% nrow()) <= (label_top_n + 5)
  
  build <- function(df, fam) {
    lab <- if (want_all) df else dplyr::slice_max(df, epaa_total_mean, n = label_top_n, with_ties = FALSE)
    p <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[ax$x]], y = .data[[ax$y]], color = fga)) +
      ggplot2::geom_point(alpha = 0.90, size = 2.3) +
      { if (ci_level > 0) ggplot2::geom_errorbar(ggplot2::aes(ymin = .data[[ax$ylo]], ymax = .data[[ax$yhi]]),
                                                 width = 0, alpha = 0.55) } +
      ggplot2::scale_color_viridis_c(name = "FGA") +
      ggplot2::labs(title = paste0("Shooting Talent vs Shot Quality — ", fam),
                    subtitle = if (ci_level > 0) paste0(round(ci_level*100), 
                                                        "% credible intervals") else "Point estimates only",
                    x = ax$xlab, y = ax$ylab) +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(plot.title = ggplot2::element_text(face = "bold"))
    
    if (nrow(lab)) {
      p <- p + if (requireNamespace("ggrepel", quietly = TRUE))
        ggrepel::geom_text_repel(data = lab, ggplot2::aes(label = PLAYER_NAME),
                                 size = 3, max.overlaps = Inf,
                                 box.padding = 0.2, point.padding = 0.15, min.segment.length = 0)
      else
        ggplot2::geom_text(data = lab, ggplot2::aes(label = PLAYER_NAME), size = 3, vjust = -0.5)
    }
    p
  }
  
  if (length(shot_types) == 1 || !separate_panels) {
    fam <- if (length(unique(summ$shot_family)) == 1) as.character(unique(summ$shot_family)) else "multiple"
    return(list(summary = summ, plot = build(summ, fam)))
  }
  
  plots <- setNames(lapply(shot_types, function(f) 
    build(dplyr::filter(summ, shot_family == f), f)), shot_types)
  list(summary = summ,
       plot = if (requireNamespace("patchwork", quietly = TRUE)) 
         patchwork::wrap_plots(plots, nrow = 1) else plots[[1]],
       plots = plots)
}


# EXAMPLES

# 1) All players, overall, xFG vs talent (pp), label top 10
res <- plot_bball_index_map(fit9, shots_out,
                            shot_types = "overall", 
                            axis_set = "xfg_vs_talent", 
                            ci_level = 0.95,
                            min_fga = 200,
                            label_top_n = 12)
res$plot

# 2) Curry/Jokic/KD across rim/j2/j3 in one row, per-panel color scales
res2 <- plot_bball_index_map(fit9, shots_out,
                             players = c("Stephen Curry", 
                                         "James Harden", 
                                         "Kevin Durant"),
                             shot_types = c("rim", "j2", "j3"),
                             axis_set = "xpts_vs_epaa", 
                             ci_level = 0.80,
                             min_fga = 50,
                             label_top_n = 10)
res2$plot



shots_out %>% 
  group_by(shot_family) %>% 
  summarise(mean_xfg = mean(xfg),
            mean_fg = mean(SHOT_MADE_FLAG),
            p05 = quantile(xfg, probs = 0.05, na.rm = TRUE),
            p25 = quantile(xfg, probs = 0.25, na.rm = TRUE),
            p75 = quantile(xfg, probs = 0.75, na.rm = TRUE),
            p95 = quantile(xfg, probs = 0.95, na.rm = TRUE))

shots_out %>% 
  group_by(shot_family, PLAYER_NAME) %>% 
  summarise(mean_xfg = mean(xfg),
            mean_fg = mean(SHOT_MADE_FLAG),
            fga = sum(SHOT_ATTEMPTED_FLAG)) %>%
  #filter(fga >= 100) %>% 
  group_by(shot_family) %>% 
  summarise(p05 = quantile(mean_fg, probs = 0.05, na.rm = TRUE),
            p25 = quantile(mean_fg, probs = 0.25, na.rm = TRUE),
            p50 = quantile(mean_fg, probs = 0.50, na.rm = TRUE),
            p75 = quantile(mean_fg, probs = 0.75, na.rm = TRUE),
            p95 = quantile(mean_fg, probs = 0.95, na.rm = TRUE))





