# ============================================================
# Expected FG% (xFG%) - xG-style build in NBA using hoopR + Stan
# Brooklyn Nets case study
# ============================================================

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

# Stan options
set_cmdstan_path(path = cmdstanr::cmdstan_path())
options(mc.cores = parallel::detectCores())


# ============================================================
# Config (defaults = all teams, all players)
# ============================================================
season <- year_to_season(2024)      # e.g., "2024-25" depending on hoopR
season_type <- "Regular Season"

# Optional filters (default NULL = no filtering)
team_filter <- NULL   # examples: c("Brooklyn Nets"), c("BKN"), c(1610612751)
player_filter <- NULL # examples: c("Mikal Bridges"), c("1629651")

# ============================================================
# Helpers: normalize team/player filters
# ============================================================
teams_tbl <- nba_teams() %>%
  mutate(
    team_id_chr = as.character(team_id),
  )

resolve_team_ids <- function(team_filter, teams_tbl) {
  if (is.null(team_filter)) return(NULL)
  
  tf <- as.character(team_filter)
  
  # Match by numeric team_id OR abbreviation OR full name (case-insensitive)
  ids <- teams_tbl %>%
    filter(
      team_id_chr %in% tf |
        tolower(team_abbrev) %in% tolower(tf) |
        tolower(team_name_full) %in% tolower(tf)
    ) %>%
    pull(team_id) %>%
    unique()
  
  if (length(ids) == 0) stop("team_filter matched 0 teams. Check spelling/IDs.")
  ids
}

resolve_player_ids <- function(player_filter, season, season_type) {
  if (is.null(player_filter)) return(NULL)
  
  pf <- as.character(player_filter)
  
  # Pull league player list from dashboard (has PLAYER_ID + PLAYER_NAME)
  pl <- nba_leaguedashplayerstats(
    season = season,
    season_type = season_type
  )$LeagueDashPlayerStats %>%
    transmute(
      player_id = as.character(PLAYER_ID),
      player_name = PLAYER_NAME
    )
  
  ids <- pl %>%
    filter(
      player_id %in% pf |
        tolower(player_name) %in% tolower(pf)
    ) %>%
    pull(player_id) %>%
    unique()
  
  if (length(ids) == 0) stop("player_filter matched 0 players. Check spelling/IDs.")
  ids
}

team_ids <- resolve_team_ids(team_filter, teams_tbl)
player_ids <- resolve_player_ids(player_filter, season, season_type)

# ============================================================
# Pull shots: LEAGUE-WIDE by default
# ============================================================
pull_shots_league <- function(season, season_type) {
  res <- nba_shotchartdetail(
    player_id = 0,   # 0 means "all players" (league)
    team_id   = 0,   # 0 means "all teams" (league)
    season    = season,
    season_type = season_type,
    context_measure = "FGA"
  )
  
  as_tibble(res$Shot_Chart_Detail)
}

shots_raw <- pull_shots_league(season, season_type)

# ============================================================
# Apply optional filters
# ============================================================
shots_raw_filt <- shots_raw %>%
  mutate(
    PLAYER_ID = as.character(PLAYER_ID),
    TEAM_ID   = as.character(TEAM_ID)
  ) %>%
  { if (!is.null(team_ids)) 
    filter(., TEAM_ID %in% as.character(team_ids)) else . } %>%
  { if (!is.null(player_ids)) 
    filter(., PLAYER_ID %in% as.character(player_ids)) else . }

# ============================================================
# Basic cleaning (same as your original)
# ============================================================
shots <- shots_raw_filt %>%
  mutate(
    y = as.integer(SHOT_MADE_FLAG),
    loc_x = as.numeric(LOC_X),
    loc_y = as.numeric(LOC_Y),
    shot_dist = as.numeric(SHOT_DISTANCE),
    period = as.integer(PERIOD),
    min_rem = as.integer(MINUTES_REMAINING),
    sec_rem = as.integer(SECONDS_REMAINING)
  ) %>%
  filter(!is.na(y), !is.na(loc_x), !is.na(loc_y), !is.na(shot_dist)) %>%
  distinct(GAME_ID, GAME_EVENT_ID, PLAYER_ID, .keep_all = TRUE)

glimpse(shots)


# Court coordinate notes:
# LOC_X / LOC_Y are in the NBA shotchart coordinate system.
# We’ll use SHOT_DISTANCE and derive an angle from LOC_X/LOC_Y.

zscore <- function(x) as.numeric(scale(x))

shots2 <- shots %>%
  mutate(
    # symmetry: left/right should be equivalent for "difficulty"
    angle = atan2(abs(loc_x), pmax(loc_y, 1e-6)), 
    # radians, 0=straight, larger=more baseline/corner-ish
    
    is_three = as.integer(str_detect(SHOT_TYPE, "3PT")),
    is_dunk = as.integer(str_detect(ACTION_TYPE, "Dunk")),
    is_rim = as.integer(SHOT_ZONE_BASIC == "Restricted Area" |
                          str_detect(ACTION_TYPE, "Dunk|Layup|Tip") |
                          shot_dist <= 4),
    
    # crude jumper flag (you can refine)
    is_jump = as.integer(!is_rim),
    
    # game clock within period (optional; NBA official xFG% excludes “situational”)
    sec_in_period = (min_rem * 60 + sec_rem),
    
    # keep a compact set of “action types”
    action_slim = case_when(
      str_detect(ACTION_TYPE, "Dunk") ~ "Dunk",
      str_detect(ACTION_TYPE, "Layup") ~ "Layup",
      str_detect(ACTION_TYPE, "Hook") ~ "Hook",
      str_detect(ACTION_TYPE, "Fadeaway|Turnaround") ~ "Turn/Fade",
      str_detect(ACTION_TYPE, "Step Back") ~ "StepBack",
      str_detect(ACTION_TYPE, "Pullup|Pull-Up") ~ "PullUp",
      str_detect(ACTION_TYPE, "Jump Shot") ~ "JumpShot",
      TRUE ~ "Other"
    )
  ) %>%
  filter(shot_dist <= 60) %>%  # drop heaves
  mutate(
    D = zscore(shot_dist),
    A = zscore(angle),
    T = zscore(sec_in_period),
    # Indexing for hierarchical components
    player_idx = as.integer(factor(PLAYER_ID)),
    zone_idx   = as.integer(factor(SHOT_ZONE_BASIC)),
    action_idx = as.integer(factor(action_slim))
  )


list(
  N = nrow(shots2),
  n_players = n_distinct(shots2$player_idx),
  n_zones = n_distinct(shots2$zone_idx),
  n_actions = n_distinct(shots2$action_idx)
)



# ---------------------------
# Stan data lists
# ---------------------------
dat1 <- list(N = nrow(shots2), y = shots2$y)

dat2 <- c(dat1, list(D = shots2$D))

dat3 <- c(dat1, list(
  D = shots2$D,
  A = shots2$A,
  is_three = shots2$is_three,
  is_dunk = shots2$is_dunk
))

dat6 <- c(dat1, list(
  D = shots2$D,
  A = shots2$A,
  is_three = shots2$is_three,
  is_dunk = shots2$is_dunk,
  is_jump = shots2$is_jump,
  J_player = max(shots2$player_idx),
  player   = shots2$player_idx
))

dat4 <- c(dat3, list(
  J_zone = max(shots2$zone_idx),
  zone   = shots2$zone_idx,
  J_action = max(shots2$action_idx),
  action   = shots2$action_idx
))

datF <- c(dat4, list(
  is_jump = shots2$is_jump,
  J_player = max(shots2$player_idx),
  player   = shots2$player_idx
))

# ---------------------------
# Compile + sample
# ---------------------------
m1 <- cmdstan_model("stan/nba_xfg1.stan")
m2 <- cmdstan_model("stan/nba_xfg2.stan")
m3 <- cmdstan_model("stan/nba_xfg3.stan")
m4 <- cmdstan_model("stan/nba_xfg4.stan")
m6 <- cmdstan_model("stan/nba_xfg6.stan")
mF <- cmdstan_model("stan/nba_xfg_final.stan")

# Start smaller while iterating; then scale up
fit1 <- m1$sample(data = dat1, chains = 4, iter_warmup = 800, iter_sampling = 800, 
                  seed = 1)
fit2 <- m2$sample(data = dat2, chains = 4, iter_warmup = 800, iter_sampling = 800, 
                  seed = 1)
fit3 <- m3$sample(data = dat3, chains = 4, iter_warmup = 800, iter_sampling = 800, 
                  seed = 1)
fit4 <- m4$sample(data = dat4, chains = 4, iter_warmup = 800, iter_sampling = 800, 
                  seed = 1)
fit6 <- m6$sample(data = dat6, chains = 4, iter_warmup = 800, iter_sampling = 800, 
                  seed = 1)
fitF <- mF$sample(data = datF, 
                  chains = 4, 
                  iter_warmup = 1000, 
                  iter_sampling = 1000, 
                  seed = 1, 
                  adapt_delta = 0.95)

fit1$summary(variables = c('alpha'))
fit2$summary(variables = c('alpha', 'beta_D'))
fit3$summary(variables = c('alpha', 'beta_D', 'beta_A', 'beta_3', 'beta_dunk'))

# LOO comparisons
loo1 <- fit1$loo()
loo2 <- fit2$loo()
loo3 <- fit3$loo()
loo4 <- fit4$loo()
looF <- fitF$loo()

loo_compare(loo1, loo2)
loo_compare(loo1, loo2, loo3, loo4, looF)



# Extract per-shot xFG (context-only) from generated quantities
draws <- fit1$draws(variables = c("p_xfg", "p_skill")) %>% posterior::as_draws_rvars()
draws <- fit2$draws(variables = c("p_xfg")) %>% posterior::as_draws_rvars()
draws <- fit3$draws(variables = c("p_xfg")) %>% posterior::as_draws_rvars()
draws <- fitF$draws(variables = c("p_xfg", "p_skill")) %>% posterior::as_draws_rvars()

# Posterior mean xFG per shot (average-player context-only)
shots_out <- shots2 %>%
  mutate(xfg = as.numeric(posterior::E(draws$p_xfg)),
         xpoints = (xfg * (2 + is_three)))
#xfg = as_draws_rvars(fit2$draws(variables = 'p_xfg'))$p_xfg)
  #mutate(xfg = as.numeric(posterior::mean(draws$p_xfg)),
  #       p_skill = as.numeric(posterior::mean(draws$p_skill)))

# Player-level summary:
# - "Shot Difficulty" ~ average xFG (lower = harder shots)
# - "FG% over expected" ~ actual FG% - expected FG%
player_tbl <- shots_out %>%
  group_by(PLAYER_ID, PLAYER_NAME) %>%
  summarise(
    fga = n(),
    fg_pct = mean(y),
    xfg_pct = mean(xfg),
    fg_over_exp = fg_pct - xfg_pct,
    exp_makes = sum(xfg),
    act_makes = sum(y),
    makes_over_exp = act_makes - exp_makes,
    .groups = "drop"
  ) %>%
  arrange(desc(fg_over_exp))

player_tbl


shots_out %>%
  mutate(
    x_bin = round(loc_x / 10) * 10,
    y_bin = round(loc_y / 10) * 10
  ) %>%
  group_by(x_bin, y_bin) %>%
  summarise(xfg = mean(xfg), n = n(), .groups = "drop") %>%
  filter(n >= 10) %>%
  ggplot(aes(x = x_bin, y = y_bin, fill = xfg)) +
  geom_raster() +
  coord_fixed() +
  labs(title = "Brooklyn Nets: expected FG% (context-only) by shot location",
       x = "LOC_X", y = "LOC_Y", fill = "xFG") +
  theme_minimal()



# ---------- geometry helpers ----------
arc_df <- function(center = c(0, 0), r, start, end, n = 200) {
  t <- seq(start, end, length.out = n)
  tibble(
    x = center[1] + r * cos(t),
    y = center[2] + r * sin(t)
  )
}

rect_df <- function(xmin, xmax, ymin, ymax) {
  tibble(
    x = c(xmin, xmax, xmax, xmin, xmin),
    y = c(ymin, ymin, ymax, ymax, ymin)
  )
}

# ---------- court layers (NBA Stats shotchart coordinates) ----------
nba_court_layers <- function(
    line_color = "black",
    line_size  = 0.35
) {
  
  # Dimensions in NBA Stats shotchart coordinate system (same as LOC_X/LOC_Y)
  x_min <- -250
  x_max <-  250
  y_min <- -47.5
  y_max <-  422.5
  
  # Key / paint
  lane_half_width <- 80        # 16 ft wide lane -> +/- 8 ft -> 80 units
  ft_line_y <- 142.5           # free throw line (y)
  backboard_y <- -7.5
  rim_center <- c(0, 0)
  rim_r <- 7.5                 # rim radius
  restricted_r <- 40           # 4 ft restricted arc radius
  
  # 3pt
  corner_x <- 220              # 22 ft corner 3
  corner_y <- 92.5             # where corner line meets arc
  three_r  <- 237.5            # 23.75 ft arc radius
  theta_corner <- atan2(corner_y, corner_x) # arc start angle
  
  # Circles
  ft_circle_r <- 60            # 6 ft radius free throw circle
  
  # Data frames for lines/arcs
  outer <- rect_df(x_min, x_max, y_min, y_max)
  lane  <- rect_df(-lane_half_width, lane_half_width, y_min, ft_line_y)
  
  # Backboard + rim
  backboard <- tibble(x = c(-30, 30), y = c(backboard_y, backboard_y))
  rim <- arc_df(center = rim_center, r = rim_r, start = 0, end = 2*pi, n = 200)
  
  # Restricted area arc (upper semicircle only)
  restricted <- arc_df(center = rim_center, 
                       r = restricted_r, 
                       start = 0, end = pi, n = 200)
  
  # Free throw circle: top half solid, bottom half dashed
  ft_circle_top <- arc_df(center = c(0, ft_line_y), r = ft_circle_r, 
                          start = 0, end = pi, n = 200)
  ft_circle_bot <- arc_df(center = c(0, ft_line_y), r = ft_circle_r, 
                          start = pi, end = 2*pi, n = 200)
  
  # 3pt lines (corners) + arc
  corner_left  <- tibble(x = c(-corner_x, -corner_x), y = c(y_min, corner_y))
  corner_right <- tibble(x = c( corner_x,  corner_x), y = c(y_min, corner_y))
  three_arc <- arc_df(center = rim_center, 
                      r = three_r, 
                      start = theta_corner, 
                      end = pi - theta_corner, n = 400)
  
  # Half-court line (top boundary of this shotchart half)
  half_line <- tibble(x = c(x_min, x_max), y = c(y_max, y_max))
  
  # Small “charge circle” hash marks are optional; keeping it clean for xFG heatmaps.
  
  list(
    # Outer boundary
    geom_path(data = outer, aes(x, y), inherit.aes = FALSE,
              color = line_color, linewidth = line_size),
    
    # Half court line
    geom_segment(data = half_line, aes(x = x[1], xend = x[2], y = y[1], yend = y[2]),
                 inherit.aes = FALSE, color = line_color, linewidth = line_size),
    
    # Paint
    geom_path(data = lane, aes(x, y), inherit.aes = FALSE,
              color = line_color, linewidth = line_size),
    
    # Free throw line
    geom_segment(aes(x = -lane_half_width, xend = lane_half_width, 
                     y = ft_line_y, yend = ft_line_y),
                 inherit.aes = FALSE, color = line_color, linewidth = line_size),
    
    # Free throw circle (top solid, bottom dashed)
    geom_path(data = ft_circle_top, aes(x, y), inherit.aes = FALSE,
              color = line_color, linewidth = line_size),
    geom_path(data = ft_circle_bot, aes(x, y), inherit.aes = FALSE,
              color = line_color, linewidth = line_size, linetype = "dashed"),
    
    # Restricted area
    geom_path(data = restricted, aes(x, y), inherit.aes = FALSE,
              color = line_color, linewidth = line_size),
    
    # Backboard + rim
    geom_segment(data = backboard, aes(x = x[1], xend = x[2], y = y[1], yend = y[2]),
                 inherit.aes = FALSE, color = line_color, linewidth = line_size),
    geom_path(data = rim, aes(x, y), inherit.aes = FALSE,
              color = line_color, linewidth = line_size),
    
    # 3pt
    geom_segment(data = corner_left, aes(x = x[1], xend = x[2], 
                                         y = y[1], yend = y[2]),
                 inherit.aes = FALSE, color = line_color, linewidth = line_size),
    geom_segment(data = corner_right, aes(x = x[1], xend = x[2], 
                                          y = y[1], yend = y[2]),
                 inherit.aes = FALSE, color = line_color, linewidth = line_size),
    geom_path(data = three_arc, aes(x, y), inherit.aes = FALSE,
              color = line_color, linewidth = line_size)
  )
}



# Example grid in same coordinate space as LOC_X/LOC_Y
nd <- tidyr::expand_grid(
  x = seq(-250, 250, by = 5),
  y = seq(-47.5, 422.5, by = 5)
) %>%
  mutate(D = get_distance(x, y))

post_efg1 <- fit1$draws(variables = c('alpha')) %>% as_draws_rvars()

invlogit <- function(x) exp(x) / (1 + exp(x))

nd <- nd %>% 
  mutate(pi = invlogit(post_efg1),
         E_pi = mean(pi))

# nd must contain E_pi in [0,1]
# nd <- nd %>% mutate(E_pi = ...)

ggplot(nd) +
  coord_fixed(xlim = c(-250, 250), ylim = c(-47.5, 422.5)) +
  geom_raster(aes(x = x, y = y, fill = E_pi), interpolate = TRUE) +
  scale_fill_gradient(name = "xFG%", low = "black", high = "white",
                      labels = scales::percent_format(accuracy = 1)) +
  nba_court_layers(line_color = "black", line_size = 0.35) +
  theme_void() +
  theme(
    legend.position = "right",
    plot.title = element_text(hjust = 0.5)
  ) +
  labs(title = "Expected FG% surface (NBA court)")
