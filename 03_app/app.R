## ═══════════════════════════════════════════════════════════════════════════════
## app.R — EPAA Shot Explorer
##
## Interactive Shiny app for the EPAA (Expected Points Above Average) framework.
## Click anywhere on a basketball half-court to set a shot location, configure
## context features, select a shooter/defender/team, and see:
##   - Baseline xFG% (league-average for the shot context)
##   - 90% credible intervals after applying shooter/defender/team random effects
##   - Isolated contribution of each actor
##   - Shot grade (percentile + letter grade vs. league distribution)
##
## Required packages:
##   shiny, bslib, shinyjs, ggplot2, xgboost, dplyr, htmltools
## ═══════════════════════════════════════════════════════════════════════════════

library(shiny)
library(bslib)
library(shinyjs)
library(ggplot2)
library(xgboost)
library(dplyr)
library(htmltools)
library(Matrix)

inv_logit <- function(x) 1 / (1 + exp(-x))

## ═══════════════════════════════════════════════════════════════════════════════
## DATA LOADING ================================================================
## ═══════════════════════════════════════════════════════════════════════════════

player_map <- read.csv("player_map.csv")
defender_map <- read.csv("defender_map.csv")
defteam_map <- read.csv("defteam_map.csv")
shots <- read.csv("shots.csv")

data_dir <- ""

# XGBoost models
xgb_models <- list(rim = readRDS("xgb_rim.rds"),
                   j2  = readRDS("xgb_j2.rds"),
                   j3  = readRDS("xgb_j3.rds"))

# Calibration draws (n_draws × 6)
draws_mat <- readRDS(file.path("draws_mat.rds"))
n_draws   <- nrow(draws_mat)
cal_draws <- draws_mat[, c(paste0("cal_intercept[", 1:3, "]"),
                           paste0("cal_slope[", 1:3, "]"))]

# Random effect draws — extract per-family matrices in one pass
extract_re <- function(prefix, J) {
  lapply(1:3, function(t)
    draws_mat[, paste0(prefix, "[", 1:J, ",", t, "]"), drop = FALSE])
}

J_player   <- max(as.integer(gsub(".*\\[(\\d+),.*", "\\1",
                                  grep("^a_player\\[", colnames(draws_mat), value = TRUE))))
J_defender <- max(as.integer(gsub(".*\\[(\\d+),.*", "\\1",
                                  grep("^a_defender\\[", colnames(draws_mat), value = TRUE))))
J_defteam  <- max(as.integer(gsub(".*\\[(\\d+),.*", "\\1",
                                  grep("^a_defteam\\[", colnames(draws_mat), value = TRUE))))

cat("  J_player =", J_player, " J_defender =", J_defender,
    " J_defteam =", J_defteam, "\n")

player_draws   <- extract_re("a_player",   J_player)
defender_draws <- extract_re("a_defender", J_defender)
defteam_draws  <- extract_re("a_defteam",  J_defteam)

# Named choice vectors for selectize inputs
player_choices  <- setNames(player_map$player_idx, player_map$player_name_full)
defender_choices <- setNames(defender_map$defender_idx, defender_map$defender_name)
defteam_choices <- setNames(defteam_map$defteam_idx, defteam_map$defender_team)

# Player ID lookup for headshots
player_id_lookup <- setNames(as.character(player_map$player1_id),
                              as.character(player_map$player_idx))
defender_id_lookup <- setNames(as.character(defender_map$defender_id),
                                as.character(defender_map$defender_idx))

# Defender → primary team lookup (most common team each defender played for)
.shots_link <- shots[, c("defender_idx", "defteam_idx")]
.shots_link <- na.omit(.shots_link)
def_team_link <- tapply(.shots_link$defteam_idx, .shots_link$defender_idx, function(x) {
  as.integer(names(which.max(table(x))))
})
rm(.shots_link)

# Build empirical CDF of expected points for shot grading
cat("Building xPts empirical CDF for shot grading...\n")
xpts_ecdf <- ecdf(shots$xpoints)


## ═══════════════════════════════════════════════════════════════════════════════
## CONSTANTS & HELPERS =========================================================
## ═══════════════════════════════════════════════════════════════════════════════

# Feature encoding levels (must match 02_xgb pipeline)
DEFENDER_DIST_LEVELS <- c("0-2 Feet - Very Tight" = 1L,
                          "2-4 Feet - Tight"      = 2L,
                          "4-6 Feet - Open"        = 3L,
                          "6+ Feet - Wide Open"    = 4L)

DRIBBLE_LEVELS <- c("0 Dribbles"   = 1L,
                     "1 Dribble"    = 2L,
                     "2 Dribbles"   = 3L,
                     "3-6 Dribbles" = 4L,
                     "7+ Dribbles"  = 5L)

# Shot family classification from court position
classify_shot <- function(x, y) {
  dist <- sqrt(x^2 + y^2)
  is_corner <- abs(x) >= 22
  three_pt_dist <- ifelse(is_corner, 22, 23.75)
  if (dist <= 4) return(list(family = "rim", type = 1L, dist = dist))
  if (dist >= three_pt_dist) return(list(family = "j3", type = 3L, dist = dist))
  return(list(family = "j2", type = 2L, dist = dist))
}

# Headshot URL builder
get_headshot_url <- function(player_id) {
  if (is.null(player_id) || is.na(player_id) || player_id == "") return(NULL)
  paste0("https://cdn.nba.com/headshots/nba/latest/1040x760/",
         as.integer(player_id), ".png")
}

# Shot grade helpers
get_grade_color <- function(pct) {
  # Red (0) → Yellow (50) → Green (100)
  if (pct <= 50) {
    r <- 255
    g <- as.integer(255 * (pct / 50))
  } else {
    r <- as.integer(255 * (1 - (pct - 50) / 50))
    g <- 255
  }
  sprintf("rgb(%d, %d, 40)", r, g)
}

get_letter_grade <- function(pct) {
  if (pct >= 90) "A"
  else if (pct >= 80) "B"
  else if (pct >= 70) "C"
  else if (pct >= 60) "D"
  else "F"
}


## ═══════════════════════════════════════════════════════════════════════════════
## COURT DRAWING ===============================================================
## ═══════════════════════════════════════════════════════════════════════════════

draw_court <- function(shot_x = NULL, shot_y = NULL, shot_family = NULL) {
  
  court_color <- "#dfbb85"
  paint_color <- "#c9a870"
  line_color  <- "#FFFFFF"
  line_size   <- 0.7
  
  # Three-point arc
  arc_angles <- seq(-68, 68, length.out = 200) * pi / 180
  three_arc_x <- 23.75 * sin(arc_angles)
  three_arc_y <- 23.75 * cos(arc_angles)
  
  # Free throw circle
  ft_angles <- seq(0, 2 * pi, length.out = 100)
  ft_x <- 6 * cos(ft_angles)
  ft_y <- 6 * sin(ft_angles) + 13.75
  
  # Restricted area arc (opens upward)
  ra_angles <- seq(0, pi, length.out = 50)
  ra_x <- 4 * cos(ra_angles)
  ra_y <- 4 * sin(ra_angles)
  
  p <- ggplot() +
    annotate("rect", xmin = -25, xmax = 25, ymin = -5.25, ymax = 41.75,
             fill = court_color, color = NA) +
    annotate("rect", xmin = -8, xmax = 8, ymin = -5.25, ymax = 13.75,
             fill = paint_color, color = line_color, linewidth = line_size) +
    annotate("segment", x = -3, xend = 3, y = -1.25, yend = -1.25,
             color = line_color, linewidth = 1.2) +
    annotate("point", x = 0, y = 0, size = 4, shape = 21,
             color = "#FF6B35", fill = NA, stroke = 1.5) +
    annotate("path", x = ra_x, y = ra_y,
             color = line_color, linewidth = line_size) +
    annotate("segment", x = -8, xend = 8, y = 13.75, yend = 13.75,
             color = line_color, linewidth = line_size) +
    annotate("path", x = ft_x, y = ft_y,
             color = line_color, linewidth = line_size * 0.7,
             linetype = "dashed") +
    annotate("path", x = three_arc_x, y = three_arc_y,
             color = line_color, linewidth = line_size) +
    annotate("segment", x = -22, xend = -22, y = -5.25, yend = three_arc_y[1],
             color = line_color, linewidth = line_size) +
    annotate("segment", x = 22, xend = 22, y = -5.25, yend = three_arc_y[1],
             color = line_color, linewidth = line_size) +
    annotate("rect", xmin = -25, xmax = 25, ymin = -5.25, ymax = 41.75,
             fill = NA, color = line_color, linewidth = 1.0) +
    annotate("segment", x = -25, xend = 25, y = -5.25, yend = -5.25,
             color = line_color, linewidth = 1.2) +
    annotate("segment", x = -25, xend = 25, y = 41.75, yend = 41.75,
             color = line_color, linewidth = 1.0) +
    annotate("path",
             x = 6 * cos(seq(pi, 2*pi, length.out = 50)),
             y = 6 * sin(seq(pi, 2*pi, length.out = 50)) + 41.75,
             color = line_color, linewidth = line_size) +
    coord_fixed(xlim = c(-26, 26), ylim = c(-6.5, 43)) +
    theme_void() +
    theme(
      plot.background  = element_rect(fill = "#1a1a2e", color = NA),
      panel.background = element_rect(fill = "#1a1a2e", color = NA),
      plot.margin      = margin(5, 5, 5, 5)
    )
  
  if (!is.null(shot_x) && !is.null(shot_y)) {
    marker_color <- switch(shot_family %||% "j2",
                           rim = "#FF6B35", j2 = "#FFD700", j3 = "#00D4FF")
    p <- p +
      annotate("point", x = shot_x, y = shot_y, size = 6, shape = 21,
               fill = marker_color, color = "white", stroke = 1.5) +
      annotate("segment", x = shot_x, y = shot_y, xend = 0, yend = 0,
               color = "white", linewidth = 0.3, linetype = "dotted", alpha = 0.5)
  }
  
  p
}


## ═══════════════════════════════════════════════════════════════════════════════
## CUSTOM CSS ==================================================================
## ═══════════════════════════════════════════════════════════════════════════════

app_css <- "
/* ── Global ── */
body { background: #0f0f23; }

.card {
  background: #1a1a2e !important;
  border: 1px solid #2a2a4a !important;
  border-radius: 12px !important;
}

.card-header {
  background: transparent !important;
  border-bottom: 1px solid #2a2a4a !important;
  color: #e0e0e0 !important;
  font-weight: 600;
  letter-spacing: 0.03em;
}

/* ── Inputs ── */
.form-label, .control-label {
  color: #a0a0c0 !important;
  font-size: 0.78rem !important;
  font-weight: 500 !important;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  margin-bottom: 2px !important;
}

.form-control, .selectize-input, .form-select {
  background: #12122a !important;
  border: 1px solid #2a2a4a !important;
  color: #e0e0e0 !important;
  border-radius: 8px !important;
  font-size: 0.88rem !important;
}

.selectize-input.focus { border-color: #FFD700 !important; }

.selectize-dropdown {
  background: #1a1a2e !important;
  border: 1px solid #2a2a4a !important;
  color: #e0e0e0 !important;
}
.selectize-dropdown .active {
  background: #2a2a5a !important;
  color: #FFD700 !important;
}
.selectize-dropdown-content .option {
  color: #c0c0d0 !important;
}

.form-check-input:checked {
  background-color: #FFD700 !important;
  border-color: #FFD700 !important;
}

/* Disabled checkbox styling */
.form-check-input:disabled {
  opacity: 0.3 !important;
  cursor: not-allowed !important;
}
.form-check-input:disabled + .form-check-label {
  opacity: 0.3 !important;
  cursor: not-allowed !important;
}

/* ── Shot info badge ── */
.shot-badge {
  display: inline-block;
  padding: 4px 12px;
  border-radius: 20px;
  font-size: 0.78rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}
.shot-badge.rim  { background: #FF6B35; color: #fff; }
.shot-badge.j2   { background: #FFD700; color: #1a1a2e; }
.shot-badge.j3   { background: #00D4FF; color: #1a1a2e; }

/* ── Results panel ── */
.result-section {
  padding: 14px 16px;
  margin-bottom: 10px;
  border-radius: 10px;
  background: #12122a;
  border: 1px solid #2a2a4a;
}

.result-label {
  font-size: 0.72rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #808098;
  margin-bottom: 4px;
}

.result-value {
  font-size: 1.6rem;
  font-weight: 700;
  color: #e0e0e0;
  font-variant-numeric: tabular-nums;
}

.result-ci {
  font-size: 0.82rem;
  color: #808098;
  font-variant-numeric: tabular-nums;
}

/* ── Actor breakdown ── */
.actor-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 14px;
  margin-bottom: 6px;
  border-radius: 8px;
  background: #16162e;
  border-left: 3px solid #2a2a4a;
}

.actor-row.positive { border-left-color: #4CAF50; }
.actor-row.negative { border-left-color: #F44336; }
.actor-row.neutral  { border-left-color: #808098; }

.actor-name {
  font-size: 0.82rem;
  font-weight: 600;
  color: #c0c0d0;
}

.actor-effect {
  font-size: 1rem;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}

.actor-effect.positive { color: #4CAF50; }
.actor-effect.negative { color: #F44336; }
.actor-effect.neutral  { color: #808098; }

.actor-ci {
  font-size: 0.72rem;
  color: #606078;
  font-variant-numeric: tabular-nums;
}

/* ── Headshots ── */
.headshot-container {
  text-align: center;
  padding: 8px;
}

.headshot-img {
  width: 90px;
  height: 66px;
  object-fit: cover;
  object-position: top;
  border-radius: 8px;
  border: 2px solid #2a2a4a;
  background: #12122a;
}

.headshot-name {
  font-size: 0.72rem;
  color: #a0a0c0;
  margin-top: 4px;
  font-weight: 500;
}

/* ── Prompt text ── */
.prompt-text {
  text-align: center;
  color: #606078;
  font-size: 0.95rem;
  padding: 40px 20px;
  font-style: italic;
}

/* ── Title ── */
.app-title {
  font-size: 1.4rem;
  font-weight: 800;
  color: #FFD700;
  letter-spacing: 0.04em;
}

.app-subtitle {
  font-size: 0.78rem;
  color: #606078;
  letter-spacing: 0.04em;
}

/* ── Toggle grid ── */
.toggle-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 2px 14px;
}

.toggle-grid .form-check {
  margin-bottom: 0 !important;
  padding-top: 2px;
  padding-bottom: 2px;
}

/* Slider styling */
.irs--shiny .irs-bar { background: #FFD700; }
.irs--shiny .irs-handle { border-color: #FFD700; }
.irs--shiny .irs-single { background: #FFD700; color: #1a1a2e; }
.irs--shiny .irs-min, .irs--shiny .irs-max { color: #606078; }
.irs--shiny .irs-line { background: #2a2a4a; }
.irs--shiny .irs-grid-text { color: #606078; }

/* ── Shot grade ── */
.grade-circle {
  width: 68px;
  height: 68px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 1.5rem;
  font-weight: 800;
  margin: 0 auto 4px auto;
  border: 3px solid;
}

.grade-number {
  font-size: 1.1rem;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}

.grade-sublabel {
  font-size: 0.68rem;
  color: #808098;
  text-align: center;
}
"

## ═══════════════════════════════════════════════════════════════════════════════
## UI ==========================================================================
## ═══════════════════════════════════════════════════════════════════════════════

ui <- page_fluid(
  
  useShinyjs(),
  tags$head(tags$style(HTML(app_css))),
  
  theme = bs_theme(
    version = 5,
    bg = "#0f0f23",
    fg = "#e0e0e0",
    primary = "#FFD700",
    secondary = "#2a2a4a",
    font_scale = 0.95
  ),
  
  # Header
  div(style = "padding: 16px 20px 8px 20px;",
    span(class = "app-title", "Expected Points Above Average"),
    span(class = "app-subtitle", style = "margin-left: 12px;",
         "Interactive Dashbaord")
  ),
  
  div(style = "display: flex; gap: 20px; padding: 0 12px;",
    
    # ── LEFT: Players + Court (5/12) ─────────────────────────────────────
    div(style = "flex: 5; display: flex; flex-direction: column; gap: 12px; min-width: 0;",
      
      # Player selection (above court)
      card(
        card_header("Players & Team"),
        card_body(
          layout_columns(
            col_widths = c(5, 5, 2),
            div(
              selectizeInput("shooter", "Shooter", choices = NULL,
                             options = list(placeholder = "Type a name...",
                                            maxOptions = 10)),
              uiOutput("shooter_headshot")
            ),
            div(
              selectizeInput("defender", "Primary Defender", choices = NULL,
                             options = list(placeholder = "Type a name...",
                                            maxOptions = 10)),
              uiOutput("defender_headshot")
            ),
            div(
              tags$label("Team", class = "control-label"),
              uiOutput("defteam_display"),
              div(style = "height: 76px;")
            )
          )
        )
      ),
      
      # Court (fills remaining left-column space)
      card(style = "flex: 1;",
        card_header("Click to Set Shot Location"),
        card_body(
          plotOutput("court_plot", click = "court_click",
                     height = "520px", width = "100%"),
          uiOutput("shot_info_ui")
        )
      )
    ),
    
    # ── RIGHT: Context + Results (7/12) ──────────────────────────────────
    div(style = "flex: 7; display: flex; flex-direction: column; gap: 12px; min-width: 0;",
      
      # Context features
      card(
        card_header("Shot Quality Features"),
        card_body(
          layout_columns(
            col_widths = c(4, 4, 4),
            sliderInput("shot_clock", "Shot Clock (sec)", 
                        min = 0, max = 24, value = 14, step = 1),
            selectInput("dribble_range", "Dribbles",
                        choices = names(DRIBBLE_LEVELS),
                        selected = "0 Dribbles"),
            selectInput("defender_dist", "Defender Distance",
                        choices = names(DEFENDER_DIST_LEVELS),
                        selected = "4-6 Feet - Open")
          ),
          layout_columns(
            col_widths = c(4, 4, 4),
            sliderInput("min_game", "Game Min. Played",
                        min = 0, max = 48, value = 18, step = 1),
            sliderInput("min_stint", "Stint Min. Played",
                        min = 0, max = 20, value = 5, step = 1),
            div(style = "padding-top: 10px;",
              div(class = "toggle-grid",
                checkboxInput("is_fastbreak", "Fast Break", value = FALSE),
                checkboxInput("is_fromturnover", "Off Turnover", value = FALSE),
                checkboxInput("is_2ndchance", "2nd Chance", value = FALSE),
                checkboxInput("is_dunk", "Dunk", value = FALSE)
              )
            )
          )
        )
      ),
      
      # Results (fills remaining right-column space)
      card(style = "flex: 1;",
        card_header("EPAA Prediction with 90% Credible Intervals"),
        card_body(
          uiOutput("results_ui")
        )
      )
    )
  )
)


## ═══════════════════════════════════════════════════════════════════════════════
## SERVER ======================================================================
## ═══════════════════════════════════════════════════════════════════════════════

server <- function(input, output, session) {
  
  # Server-side selectize for player search (shows first 10 matches)
  updateSelectizeInput(session, "shooter",
                       choices = c("Select shooter" = "", player_choices),
                       server = TRUE)
  updateSelectizeInput(session, "defender",
                       choices = c("Select defender" = "", defender_choices),
                       server = TRUE)
  
  # Start with dunk disabled (no shot selected yet)
  shinyjs::disable("is_dunk")
  
  # ── Reactive: shot location from click ─────────────────────────────────
  shot_loc <- reactiveValues(x = NULL, y = NULL, family = NULL,
                              type = NULL, distance = NULL, angle = NULL)
  
  observeEvent(input$court_click, {
    click <- input$court_click
    if (is.null(click)) return()
    
    x <- max(-25, min(25, click$x))
    y <- max(-5.25, min(41.75, click$y))
    
    info <- classify_shot(x, y)
    
    shot_loc$x <- x
    shot_loc$y <- y
    shot_loc$family   <- info$family
    shot_loc$type     <- info$type
    shot_loc$distance <- info$dist
    shot_loc$angle    <- abs(atan2(x, y)) * 180 / pi
  })
  
  # ── Input constraints ──────────────────────────────────────────────────
  
  # 1. Stint min ≤ Game min (hard cap)
  observeEvent(input$min_game, {
    game_val <- input$min_game
    stint_max <- min(20, game_val)
    stint_val <- min(input$min_stint, stint_max)
    updateSliderInput(session, "min_stint",
                      max = stint_max, value = stint_val)
  })
  
  # 2. Dunk: only available for rim shots
  observe({
    fam <- shot_loc$family
    if (is.null(fam) || fam != "rim") {
      updateCheckboxInput(session, "is_dunk", value = FALSE)
      shinyjs::disable("is_dunk")
    } else {
      shinyjs::enable("is_dunk")
    }
  })
  
  # 3. Fast break ↔ Shot clock coupling
  #    - Fast break requires shot clock ≥ 16
  #    - If shot clock < 16, fast break is disabled and unchecked
  #    - If fast break is checked, shot clock is forced to ≥ 16
  observe({
    sc <- input$shot_clock
    if (sc < 16) {
      updateCheckboxInput(session, "is_fastbreak", value = FALSE)
      shinyjs::disable("is_fastbreak")
    } else {
      shinyjs::enable("is_fastbreak")
    }
  })
  
  observeEvent(input$is_fastbreak, {
    if (input$is_fastbreak && input$shot_clock < 16) {
      updateSliderInput(session, "shot_clock", value = 16)
    }
  })
  
  # ── Court plot ─────────────────────────────────────────────────────────
  output$court_plot <- renderPlot({
    draw_court(shot_loc$x, shot_loc$y, shot_loc$family)
  }, bg = "#1a1a2e")
  
  # ── Shot info display ──────────────────────────────────────────────────
  output$shot_info_ui <- renderUI({
    if (is.null(shot_loc$x)) {
      return(div(class = "prompt-text", "Click anywhere on the court above"))
    }
    
    badge_class <- paste("shot-badge", shot_loc$family)
    family_label <- switch(shot_loc$family,
                           rim = "Rim", j2 = "Mid-Range", j3 = "Three-Pointer")
    
    div(style = "display: flex; align-items: center; gap: 16px; padding: 8px 4px;",
      span(class = badge_class, family_label),
      span(style = "color: #a0a0c0; font-size: 0.88rem;",
           sprintf("%.1f ft  |  %.0f\u00B0 from center", 
                   shot_loc$distance, shot_loc$angle))
      # ,
      # if (shot_loc$family == "j3")
      #   span(style = "color: #a0a0c0; font-size: 0.82rem;", "(3 pts)")
      # else
      #   span(style = "color: #a0a0c0; font-size: 0.82rem;", "(2 pts)")
    )
  })
  
  # ── Headshots ──────────────────────────────────────────────────────────
  output$shooter_headshot <- renderUI({
    req(input$shooter)
    if (input$shooter == "") return(NULL)
    pid <- player_id_lookup[as.character(input$shooter)]
    url <- get_headshot_url(pid)
    if (is.null(url)) return(NULL)
    
    name <- player_map$player_name_full[player_map$player_idx == as.integer(input$shooter)]
    
    div(class = "headshot-container",
      tags$img(src = url, class = "headshot-img",
               onerror = "this.style.display='none'"),
      div(class = "headshot-name", name)
    )
  })
  
  output$defender_headshot <- renderUI({
    req(input$defender)
    if (input$defender == "") return(NULL)
    did <- defender_id_lookup[as.character(input$defender)]
    url <- get_headshot_url(did)
    if (is.null(url)) return(NULL)
    
    name <- defender_map$defender_name[defender_map$defender_idx == as.integer(input$defender)]
    
    div(class = "headshot-container",
      tags$img(src = url, class = "headshot-img",
               onerror = "this.style.display='none'"),
      div(class = "headshot-name", name)
    )
  })
  
  # ── Auto-set defteam when defender is selected ─────────────────────────
  defteam_idx <- reactiveVal(NULL)
  
  observeEvent(input$defender, {
    if (is.null(input$defender) || input$defender == "") {
      defteam_idx(NULL)
    } else {
      team <- def_team_link[as.character(input$defender)]
      defteam_idx(if (!is.null(team) && !is.na(team)) as.integer(team) else NULL)
    }
  })
  
  output$defteam_display <- renderUI({
    idx <- defteam_idx()
    if (is.null(idx)) {
      div(style = "padding: 8px 12px; background: #12122a; border: 1px solid #2a2a4a;
                   border-radius: 8px; color: #606078; font-size: 0.88rem; margin-top: 2px;",
          "\u2014")
    } else {
      abbr <- defteam_map$defender_team[defteam_map$defteam_idx == idx]
      div(style = "padding: 8px 12px; background: #12122a; border: 1px solid #FFD700;
                   border-radius: 8px; color: #FFD700; font-size: 0.88rem;
                   font-weight: 700; margin-top: 2px; letter-spacing: 0.06em;",
          abbr)
    }
  })
  
  # ── XGBoost prediction ────────────────────────────────────────────────
  xfg_baseline <- reactive({
    req(shot_loc$x)
    
    fam <- shot_loc$family
    t   <- shot_loc$type
    
    feats <- data.frame(
      shot_distance      = shot_loc$distance,
      shot_angle         = shot_loc$angle,
      player_min_game    = as.numeric(input$min_game),
      player_min_stint   = as.numeric(input$min_stint),
      shot_clock         = as.numeric(input$shot_clock),
      defender_dist_level = DEFENDER_DIST_LEVELS[input$defender_dist],
      dribble_level      = DRIBBLE_LEVELS[input$dribble_range],
      is_2ndchance       = as.integer(input$is_2ndchance),
      is_fastbreak       = as.integer(input$is_fastbreak),
      is_fromturnover    = as.integer(input$is_fromturnover)
    )
    
    if (fam == "rim") {
      feats$is_dunk <- as.integer(input$is_dunk)
      feat_order <- c("shot_distance", "shot_angle", "player_min_game",
                       "player_min_stint", "shot_clock",
                       "defender_dist_level", "dribble_level",
                       "is_2ndchance", "is_fastbreak", "is_fromturnover",
                       "is_dunk")
    } else {
      feat_order <- c("shot_distance", "shot_angle", "player_min_game",
                       "player_min_stint", "shot_clock",
                       "defender_dist_level", "dribble_level",
                       "is_2ndchance", "is_fastbreak", "is_fromturnover")
    }
    
    X <- as.matrix(feats[, feat_order, drop = FALSE])
    d <- xgb.DMatrix(data = X)
    xfg <- predict(xgb_models[[fam]], d)
    
    list(xfg = xfg, xfg_logit = qlogis(xfg), type = t, family = fam)
  })
  
  # ── Full posterior computation ─────────────────────────────────────────
  posterior_results <- reactive({
    base <- xfg_baseline()
    req(base)
    
    t    <- base$type
    xfg  <- base$xfg
    xfgl <- base$xfg_logit
    
    point_value <- ifelse(t == 3, 3, 2)
    
    cal_int   <- cal_draws[, t]
    cal_slope <- cal_draws[, t + 3]
    
    eta_base <- cal_int + cal_slope * xfgl
    p_base   <- inv_logit(eta_base)
    
    # Random effects
    has_shooter  <- !is.null(input$shooter)  && input$shooter  != ""
    has_defender <- !is.null(input$defender)  && input$defender != ""
    dt_idx       <- defteam_idx()
    has_defteam  <- !is.null(dt_idx)
    
    shooter_eff  <- rep(0, n_draws)
    defender_eff <- rep(0, n_draws)
    defteam_eff  <- rep(0, n_draws)
    
    if (has_shooter) {
      idx <- as.integer(input$shooter)
      shooter_eff <- player_draws[[t]][, idx]
    }
    
    if (has_defender) {
      idx <- as.integer(input$defender)
      defender_eff <- defender_draws[[t]][, idx]
    }
    
    if (has_defteam) {
      defteam_eff <- defteam_draws[[t]][, dt_idx]
    }
    
    # Full eta
    eta_full <- eta_base + shooter_eff + defender_eff + defteam_eff
    p_full   <- inv_logit(eta_full)
    
    # Isolated impacts (percentage point change from baseline)
    shooter_pp  <- (inv_logit(eta_base + shooter_eff)  - p_base) * 100
    defender_pp <- (inv_logit(eta_base + defender_eff)  - p_base) * 100
    defteam_pp  <- (inv_logit(eta_base + defteam_eff)   - p_base) * 100
    
    # Shot grade: percentile of xPts vs league distribution
    xpts_med <- median(p_full) * point_value
    shot_pctile <- round(xpts_ecdf(xpts_med) * 100)
    
    list(
      xfg_raw     = xfg,
      xfg_pct     = xfg * 100,
      point_value = point_value,
      
      baseline_median = median(p_base) * 100,
      baseline_q05    = quantile(p_base, 0.05) * 100,
      baseline_q95    = quantile(p_base, 0.95) * 100,
      
      final_median = median(p_full) * 100,
      final_q05    = quantile(p_full, 0.05) * 100,
      final_q95    = quantile(p_full, 0.95) * 100,
      
      xpts_median  = median(p_full) * point_value,
      xpts_q05     = quantile(p_full, 0.05) * point_value,
      xpts_q95     = quantile(p_full, 0.95) * point_value,
      
      shot_grade   = shot_pctile,
      letter_grade = get_letter_grade(shot_pctile),
      grade_color  = get_grade_color(shot_pctile),
      
      shooter_median  = median(shooter_pp),
      shooter_q05     = quantile(shooter_pp, 0.05),
      shooter_q95     = quantile(shooter_pp, 0.95),
      
      defender_median = median(defender_pp),
      defender_q05    = quantile(defender_pp, 0.05),
      defender_q95    = quantile(defender_pp, 0.95),
      
      defteam_median  = median(defteam_pp),
      defteam_q05     = quantile(defteam_pp, 0.05),
      defteam_q95     = quantile(defteam_pp, 0.95),
      
      has_shooter  = has_shooter,
      has_defender  = has_defender,
      has_defteam   = has_defteam
    )
  })
  
  # ── Results UI ─────────────────────────────────────────────────────────
  output$results_ui <- renderUI({
    if (is.null(shot_loc$x)) {
      return(div(class = "prompt-text",
                 "Select a shot location and players to see predictions"))
    }
    
    res <- posterior_results()
    
    fmt_effect <- function(val) {
      if (val >= 0) sprintf("+%.1f%%", val) else sprintf("%.1f%%", val)
    }
    
    fmt_ci <- function(lo, hi) {
      sprintf("[ %s, %s ]", 
              ifelse(lo >= 0, sprintf("+%.1f", lo), sprintf("%.1f", lo)),
              ifelse(hi >= 0, sprintf("+%.1f", hi), sprintf("%.1f", hi)))
    }
    
    effect_class <- function(val) {
      if (abs(val) < 0.3) "neutral" else if (val > 0) "positive" else "negative"
    }
    
    # Build actor rows
    actor_rows <- list()
    
    # Baseline
    actor_rows[[length(actor_rows) + 1]] <- div(class = "actor-row neutral",
                                                div(div(class = "actor-name", 
                                                        "xFG"),
                                                    div(class = "actor-ci",
                                                        sprintf("League-Average Shot Quality"))),
                                                div(style = "text-align: right;",
                                                    div(class = "actor-effect neutral",
                                                        sprintf("%.1f%%", res$baseline_median)),
                                                    div(class = "actor-ci",
                                                        sprintf("[ %.1f%%, %.1f%% ]", 
                                                                res$baseline_q05, res$baseline_q95))))
    
    # Shooter
    if (res$has_shooter) {
      sname <- player_map$player_name_full[player_map$player_idx == as.integer(input$shooter)]
      cls <- effect_class(res$shooter_median)
      actor_rows[[length(actor_rows) + 1]] <- div(class = paste("actor-row", cls),
                                                  div(div(class = "actor-name", 
                                                          paste0("\U1F3C0 Shooter: ", sname))),
                                                  div(style = "text-align: right;",
                                                      div(class = paste("actor-effect", cls),
                                                          fmt_effect(res$shooter_median),
                                                          div(class = "actor-ci", 
                                                              fmt_ci(res$shooter_q05, res$shooter_q95)))))
    }
    
    # Defender
    if (res$has_defender) {
      dname <- defender_map$defender_name[defender_map$defender_idx == as.integer(input$defender)]
      cls <- effect_class(res$defender_median)
      actor_rows[[length(actor_rows) + 1]] <- div(class = paste("actor-row", cls),
                                                  div(div(class = "actor-name", 
                                                          paste0("\U1F6E1\UFE0F Defender: ", dname))),
                                                  div(style = "text-align: right;",
                                                      div(class = paste("actor-effect", cls),
                                                          fmt_effect(res$defender_median),
                                                          div(class = "actor-ci", 
                                                              fmt_ci(res$defender_q05, res$defender_q95)))))
    }
    
    # Defteam
    if (res$has_defteam) {
      dt_idx <- defteam_idx()
      tname <- defteam_map$defender_team[defteam_map$defteam_idx == dt_idx]
      cls <- effect_class(res$defteam_median)
      actor_rows[[length(actor_rows) + 1]] <- div(class = paste("actor-row", cls),
                                                  div(div(class = "actor-name", 
                                                          paste0("\U1F3E2 Team Defense: ", tname))),
                                                  div(style = "text-align: right;",
                                                      div(class = paste("actor-effect", cls),
                                                          fmt_effect(res$defteam_median)),
                                                      div(class = "actor-ci", 
                                                          fmt_ci(res$defteam_q05, res$defteam_q95))))
    }
    
    # Assemble full results
    tagList(
      # Hero section: xFG%, xPts, Shot Grade
      div(class = "result-section",
          style = "border: 1px solid #FFD700; background: #16162e;",
        layout_columns(
          col_widths = c(4, 4, 4),
          # Adjusted xFG%
          div(
            div(class = "result-label", "Adjusted xFG%"),
            div(class = "result-value",
                style = "color: #FFD700;",
                sprintf("%.1f%%", res$final_median)),
            div(class = "result-ci",
                sprintf("[ %.1f%%, %.1f%% ]",
                        res$final_q05, res$final_q95))
          ),
          # Expected Points per Shot
          div(
            div(class = "result-label", "Expected Points"),
            div(class = "result-value",
                style = "color: #00D4FF;",
                sprintf("%.2f", res$xpts_median)),
            div(class = "result-ci",
                sprintf("[ %.2f, %.2f ]",
                        res$xpts_q05, res$xpts_q95))
          ),
          # Shot Grade
          div(style = "text-align: center;",
            div(class = "result-label", "Shot Grade"),
            div(class = "grade-circle",
                style = sprintf("color: %s; border-color: %s;",
                                res$grade_color, res$grade_color),
                res$letter_grade),
            div(class = "grade-number",
                style = sprintf("color: %s;", res$grade_color),
                sprintf("%d", res$shot_grade)),
            div(class = "grade-sublabel", "percentile")
          )
        )
      ),
      
      # Actor breakdown
      div(style = "margin-top: 12px;",
        div(class = "result-label", style = "margin-bottom: 8px; padding-left: 4px;",
            "Effect Breakdown"),
        tagList(actor_rows)
      )
    )
  })
}


## ═══════════════════════════════════════════════════════════════════════════════
## RUN =========================================================================
## ═══════════════════════════════════════════════════════════════════════════════

shinyApp(ui, server)
