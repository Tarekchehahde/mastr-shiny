# =============================================================================
# ui_helpers.R — shared bslib UI pieces used by every dashboard
# =============================================================================

suppressPackageStartupMessages({
  library(bslib)
  library(shiny)
  library(htmltools)
})

# Transtek-friendly palette (can be overridden per app).
MASTR_PALETTE <- list(
  primary = "#0B5ED7",
  accent  = "#10b981",
  warn    = "#f59e0b",
  danger  = "#ef4444",
  solar   = "#F59E0B",
  wind    = "#0EA5E9",
  biomass = "#65A30D",
  water   = "#06B6D4",
  geo     = "#B45309",
  nuclear = "#A855F7",
  fossil  = "#6B7280",
  storage = "#111827"
)

mastr_theme <- function(primary = MASTR_PALETTE$primary) {
  bslib::bs_theme(
    version = 5,
    bootswatch = "flatly",
    primary = primary,
    base_font = bslib::font_google("Inter"),
    heading_font = bslib::font_google("Inter"),
    "font-size-base" = "0.95rem"
  )
}

mastr_page <- function(title, subtitle = NULL, ...,
                       primary = MASTR_PALETTE$primary) {
  bslib::page_fillable(
    title = title,
    theme = mastr_theme(primary),
    tags$style(HTML("
      .mastr-footer { font-size: 0.75rem; color: #6b7280; padding: 0.5rem 0; }
      .mastr-kpi { font-variant-numeric: tabular-nums; }
      .value-box { min-height: 130px; }
      .value-box .value-box-title { font-size: 0.85rem; opacity: 0.9;
                                    white-space: nowrap; overflow: hidden;
                                    text-overflow: ellipsis; }
      .value-box .value-box-value { font-size: 1.75rem; line-height: 1.1;
                                    white-space: nowrap; overflow: hidden;
                                    text-overflow: ellipsis; }
      .value-box h3 { font-variant-numeric: tabular-nums; }
    ")),
    div(class = "py-2",
        h2(title, class = "mb-0"),
        if (!is.null(subtitle)) p(subtitle, class = "text-muted mb-0")),
    ...,
    mastr_footer()
  )
}

mastr_footer <- function() {
  div(class = "mastr-footer text-center border-top mt-3 pt-2",
      HTML(mastr_attribution()),
      " · ",
      tags$a(href = "https://www.marktstammdatenregister.de/MaStR/Datendownload",
             target = "_blank", "BNetzA MaStR"),
      " · ",
      tags$a(href = "https://github.com/Tarekchehahde/mastr-shiny",
             target = "_blank", "Source"))
}

# Shorthand value_box with tabular-numeric formatting.
#
# A fixed min_height is required because page_fillable() otherwise collapses
# the KPI row when the plots below request more height, which clips the title
# and caption lines inside the value box.
mastr_kpi <- function(title, value, subtitle = NULL,
                      color = "primary", icon = NULL,
                      min_height = "130px") {
  bslib::value_box(
    title = title,
    value = span(class = "mastr-kpi", value),
    subtitle,
    theme = color,
    showcase = icon,
    min_height = min_height,
    fill = FALSE
  )
}

# Format helpers
fmt_num <- function(x, digits = 0, suffix = "") {
  if (is.null(x) || length(x) == 0 || is.na(x)) return("–")
  paste0(formatC(x, big.mark = ".", decimal.mark = ",", format = "f", digits = digits),
         suffix)
}

fmt_mw <- function(kw) fmt_num(kw / 1000, 1, " MW")
fmt_gw <- function(kw) fmt_num(kw / 1e6, 2, " GW")
