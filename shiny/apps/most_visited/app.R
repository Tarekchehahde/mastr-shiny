# =============================================================================
# most_visited :: flagship in-house R Shiny replica of the Tableau panel that
# Candida's team posts to the dashboard feed each month.
#
# Source Tableau panel:
#   "Aktuelle Zubauleistung für <Monat> in DE pro Segment.
#    Monate im Vergleich zu den Vorjahren über alle Segmente.
#    Segmente enthalten Anlagen wie folgt:
#       <10 kW = Home, <1 MW = C&I, Rest Large Scale."
#
# Parity goals (match what Candida generates):
#   1. 4 small multiples (Home / C&I / Large Scale / Grand Total) of monthly
#      new-capacity (DC/Brutto MW), one colored line per year 2022..current.
#   2. Year-to-date columns highlighted with a light orange band.
#   3. Side table "IBN Differenz der Vorjahre - Total | Brutto/DC-Leistung MW"
#      — YTD months x last 6 years, with absolute Wert and % Abw. zu Vorjahr.
#
# Data differences from Tableau:
#   - Candida re-buckets Einheiten after BNetzA's size classes; we do the
#     same Home/C&I/Large-Scale split on Bruttoleistung (kW) at query time.
#   - We read the raw solar parquet through DuckDB httpfs, so the numbers
#     come from the SAME BNetzA MaStR source, one night newer than Tableau.
# =============================================================================

suppressPackageStartupMessages({
  library(shiny); library(bslib); library(plotly); library(reactable)
  library(dplyr); library(tidyr)
})
source("../../R/mastr_data.R")
source("../../R/ui_helpers.R")
source("../../R/tableau_helpers.R")

YEAR_NOW  <- as.integer(format(Sys.Date(), "%Y"))
MONTH_NOW <- as.integer(format(Sys.Date(), "%m"))

# Color palette aligned with the Candida screenshot:
# light blue (oldest) -> dark grey -> orange (current year).
SEGMENT_YEAR_COLORS <- function(years) {
  n <- length(years)
  pal <- c("#9ec3dc","#7b9db7","#5e7a8e","#44566a","#f97316")
  if (n <= length(pal)) tail(pal, n) else c(rep(pal[1], n - length(pal) + 1), pal[-1])
}

ui <- mastr_page(
  title = "Most Visited \u2014 Zubauleistung pro Segment (R Shiny-Nachbau)",
  subtitle = sprintf(
    "Aktuelle Zubauleistung f\u00fcr %s in DE pro Segment. Monate im Vergleich zu den Vorjahren \u00fcber alle Segmente. Segmente enthalten Anlagen wie folgt: <10 kW = Home, <1 MW = C&I, Rest Large Scale.",
    MONTHS_DE[MONTH_NOW]),
  fluid = TRUE,

  tableau_parity_banner("Aktuelle Zubauleistung pro Segment (Candida)"),

  layout_sidebar(
    sidebar = sidebar(
      title = "Einstellungen", width = 280,
      sliderInput("yr_from", "Vergleichs-Startjahr",
                  min = 2015, max = YEAR_NOW - 1,
                  value = max(2022, YEAR_NOW - 4),
                  sep = "", step = 1, ticks = FALSE),
      sliderInput("ytd_m",  "YTD bis Monat",
                  min = 1, max = 12, value = MONTH_NOW,
                  step = 1, ticks = FALSE),
      radioButtons("metric", "Metrik",
                   choices = c("Brutto/DC-Leistung MW" = "brutto",
                               "Nettonennleistung MW"  = "netto"),
                   selected = "brutto"),
      checkboxInput("only_active", "Nur aktive Einheiten", value = FALSE),
      tags$hr(),
      tags$small(class = "text-muted",
        "Daten live aus dem neuesten GitHub-Release (", code("runGitHub"),
        "). Quelle BNetzA MaStR.")
    ),

    layout_column_wrap(
      width = 1/2, heights_equal = "row",
      card(full_screen = TRUE,
           card_header("MaStR \u2014 monatlicher Zubau pro Segment"),
           plotlyOutput("plot_segments", height = "620px")),
      card(full_screen = TRUE,
           card_header(sprintf(
             "IBN Differenz der Vorjahre \u2014 Total | %s",
             "Brutto/DC-Leistung MW")),
           reactableOutput("tbl_diff", height = "auto"))
    )
  )
)

server <- function(input, output, session) {

  data_monthly <- reactive({
    metric_col <- if (input$metric == "netto") "Nettonennleistung"
                  else                          "Bruttoleistung"
    active_filter <- if (input$only_active)
      "AND EinheitBetriebsstatus = 'InBetrieb'" else ""
    sql <- sprintf("
      SELECT
        %s AS year,
        %s AS month,
        %s AS segment,
        SUM(%s) / 1000.0 AS mw,
        COUNT(*)         AS units
      FROM solar
      WHERE Inbetriebnahmedatum IS NOT NULL
        AND %s IS NOT NULL
        AND %s >= %d
        AND %s <= %d
        %s
      GROUP BY 1, 2, 3
      ORDER BY 1, 2, 3",
      sql_ibn_year("Inbetriebnahmedatum"),
      sql_ibn_month("Inbetriebnahmedatum"),
      sql_segment_3("Bruttoleistung"),
      metric_col,
      metric_col,
      sql_ibn_year("Inbetriebnahmedatum"),
      input$yr_from,
      sql_ibn_year("Inbetriebnahmedatum"),
      YEAR_NOW,
      active_filter)
    mastr_query(sql)
  })

  # ---- small multiples (Home / C&I / Large Scale / Grand Total) -------------
  output$plot_segments <- renderPlotly({
    d <- data_monthly()
    if (!nrow(d)) return(plotly_empty(type = "scatter", mode = "lines"))
    d$year    <- as.integer(d$year)
    d$month   <- as.integer(d$month)
    d$segment <- factor(d$segment, levels = c("Home","C&I","Large Scale"))

    total <- d |>
      group_by(year, month) |>
      summarise(mw = sum(mw, na.rm = TRUE), .groups = "drop") |>
      mutate(segment = factor("Grand Total",
                              levels = c("Home","C&I","Large Scale","Grand Total")))

    dat <- bind_rows(d |> mutate(segment = factor(as.character(segment),
                                                  levels = levels(total$segment))),
                     total)

    years <- sort(unique(dat$year))
    cols  <- setNames(SEGMENT_YEAR_COLORS(years), as.character(years))

    segments <- levels(dat$segment)
    plots <- lapply(segments, function(seg) {
      dd <- dat |> filter(segment == seg)
      p  <- plot_ly(height = 150)
      for (y in years) {
        dy <- dd |> filter(year == y) |> arrange(month)
        p <- add_trace(p, data = dy,
                       x = ~month, y = ~mw,
                       name = as.character(y),
                       legendgroup = as.character(y),
                       showlegend = (seg == segments[1]),
                       type = "scatter", mode = "lines+markers",
                       line = list(color = cols[[as.character(y)]],
                                   width = if (y == YEAR_NOW) 3 else 1.5),
                       marker = list(color = cols[[as.character(y)]],
                                     size = if (y == YEAR_NOW) 6 else 4))
      }
      p |> layout(
        annotations = list(list(
          xref = "paper", yref = "paper", x = 0.02, y = 0.95,
          text = paste0("<b>", seg, "</b>"),
          showarrow = FALSE, align = "left", font = list(size = 13))),
        shapes = list(list(
          type = "rect", xref = "x", yref = "paper",
          x0 = 0.5, x1 = input$ytd_m + 0.5, y0 = 0, y1 = 1,
          fillcolor = "#fde68a", opacity = 0.25, line = list(width = 0))),
        xaxis = list(
          title = "", tickmode = "array", tickvals = 1:12,
          ticktext = substr(MONTHS_DE, 1, 3),
          tickangle = 0),
        yaxis = list(title = "", automargin = TRUE),
        margin = list(t = 20, r = 10, b = 30, l = 40)
      )
    })

    subplot(plots, nrows = 4, shareX = TRUE, titleY = FALSE,
            margin = 0.02) |>
      layout(legend = list(orientation = "h", y = 1.05, x = 0.5,
                           xanchor = "center",
                           title = list(text = ""))) |>
      config(displaylogo = FALSE,
             modeBarButtonsToRemove = c("lasso2d","select2d","autoScale2d"))
  })

  # ---- IBN Differenz der Vorjahre - Total (table) ---------------------------
  table_diff <- reactive({
    d <- data_monthly()
    if (!nrow(d)) return(NULL)

    ytd_m <- input$ytd_m
    total <- d |>
      group_by(year, month) |>
      summarise(mw = sum(mw, na.rm = TRUE), .groups = "drop") |>
      filter(month <= ytd_m)

    years <- sort(unique(total$year))
    wide <- total |>
      mutate(MonthName = factor(MONTHS_DE[month],
                                levels = MONTHS_DE)) |>
      select(MonthName, year, mw) |>
      tidyr::pivot_wider(names_from = year, values_from = mw, values_fill = 0)

    out <- wide |> arrange(MonthName)
    yr_cols <- as.character(years)
    keep <- c("MonthName", yr_cols)
    out <- out[, keep, drop = FALSE]

    # For each year column, add an "abw. zu Vorjahr" column as fraction.
    for (i in seq_along(yr_cols)) {
      y  <- yr_cols[i]
      if (i == 1) next
      prev <- yr_cols[i - 1]
      out[[paste0("\u0394 ", y)]] <- (out[[y]] - out[[prev]]) / out[[prev]]
    }
    out
  })

  output$tbl_diff <- renderReactable({
    d <- table_diff()
    if (is.null(d) || !nrow(d)) return(reactable(data.frame()))

    val_cols <- grep("^\\d{4}$", names(d), value = TRUE)
    dlt_cols <- grep("^\u0394", names(d), value = TRUE)

    cdefs <- c(
      list(MonthName = reactable::colDef(name = "Monat", minWidth = 110, sticky = "left")),
      setNames(lapply(val_cols, function(y) reactable::colDef(
        name = y, align = "right",
        format = reactable::colFormat(separators = TRUE, digits = 0))),
        val_cols),
      setNames(lapply(dlt_cols, function(y) reactable::colDef(
        name = gsub("^\u0394 ", "\u0394 ", y), align = "right",
        style = function(value) {
          if (is.na(value) || !is.numeric(value)) return(NULL)
          if (value >= 0) list(color = "#15803d", fontWeight = 500)
          else             list(color = "#b91c1c", fontWeight = 500)
        },
        format = reactable::colFormat(percent = TRUE, digits = 2,
                                      separators = TRUE))),
        dlt_cols)
    )

    reactable(d,
              columns = cdefs,
              compact = TRUE, striped = TRUE, highlight = TRUE,
              defaultPageSize = 12, minRows = 1,
              bordered = FALSE,
              theme = reactableTheme(
                headerStyle = list(fontWeight = 600)))
  })
}

shinyApp(ui, server)
