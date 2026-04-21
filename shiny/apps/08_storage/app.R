# 08_storage :: Stromspeicher (Batterien + Pumpspeicher)

suppressPackageStartupMessages({
  library(shiny); library(bslib); library(plotly); library(reactable)
})
source("../../R/mastr_data.R"); source("../../R/ui_helpers.R")

ui <- mastr_page(
  title = "Stromspeicher",
  subtitle = "Batteriespeicher- und Pumpspeicher-Einheiten, Leistung & Kapazität.",

  layout_column_wrap(1/4,
    uiOutput("kpi_n"), uiOutput("kpi_mw"), uiOutput("kpi_mwh"), uiOutput("kpi_crate")),

  layout_column_wrap(1/2, heights_equal = "row",
    card(card_header("Leistung vs. Nutzbare Speicherkapazität"),
         plotlyOutput("plot_scatter", height = "400px")),
    card(card_header("Zubau-Historie"),
         plotlyOutput("plot_buildout", height = "400px"))
  ),

  card(card_header("Top-Einheiten nach Speicherkapazität"),
       reactableOutput("table_top"))
)

server <- function(input, output, session) {
  df <- reactive(mastr_query("
    SELECT EinheitMastrNummer AS mastr_nr,
           TRY_CAST(Bruttoleistung AS DOUBLE) AS kw,
           TRY_CAST(NutzbareSpeicherkapazitaet AS DOUBLE) AS kwh,
           TRY_CAST(Inbetriebnahmedatum AS DATE) AS inbetrieb,
           Technologie, Bundesland
    FROM stromspeicher"))

  output$kpi_n   <- renderUI(mastr_kpi("Einheiten", fmt_num(nrow(df()))))
  output$kpi_mw  <- renderUI(mastr_kpi("Leistung", fmt_num(sum(df()$kw,  na.rm=TRUE)/1000, 0, " MW")))
  output$kpi_mwh <- renderUI(mastr_kpi("Kapazität", fmt_num(sum(df()$kwh, na.rm=TRUE)/1000, 0, " MWh")))
  output$kpi_crate <- renderUI({
    ok <- df()$kw > 0 & df()$kwh > 0
    cr <- ifelse(any(ok, na.rm = TRUE),
                 mean(df()$kw[ok] / df()$kwh[ok], na.rm = TRUE), NA)
    mastr_kpi("Ø C-Rate", fmt_num(cr, 2, "  (kW/kWh)"))
  })

  output$plot_scatter <- renderPlotly({
    d <- df()[df()$kw > 0 & df()$kwh > 0, ]
    plot_ly(d, x = ~kwh/1000, y = ~kw/1000, color = ~Technologie,
            type = "scatter", mode = "markers", marker = list(opacity = 0.5)) |>
      layout(xaxis = list(title = "Kapazität [MWh]", type = "log"),
             yaxis = list(title = "Leistung [MW]", type = "log"))
  })

  output$plot_buildout <- renderPlotly({
    d <- aggregate(kw/1000 ~ format(inbetrieb, "%Y"), data = df(), FUN = sum)
    names(d) <- c("year", "mw")
    plot_ly(d, x = ~year, y = ~mw, type = "bar",
            marker = list(color = MASTR_PALETTE$storage))
  })

  output$table_top <- renderReactable({
    d <- df()[order(-df()$kwh, na.last = TRUE), ][1:30, ]
    d$kw  <- round(d$kw / 1000, 2)
    d$kwh <- round(d$kwh / 1000, 2)
    names(d)[names(d) == "kw"]  <- "MW"
    names(d)[names(d) == "kwh"] <- "MWh"
    reactable(d, compact = TRUE, striped = TRUE, defaultPageSize = 10, searchable = TRUE)
  })
}
shinyApp(ui, server)
