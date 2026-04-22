# =============================================================================
# app.R — single entry point that shows a picker and launches the chosen
# dashboard. Open this file in RStudio and click "Run App", or call
#   shiny::runApp("shiny")                    # from the repo root
#   shiny::runGitHub("mastr-shiny", "Tarekchehahde", subdir = "shiny",
#                    ref = "main", launch.browser = TRUE)
#
# Tip: to launch a single dashboard directly, run
#   shiny::runApp("apps/02_solar_pv")
# =============================================================================

suppressPackageStartupMessages({
  library(shiny); library(bslib)
})

APPS <- list(
  list(id = "01_overview",       title = "MaStR — Überblick",
       desc = "KPIs: Einheiten, installierte Leistung, EE-Anteil"),
  list(id = "02_solar_pv",       title = "Solar PV",
       desc = "PV-Fleet nach Größenklasse und Bundesland"),
  list(id = "03_wind_onshore",   title = "Wind Onshore",
       desc = "Turbinen, Nabenhöhen, Rotordurchmesser"),
  list(id = "04_wind_offshore",  title = "Wind Offshore",
       desc = "Offshore-Parks, Wassertiefe, Küstenentfernung"),
  list(id = "05_biomass",        title = "Biomasse",
       desc = "Biogas- und Biomasseanlagen"),
  list(id = "06_hydro",          title = "Wasserkraft",
       desc = "Laufwasser, Speicher, Pumpspeicher"),
  list(id = "07_geothermal",     title = "Geothermie & Sonstige",
       desc = "Tiefe Geothermie, Solarthermie, Grubengas"),
  list(id = "08_storage",        title = "Stromspeicher",
       desc = "Batterie + Pumpspeicher, Leistung und Kapazität"),
  list(id = "09_chp",            title = "KWK",
       desc = "Kraft-Wärme-Kopplung: el/th Nutzleistung"),
  list(id = "10_grid_operators", title = "Netzbetreiber",
       desc = "Netzanschlusspunkte je Betreiber + Spannungsebene"),
  list(id = "11_market_actors",  title = "Marktakteure",
       desc = "Betreiber, Händler, Netzbetreiber, …"),
  list(id = "12_geo_map",        title = "Geo-Karte",
       desc = "PLZ-geclusterte Karte aller Einheiten"),
  list(id = "13_capacity_trends",title = "Zubau-Trends",
       desc = "Monatlicher / kumulativer Zubau nach Technologie"),
  list(id = "14_state_comparison",title = "Bundesländer-Vergleich",
       desc = "Absolut + pro Kopf Ranking"),
  list(id = "15_ee_quote",       title = "EE-Quote",
       desc = "EE-Anteil pro Jahr und Bundesland")
)

ui <- page_fillable(
  title = "MaStR Shiny — Launcher",
  theme = bs_theme(version = 5, bootswatch = "flatly"),
  div(class = "container py-3",
      h2("MaStR Shiny Dashboards"),
      p(class = "text-muted",
        "Daten werden live aus dem neuesten GitHub-Release gelesen (kein XML nötig)."),
      layout_column_wrap(
        width = 1/3,
        !!!lapply(APPS, function(a) {
          card(
            card_header(a$title),
            p(a$desc),
            actionButton(paste0("go_", a$id), "Start", class = "btn-primary")
          )
        })
      )
  )
)

server <- function(input, output, session) {
  for (a in APPS) local({
    id <- a$id
    observeEvent(input[[paste0("go_", id)]], {
      path <- file.path("apps", id)
      session$sendCustomMessage(
        "jsCode",
        list(code = sprintf(
          "window.alert('Starting %s… Close this window, then run:\\nshiny::runApp(\"%s\")');",
          id, path))
      )
      # If running as a normal Shiny app (not hosted), we can also stop and
      # relaunch. In RStudio the common pattern is: stopApp then runApp.
      stopApp(returnValue = path)
    })
  })
}

launched <- shinyApp(ui, server)
# When run via Rscript, honor the returned value:
if (interactive()) {
  picked <- runApp(launched)
  if (is.character(picked) && nzchar(picked)) {
    message(sprintf(">> launching %s", picked))
    shiny::runApp(picked)
  }
} else {
  launched
}
