# =============================================================================
# care_workers_thuringia — Research dashboard (EN / DE)
# Navigating Expectations in the Recruitment Journey of Care Workers in Thuringia
# =============================================================================

suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(plotly)
  library(leaflet)
  library(reactable)
  library(jsonlite)
  library(scales)
})

source("../../R/ui_helpers.R")
source("../../R/care_workers_data.R")

`%||%` <- function(x, y) if (is.null(x)) y else x

CARE_PRIMARY <- "#6366f1"
INTERVIEWS <- care_workers_load_interviews()
ORGS <- care_workers_load_orgs()
ANALYSIS <- care_workers_load_analysis()

INTERVIEW_CHOICES <- sort(unique(INTERVIEWS$Interview))
THEME_CHOICES <- levels(INTERVIEWS$Theme)
CODE_CHOICES <- sort(unique(INTERVIEWS$Code))
ui <- mastr_page(
  title = "Care Workers in Th\u00fcringen",
  subtitle = uiOutput("page_subtitle"),
  fluid = TRUE,
  primary = CARE_PRIMARY,
  footer = "care_workers",
  hub_back_label = "\u2190 Back to hub / Zur\u00fcck zum Hub",
  creator_qr_lang = "en",
  tags$style(HTML("
    .care-banner {
      background: linear-gradient(135deg, rgba(99,102,241,.18), rgba(139,92,246,.12));
      border: 1px solid rgba(99,102,241,.35);
      border-radius: 12px;
      padding: .85rem 1rem;
      margin-bottom: 1rem;
      font-size: .92rem;
    }
    .care-lang { max-width: 160px; margin-left: auto; margin-bottom: .75rem; }
    .viz-card {
      border: 1px solid rgba(148,163,184,.2);
      border-radius: 12px;
      padding: 1rem;
      height: 100%;
      background: rgba(15,23,42,.55);
      transition: border-color .2s, transform .2s;
    }
    .viz-card:hover { border-color: rgba(99,102,241,.55); transform: translateY(-2px); }
    .viz-card h4 { font-size: 1rem; margin: 0 0 .35rem; }
    .viz-card p { font-size: .85rem; color: #94a3b8; margin: 0 0 .75rem; }
    .viz-frame { width: 100%; min-height: 520px; border: 0; border-radius: 10px; background: #0a0a12; }
  ")),
  div(class = "care-lang", selectInput("lang", NULL, c("English" = "en", "Deutsch" = "de"), selected = "en")),
  uiOutput("care_banner"),
  navset_card_tab(
    id = "main_tabs",
    nav_panel(
      value = "overview",
      title = uiOutput("tab_overview"),
      layout_columns(
        col_widths = c(3, 3, 3, 3),
        uiOutput("kpi_row")
      ),
      layout_columns(
        col_widths = c(6, 6),
        card(card_header(uiOutput("hdr_theme_dist")), plotlyOutput("plot_themes", height = "340px")),
        card(card_header(uiOutput("hdr_interview_dist")), plotlyOutput("plot_interviews", height = "340px"))
      ),
      layout_columns(
        col_widths = c(4, 4, 4),
        card(card_header(uiOutput("hdr_countries")), plotlyOutput("plot_countries", height = "280px")),
        card(card_header(uiOutput("hdr_challenges")), plotlyOutput("plot_challenges", height = "280px")),
        card(card_header(uiOutput("hdr_behaviors")), plotlyOutput("plot_behaviors", height = "280px"))
      )
    ),
    nav_panel(
      value = "orgs",
      title = uiOutput("tab_orgs"),
      layout_sidebar(
        sidebar = sidebar(
          width = 280,
          title = uiOutput("sidebar_map_title"),
          uiOutput("org_filters"),
          uiOutput("org_source_note")
        ),
        layout_columns(
          col_widths = c(8, 4),
          leafletOutput("org_map", height = "520px"),
          card(
            card_header(uiOutput("hdr_by_location")),
            plotlyOutput("plot_org_locations", height = "240px"),
            card_header(uiOutput("hdr_by_sector"), class = "mt-2"),
            plotlyOutput("plot_org_sectors", height = "220px")
          )
        ),
        card(card_header(uiOutput("hdr_org_table")), reactableOutput("org_table"))
      )
    ),
    nav_panel(
      value = "interviews",
      title = uiOutput("tab_interviews"),
      layout_sidebar(
        sidebar = sidebar(
          width = 300,
          title = uiOutput("sidebar_filter_title"),
          uiOutput("interview_filters"),
          tags$p(class = "small text-muted", textOutput("filter_count", inline = TRUE))
        ),
        layout_columns(
          col_widths = c(6, 6),
          card(card_header(uiOutput("hdr_heatmap")), plotlyOutput("plot_heatmap", height = "360px")),
          card(card_header(uiOutput("hdr_theme_stack")), plotlyOutput("plot_theme_stack", height = "360px"))
        ),
        card(card_header(uiOutput("hdr_extracts")), reactableOutput("extract_table"))
      )
    ),
    nav_panel(
      value = "viz",
      title = uiOutput("tab_viz"),
      uiOutput("viz_intro"),
      uiOutput("viz_cards"),
      hr(),
      uiOutput("viz_embed_ui"),
      uiOutput("viz_iframe_ui")
    )
  )
)

server <- function(input, output, session) {
  mastr_hub_back_server(session)

  lang <- reactive(input$lang %||% "en")

  output$page_subtitle <- renderUI({
    care_workers_L(
      lang(),
      paste0(
        "Navigating Expectations \u2014 ",
        ANALYSIS$summary$total_entries, " coded interview extracts \u00b7 ",
        ANALYSIS$summary$total_interviews, " stakeholders \u00b7 ",
        nrow(ORGS), " mapped organizations"
      ),
      paste0(
        "Erwartungen navigieren \u2014 ",
        ANALYSIS$summary$total_entries, " codierte Textstellen \u00b7 ",
        ANALYSIS$summary$total_interviews, " Stakeholder-Interviews \u00b7 ",
        nrow(ORGS), " erfasste Organisationen"
      )
    )
  })

  output$care_banner <- renderUI({
    div(
      class = "care-banner",
      tags$strong(care_workers_L(lang(), "Research project", "Forschungsprojekt")),
      care_workers_L(
        lang(),
        " \u2014 qualitative stakeholder interviews (LEG, AWO AJS, IBS, Diako) and a mapped landscape of ",
        " \u2014 qualitative Stakeholder-Interviews (LEG, AWO AJS, IBS, Diako) und eine Kartierung von "
      ),
      tags$strong("59 "),
      care_workers_L(
        lang(),
        "healthcare & migration organizations across Th\u00fcringen. ",
        "Pflege- & Migrationsorganisationen in Th\u00fcringen. "
      ),
      tags$a(
        href = "https://github.com/Tarekchehahde/Navigating-Expectations-Care-Workers-Thuringia",
        target = "_blank", rel = "noopener",
        care_workers_L(lang(), "GitHub repo", "GitHub-Repository")
      )
    )
  })

  tab_l <- function(en, de) renderUI(care_workers_L(lang(), en, de))
  output$tab_overview <- tab_l("Overview", "Übersicht")
  output$tab_orgs <- tab_l("Organizations", "Organisationen")
  output$tab_interviews <- tab_l("Interviews", "Interviews")
  output$tab_viz <- tab_l("Advanced viz", "Erweiterte Viz")
  output$hdr_theme_dist <- tab_l("Theme distribution", "Themenverteilung")
  output$hdr_interview_dist <- tab_l("Entries by interview", "Einträge nach Interview")
  output$hdr_countries <- tab_l("Recruitment countries mentioned", "Erwähnte Rekrutierungsländer")
  output$hdr_challenges <- tab_l("Challenge keywords", "Herausforderungen (Schlagworte)")
  output$hdr_behaviors <- tab_l("Support behaviour", "Unterstützungsverhalten")
  output$sidebar_map_title <- tab_l("Map filters", "Kartenfilter")
  output$hdr_by_location <- tab_l("By location", "Nach Standort")
  output$hdr_by_sector <- tab_l("By sector", "Nach Sektor")
  output$hdr_org_table <- tab_l("Organization directory", "Organisationsverzeichnis")
  output$sidebar_filter_title <- tab_l("Filters", "Filter")
  output$hdr_heatmap <- tab_l("Code heatmap", "Code-Heatmap")
  output$hdr_theme_stack <- tab_l("Theme by interview", "Themen nach Interview")
  output$hdr_extracts <- tab_l("Coded extracts", "Codierte Textstellen")

  output$kpi_row <- renderUI({
    layout_columns(
      col_widths = c(3, 3, 3, 3),
      mastr_kpi(
        care_workers_L(lang(), "Coded extracts", "Codierte Textstellen"),
        ANALYSIS$summary$total_entries,
        care_workers_L(lang(), "Interview passages", "Interviewpassagen"),
        color = "primary"
      ),
      mastr_kpi(
        care_workers_L(lang(), "Stakeholders", "Stakeholder"),
        ANALYSIS$summary$total_interviews,
        care_workers_L(lang(), "Semi-structured interviews", "Leitfadengespräche"),
        color = "info"
      ),
      mastr_kpi(
        care_workers_L(lang(), "Themes", "Themen"),
        ANALYSIS$summary$total_themes,
        care_workers_L(lang(), "Qualitative code groups", "Qualitative Codegruppen"),
        color = "success"
      ),
      mastr_kpi(
        care_workers_L(lang(), "Organizations", "Organisationen"),
        nrow(ORGS),
        care_workers_L(lang(), "Mapped in Th\u00fcringen", "In Th\u00fcringen erfasst"),
        color = "warning"
      )
    )
  })

  output$org_filters <- renderUI({
    sectors <- sort(unique(ORGS$Sector))
    all_sectors <- care_workers_L(lang(), "All sectors", "Alle Sektoren")
    all_locations <- care_workers_L(lang(), "All locations", "Alle Standorte")
    locs <- sort(unique(ORGS$Location))
    tagList(
      selectInput(
        "org_sector",
        care_workers_L(lang(), "Sector", "Sektor"),
        choices = c(setNames("all", all_sectors), setNames(sectors, care_workers_sector_label(sectors, lang()))),
        selected = input$org_sector %||% "all"
      ),
      selectInput(
        "org_location",
        care_workers_L(lang(), "Location", "Standort"),
        choices = c(setNames("all", all_locations), setNames(locs, locs)),
        selected = input$org_location %||% "all"
      )
    )
  })

  output$org_source_note <- renderUI({
    tags$p(
      class = "small text-muted mb-0",
      care_workers_L(
        lang(),
        "Source: Weltoffenes Th\u00fcringen + Bundesagentur f\u00fcr Arbeit mapping (Nov 2025).",
        "Quelle: Weltoffenes Th\u00fcringen + Bundesagentur f\u00fcr Arbeit (Nov 2025)."
      )
    )
  })

  output$interview_filters <- renderUI({
    tagList(
      checkboxGroupInput(
        "f_interview", care_workers_L(lang(), "Interview", "Interview"),
        choices = INTERVIEW_CHOICES, selected = input$f_interview %||% INTERVIEW_CHOICES
      ),
      checkboxGroupInput(
        "f_theme", care_workers_L(lang(), "Theme", "Thema"),
        choices = setNames(THEME_CHOICES, care_workers_theme_label(THEME_CHOICES, lang())),
        selected = input$f_theme %||% THEME_CHOICES
      ),
      selectizeInput(
        "f_code", care_workers_L(lang(), "Codes", "Codes"),
        choices = CODE_CHOICES, selected = input$f_code %||% CODE_CHOICES,
        multiple = TRUE, options = list(plugins = list("remove_button"))
      ),
      textInput(
        "f_search",
        care_workers_L(lang(), "Search extracts", "Textstellen suchen"),
        value = input$f_search %||% "",
        placeholder = care_workers_L(lang(), "Keyword in quote text\u2026", "Stichwort in Zitat\u2026")
      )
    )
  })

  viz_links <- reactive(care_workers_viz_links(lang()))

  output$viz_intro <- renderUI({
    p(
      class = "text-muted",
      care_workers_L(
        lang(),
        "Full-screen interactive dashboards from the research project.",
        "Vollbild-Interaktiv-Dashboards aus dem Forschungsprojekt."
      )
    )
  })

  output$viz_cards <- renderUI({
    layout_columns(
      col_widths = c(4, 4, 4),
      !!!lapply(viz_links(), function(v) {
        card(
          div(
            class = "viz-card",
            tags$h4(v$title),
            tags$p(v$desc),
            tags$a(
              class = "btn btn-sm btn-primary",
              href = paste0("viz/", v$file),
              target = "_blank", rel = "noopener",
              care_workers_L(lang(), "Open dashboard", "Dashboard öffnen")
            )
          )
        )
      })
    )
  })

  output$viz_embed_ui <- renderUI({
    vl <- viz_links()
    embed_choices <- setNames(
      vapply(vl, function(x) paste0("viz/", x$file), character(1)),
      vapply(vl, `[[`, character(1), "title")
    )
    placeholder <- care_workers_L(lang(), "Select a dashboard\u2026", "Dashboard w\u00e4hlen\u2026")
    selectInput(
      "viz_embed",
      care_workers_L(lang(), "Preview in page", "Vorschau einbetten"),
      choices = c(setNames("", placeholder), embed_choices),
      selected = input$viz_embed %||% ""
    )
  })

  filtered <- reactive({
    df <- INTERVIEWS
    if (length(input$f_interview)) {
      df <- df[df$Interview %in% input$f_interview, , drop = FALSE]
    }
    if (length(input$f_theme)) {
      df <- df[as.character(df$Theme) %in% input$f_theme, , drop = FALSE]
    }
    if (length(input$f_code)) {
      df <- df[df$Code %in% input$f_code, , drop = FALSE]
    }
    q <- input$f_search %||% ""
    if (nzchar(trimws(q))) {
      df <- df[grepl(q, df$`Data Extract`, ignore.case = TRUE), , drop = FALSE]
    }
    df
  })

  output$filter_count <- renderText({
    care_workers_L(
      lang(),
      sprintf("%s of %s extracts shown", nrow(filtered()), nrow(INTERVIEWS)),
      sprintf("%s von %s Textstellen angezeigt", nrow(filtered()), nrow(INTERVIEWS))
    )
  })

  filtered_orgs <- reactive({
    df <- ORGS
    if (!identical(input$org_sector, "all")) {
      df <- df[df$Sector == input$org_sector, , drop = FALSE]
    }
    if (!identical(input$org_location, "all")) {
      df <- df[df$Location == input$org_location, , drop = FALSE]
    }
    df
  })

  plotly_dark <- function(p) {
    ggplotly(p, tooltip = "text") |>
      layout(
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor = "rgba(0,0,0,0)",
        font = list(color = "#e2e8f0"),
        legend = list(orientation = "h", y = -0.15)
      )
  }

  output$plot_themes <- renderPlotly({
    th <- ANALYSIS$themes
    td <- data.frame(
      theme = names(th),
      count = vapply(th, function(x) x$count, numeric(1)),
      percentage = vapply(th, function(x) x$percentage, numeric(1)),
      stringsAsFactors = FALSE
    )
    td <- td[order(-td$count), ]
    td$label <- care_workers_theme_label(td$theme, lang())
    y_lab <- care_workers_L(lang(), "Entries", "Einträge")
    p <- ggplot(td, aes(x = reorder(label, count), y = count, fill = theme,
                        text = paste0(label, ": ", count, " (", percentage, "%)"))) +
      geom_col(show.legend = FALSE) +
      scale_fill_manual(values = care_workers_theme_color(td$theme)) +
      coord_flip() +
      labs(x = NULL, y = y_lab) +
      theme_minimal(base_size = 13) +
      theme(panel.grid.minor = element_blank())
    plotly_dark(p)
  })

  output$plot_interviews <- renderPlotly({
    df <- INTERVIEWS |> count(Interview, name = "count")
    y_lab <- care_workers_L(lang(), "Entries", "Einträge")
    p <- ggplot(df, aes(x = reorder(Interview, count), y = count, fill = Interview,
                        text = paste(Interview, count, sep = ": "))) +
      geom_col(show.legend = FALSE) +
      scale_fill_brewer(palette = "Set2") +
      coord_flip() +
      labs(x = NULL, y = y_lab) +
      theme_minimal(base_size = 13)
    plotly_dark(p)
  })

  named_bar_plot <- function(named_obj, translate = FALSE) {
    df <- care_workers_named_counts(named_obj)
    if (translate) {
      df$label <- care_workers_keyword_label(df$name, lang())
    } else {
      df$label <- df$name
    }
    df <- df[order(-df$count), ]
    y_lab <- care_workers_L(lang(), "Mentions", "Nennungen")
    p <- ggplot(df, aes(x = reorder(label, count), y = count, fill = label,
                        text = paste(label, count, sep = ": "))) +
      geom_col(show.legend = FALSE) +
      scale_fill_brewer(palette = "Pastel1") +
      coord_flip() +
      labs(x = NULL, y = y_lab) +
      theme_minimal(base_size = 12)
    plotly_dark(p)
  }

  output$plot_countries <- renderPlotly({
    named_bar_plot(ANALYSIS$countries, translate = FALSE)
  })

  output$plot_challenges <- renderPlotly({
    named_bar_plot(ANALYSIS$challenges, translate = TRUE)
  })

  output$plot_behaviors <- renderPlotly({
    named_bar_plot(ANALYSIS$behaviors, translate = TRUE)
  })

  output$org_map <- renderLeaflet({
    df <- filtered_orgs()
    pal <- colorFactor("Set2", domain = unique(ORGS$Sector))
    sector_title <- care_workers_L(lang(), "Sector", "Sektor")
    link_lbl <- care_workers_L(lang(), "Website", "Website")
    leaflet(df) |>
      addProviderTiles(providers$CartoDB.DarkMatter) |>
      setView(lng = 10.75, lat = 50.95, zoom = 8) |>
      addCircleMarkers(
        ~Longitude, ~Latitude,
        radius = 7,
        stroke = TRUE, weight = 1, opacity = 0.9,
        fillOpacity = 0.85,
        color = ~pal(Sector),
        label = ~Institutions,
        popup = ~paste0(
          "<strong>", Institutions, "</strong><br>",
          Location, " · ", care_workers_sector_label(Sector, lang()), "<br>",
          "<a href='", Website, "' target='_blank'>", link_lbl, "</a>"
        )
      ) |>
      addLegend(
        "bottomright", pal = pal, values = ORGS$Sector,
        title = sector_title,
        labFormat = labelFormat(transform = function(x) care_workers_sector_label(x, lang()))
      )
  })

  output$plot_org_locations <- renderPlotly({
    df <- ORGS |> count(Location, sort = TRUE) |> slice_head(n = 12)
    y_lab <- care_workers_L(lang(), "Organizations", "Organisationen")
    p <- ggplot(df, aes(x = reorder(Location, n), y = n, fill = Location)) +
      geom_col(show.legend = FALSE) +
      coord_flip() +
      labs(x = NULL, y = y_lab) +
      theme_minimal(base_size = 12)
    plotly_dark(p)
  })

  output$plot_org_sectors <- renderPlotly({
    df <- ORGS |> count(Sector) |>
      mutate(SectorLabel = care_workers_sector_label(Sector, lang()))
    p <- ggplot(df, aes(x = "", y = n, fill = SectorLabel)) +
      geom_col(width = 1) +
      coord_polar("y") +
      scale_fill_brewer(palette = "Set2") +
      theme_void(base_size = 12) +
      theme(legend.position = "right")
    plotly_dark(p)
  })

  output$org_table <- renderReactable({
    df <- filtered_orgs() |>
      mutate(Sector = care_workers_sector_label(Sector, lang())) |>
      select(
        Institutions,
        Location,
        Sector,
        Email,
        Website
      )
    names(df)[3] <- care_workers_L(lang(), "Sector", "Sektor")
    reactable(
      df,
      searchable = TRUE,
      striped = TRUE,
      highlight = TRUE,
      defaultPageSize = 10,
      columns = list(
        Website = colDef(
          cell = function(value) {
            if (is.na(value) || !nzchar(value)) return("")
            htmltools::tags$a(
              href = value, target = "_blank",
              care_workers_L(lang(), "Link", "Link")
            )
          }
        )
      )
    )
  })

  output$plot_heatmap <- renderPlotly({
    df <- filtered()
    if (!nrow(df)) {
      return(plotly_empty(type = "scatter", mode = "markers") |>
               layout(title = list(
                 text = care_workers_L(lang(), "No data for current filters", "Keine Daten für aktuelle Filter"),
                 font = list(color = "#94a3b8")
               )))
    }
    mat <- as.data.frame.matrix(table(df$Code, df$Interview))
    plot_ly(
      x = colnames(mat),
      y = rownames(mat),
      z = as.matrix(mat),
      type = "heatmap",
      colorscale = "Viridis"
    ) |>
      layout(
        xaxis = list(title = care_workers_L(lang(), "Interview", "Interview")),
        yaxis = list(title = care_workers_L(lang(), "Code", "Code")),
        paper_bgcolor = "rgba(0,0,0,0)",
        plot_bgcolor = "rgba(0,0,0,0)",
        font = list(color = "#e2e8f0")
      )
  })

  output$plot_theme_stack <- renderPlotly({
    df <- filtered() |>
      count(Interview, Theme) |>
      mutate(ThemeLabel = care_workers_theme_label(as.character(Theme), lang()))
    y_lab <- care_workers_L(lang(), "Entries", "Einträge")
    p <- ggplot(df, aes(x = Interview, y = n, fill = ThemeLabel)) +
      geom_col(position = "stack") +
      scale_fill_manual(values = setNames(
        care_workers_theme_color(unique(as.character(df$Theme))),
        care_workers_theme_label(unique(as.character(df$Theme)), lang())
      )) +
      labs(x = NULL, y = y_lab, fill = NULL) +
      theme_minimal(base_size = 12) +
      theme(axis.text.x = element_text(angle = 30, hjust = 1))
    plotly_dark(p)
  })

  output$extract_table <- renderReactable({
    df <- filtered() |>
      transmute(
        Interview,
        Code,
        `Code Name` = `Code Name`,
        Theme = care_workers_theme_label(as.character(Theme), lang()),
        `Data Extract` = `Data Extract`
      )
    if (identical(lang(), "de")) {
      names(df)[3] <- "Code-Name"
      names(df)[5] <- "Textstelle"
    }
    reactable(
      df,
      searchable = FALSE,
      striped = TRUE,
      highlight = TRUE,
      defaultPageSize = 8,
      columns = list(
        Theme = colDef(
          cell = function(value) {
            htmltools::tags$span(
              style = "background:#6366f1;color:#fff;padding:2px 8px;border-radius:999px;font-size:11px",
              value
            )
          }
        ),
        `Data Extract` = colDef(minWidth = 320),
        Textstelle = colDef(minWidth = 320)
      )
    )
  })

  output$viz_iframe_ui <- renderUI({
    src <- input$viz_embed
    if (!nzchar(src)) {
      return(tags$p(
        class = "text-muted",
        care_workers_L(lang(), "Choose a dashboard above to embed a live preview.",
                       "Wählen Sie oben ein Dashboard für die Vorschau.")
      ))
    }
    tags$iframe(
      class = "viz-frame", src = src,
      title = care_workers_L(lang(), "Research visualization", "Forschungsvisualisierung")
    )
  })
}

shinyApp(ui, server)
