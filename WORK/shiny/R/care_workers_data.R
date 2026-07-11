# Care workers Thuringia — data loaders (interviews + organizations + analysis JSON)

CARE_THEME_COLORS <- c(
  "Transnational Recruitment" = "#6366f1",
  "Worker Suitability" = "#8b5cf6",
  "Pre-Arrival Expectations" = "#ec4899",
  "Extended Support" = "#10b981",
  "Qualification Mismatches" = "#f59e0b",
  "Adverse Outcomes" = "#ef4444",
  "Unknown" = "#64748b"
)

care_workers_data_dir <- function() {
  app_dir <- Sys.getenv("MASTR_CARE_WORKERS_DATA", "")
  if (nzchar(app_dir) && dir.exists(app_dir)) {
    return(normalizePath(app_dir, winslash = "/"))
  }
  candidates <- c(
    "data",
    file.path(getwd(), "data"),
    "/opt/mastr-shiny/WORK/shiny/apps/care_workers_thuringia/data"
  )
  for (p in candidates) {
    if (file.exists(file.path(p, "consolidated_interviews.csv"))) {
      return(normalizePath(p, winslash = "/"))
    }
  }
  stop("care_workers data directory not found", call. = FALSE)
}

care_workers_load_interviews <- function() {
  path <- file.path(care_workers_data_dir(), "consolidated_interviews.csv")
  df <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  names(df) <- gsub("\\.", " ", names(df))
  if (!"Data Extract" %in% names(df) && "Data.Extract" %in% names(df)) {
    names(df)[names(df) == "Data.Extract"] <- "Data Extract"
  }
  df$Extract_Length <- nchar(df$`Data Extract`)
  df$Theme <- factor(df$Theme, levels = names(CARE_THEME_COLORS))
  df
}

care_workers_load_orgs <- function() {
  path <- file.path(care_workers_data_dir(), "Consolidated_lists.csv")
  df <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  df <- df[!is.na(df$Latitude) & !is.na(df$Longitude), , drop = FALSE]
  df$Sector <- ifelse(grepl("^R", df$Schlüssel), "Health & social work",
    ifelse(grepl("^Q", df$Schlüssel), "Education",
      ifelse(grepl("^T", df$Schlüssel), "Advocacy & services", "Other")))
  df
}

care_workers_theme_color <- function(theme) {
  out <- CARE_THEME_COLORS[as.character(theme)]
  out[is.na(out)] <- CARE_THEME_COLORS["Unknown"]
  unname(out)
}

care_workers_load_analysis <- function() {
  path <- file.path(care_workers_data_dir(), "analysis_data.json")
  jsonlite::fromJSON(path, simplifyVector = TRUE)
}

#' Convert JSON named count objects (list) to a two-column data frame.
care_workers_named_counts <- function(x) {
  if (is.null(x) || !length(x)) {
    return(data.frame(name = character(), count = integer(), stringsAsFactors = FALSE))
  }
  data.frame(
    name = names(x),
    count = vapply(x, function(v) as.integer(v), integer(1)),
    stringsAsFactors = FALSE
  )
}

care_workers_L <- function(lang, en, de) {
  if (identical(lang, "de")) de else en
}

care_workers_theme_label <- function(theme, lang = "en") {
  de <- c(
    "Transnational Recruitment" = "Transnationale Rekrutierung",
    "Worker Suitability" = "Eignung der Fachkr\u00e4fte",
    "Pre-Arrival Expectations" = "Erwartungen vor Ankunft",
    "Extended Support" = "Erweiterte Unterst\u00fctzung",
    "Qualification Mismatches" = "Qualifikations-Diskrepanzen",
    "Adverse Outcomes" = "Negative Verl\u00e4ufe",
    "Unknown" = "Unbekannt"
  )
  th <- as.character(theme)
  if (identical(lang, "de")) {
    out <- de[th]
    out[is.na(out)] <- th[is.na(out)]
    return(unname(out))
  }
  th
}

care_workers_sector_label <- function(sector, lang = "en") {
  de <- c(
    "Health & social work" = "Gesundheit & Soziales",
    "Education" = "Bildung",
    "Advocacy & services" = "Advocacy & Dienste",
    "Other" = "Sonstige"
  )
  s <- as.character(sector)
  if (identical(lang, "de")) {
    out <- de[s]
    out[is.na(out)] <- s[is.na(out)]
    return(unname(out))
  }
  s
}

care_workers_keyword_label <- function(name, lang = "en") {
  de <- c(
    Language = "Sprache",
    Bureaucracy = "B\u00fcrokratie",
    Integration = "Integration",
    Retention = "Bindung / Fluktuation",
    Discrimination = "Diskriminierung",
    `Proactive Support` = "Proaktive Unterst\u00fctzung",
    `Reactive/Acknowledgment` = "Reaktiv / Anerkennung",
    `Structural/Systemic` = "Strukturell / Systemisch"
  )
  n <- as.character(name)
  if (identical(lang, "de")) {
    out <- de[n]
    out[is.na(out)] <- n[is.na(out)]
    return(unname(out))
  }
  n
}

care_workers_viz_links <- function(lang = "en") {
  links <- list(
    list(
      file = "interview_insights_dashboard.html",
      file_de = "interview_insights_dashboard.html",
      title_en = "Interview insights",
      title_de = "Interview-Einblicke",
      desc_en = "Theme radar, stakeholder profiles, coded entry explorer.",
      desc_de = "Themen-Radar, Stakeholder-Profile, codierte Textstellen."
    ),
    list(
      file = "consolidated_dashboard.html",
      file_de = "consolidated_dashboard:de.html",
      title_en = "Organizations map",
      title_de = "Organisationskarte",
      desc_en = "59 Thuringia orgs \u2014 map, sector charts, WZ classification.",
      desc_de = "59 Organisationen \u2014 Karte, Branchen, WZ-Klassifikation."
    ),
    list(
      file = "network_dashboard.html",
      file_de = "network_dashboard:de.html",
      title_en = "Network view",
      title_de = "Netzwerk-Ansicht",
      desc_en = "Relationships between organizations and service types.",
      desc_de = "Beziehungen zwischen Organisationen und Leistungen."
    ),
    list(
      file = "quotations_by_theme.html",
      file_de = "quotations_by_theme.html",
      title_en = "Quotations by theme",
      title_de = "Zitate nach Thema",
      desc_en = "Qualitative extracts grouped by research theme.",
      desc_de = "Qualitative Textstellen nach Forschungsthema."
    ),
    list(
      file = "findings.html",
      file_de = "findings:de.html",
      title_en = "Key findings",
      title_de = "Kernergebnisse",
      desc_en = "Executive summary of recruitment pathways.",
      desc_de = "Zusammenfassung der Rekrutierungswege."
    ),
    list(
      file = "metadata_viewer.html",
      file_de = "metadata_viewer:de.html",
      title_en = "Metadata viewer",
      title_de = "Metadaten-Viewer",
      desc_en = "Project structure and data dictionary.",
      desc_de = "Projektstruktur und Datenw\u00f6rterbuch."
    )
  )
  lapply(links, function(x) {
    list(
      file = if (identical(lang, "de")) x$file_de else x$file,
      title = if (identical(lang, "de")) x$title_de else x$title_en,
      desc = if (identical(lang, "de")) x$desc_de else x$desc_en
    )
  })
}
