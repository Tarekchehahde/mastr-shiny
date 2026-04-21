# =============================================================================
# mastr_data.R — shared data-access layer for every dashboard in apps/
#
# The user NEVER downloads the XML. They also don't strictly need to download
# the DuckDB: every function here queries the Parquet files remotely through
# DuckDB's httpfs extension. Only the (usually tiny) result set is transferred.
#
# How it works:
#   1. `mastr_release_base()` resolves the most recent GitHub Release tag
#      (e.g. data-2026-04-21) and caches the base URL.
#   2. `mastr_con()` returns an in-memory DuckDB connection with httpfs + a
#      few CREATE VIEW statements that point at the remote Parquet files.
#   3. `mastr_query()` / `mastr_table()` are thin wrappers around DBI::dbGetQuery.
#   4. Optional: users can call `mastr_use_local(path)` once to switch to a
#      locally downloaded DuckDB file for fully offline use.
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(memoise)
  library(httr2)
  library(rlang)
})

# ----- configuration ---------------------------------------------------------

.mastr_env <- new.env(parent = emptyenv())
.mastr_env$repo        <- Sys.getenv("MASTR_REPO", "Tarekchehahde/mastr-shiny")
.mastr_env$release_tag <- NULL            # resolved on first use
.mastr_env$base_url    <- NULL            # e.g. https://github.com/.../releases/download/data-2026-04-21
.mastr_env$local_db    <- NULL            # optional local .duckdb
.mastr_env$con         <- NULL

mastr_set_repo <- function(repo) {
  .mastr_env$repo <- repo
  .mastr_env$release_tag <- NULL
  .mastr_env$base_url <- NULL
  mastr_disconnect()
  invisible(repo)
}

mastr_use_local <- function(duckdb_path) {
  stopifnot(file.exists(duckdb_path))
  .mastr_env$local_db <- normalizePath(duckdb_path)
  mastr_disconnect()
  invisible(duckdb_path)
}

# ----- resolve latest release ------------------------------------------------

.resolve_release <- function() {
  if (!is.null(.mastr_env$release_tag)) return(invisible())
  repo <- .mastr_env$repo
  url  <- sprintf("https://api.github.com/repos/%s/releases/latest", repo)
  req  <- httr2::request(url) |>
    httr2::req_headers("Accept" = "application/vnd.github+json")
  tok  <- Sys.getenv("GITHUB_TOKEN", "")
  if (nzchar(tok)) req <- httr2::req_auth_bearer_token(req, tok)
  resp <- tryCatch(httr2::req_perform(req), error = function(e) NULL)
  if (is.null(resp) || httr2::resp_status(resp) >= 400) {
    abort(sprintf(
      "Could not resolve latest release for %s. Set MASTR_REPO or MASTR_TAG, or call mastr_use_local().",
      repo))
  }
  body <- httr2::resp_body_json(resp)
  .mastr_env$release_tag <- body$tag_name
  .mastr_env$base_url    <- sprintf("https://github.com/%s/releases/download/%s",
                                    repo, body$tag_name)
  invisible()
}

mastr_release_info <- function() {
  .resolve_release()
  list(repo = .mastr_env$repo,
       tag  = .mastr_env$release_tag,
       base = .mastr_env$base_url)
}

# ----- connection ------------------------------------------------------------

#' Get a DuckDB connection bound to the latest MaStR release (or local db).
#'
#' Safe to call from many reactive expressions — the connection is cached.
mastr_con <- function() {
  if (!is.null(.mastr_env$con) && DBI::dbIsValid(.mastr_env$con)) return(.mastr_env$con)

  if (!is.null(.mastr_env$local_db)) {
    con <- dbConnect(duckdb::duckdb(), dbdir = .mastr_env$local_db, read_only = TRUE)
  } else {
    .resolve_release()
    con <- dbConnect(duckdb::duckdb())
    dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
    dbExecute(con, "SET enable_http_metadata_cache=true;")
    dbExecute(con, "SET http_keep_alive=true;")
    .create_remote_views(con)
  }
  .mastr_env$con <- con
  con
}

mastr_disconnect <- function() {
  if (!is.null(.mastr_env$con) && DBI::dbIsValid(.mastr_env$con)) {
    try(dbDisconnect(.mastr_env$con, shutdown = TRUE), silent = TRUE)
  }
  .mastr_env$con <- NULL
  invisible()
}

# The entity list mirrors etl/src/mastr_etl/config.py ENTITIES. Keep in sync.
.remote_entities <- c(
  "solar", "wind", "biomasse", "wasser", "geothermie", "kernkraft",
  "verbrennung", "stromspeicher", "gaserzeuger", "gasverbraucher",
  "gasspeicher", "kwk", "eeg_solar", "eeg_wind", "eeg_biomasse", "eeg_wasser",
  "marktakteure", "netzanschlusspunkte", "bilanzierungsgebiete", "lokationen"
)

.create_remote_views <- function(con) {
  base <- .mastr_env$base_url
  for (e in .remote_entities) {
    url <- sprintf("%s/%s.parquet", base, e)
    # CREATE VIEW over a remote Parquet; DuckDB only fetches row groups touched
    # by each query (range requests over HTTPS).
    sql <- sprintf(
      "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
      e, url)
    try(dbExecute(con, sql), silent = TRUE)
  }
  # Aggregate parquets (small, ~1 MB each) that Python pre-rolled:
  agg_files <- c(
    "kpi_overview", "capacity_by_state", "buildout_monthly",
    "capacity_by_plz_top5000", "solar_size_classes",
    "wind_hub_height", "ee_quote_by_year"
  )
  for (a in agg_files) {
    url <- sprintf("%s/%s.parquet", base, a)
    sql <- sprintf(
      "CREATE OR REPLACE VIEW agg_%s AS SELECT * FROM read_parquet('%s')",
      a, url)
    try(dbExecute(con, sql), silent = TRUE)
  }
  # Re-create the cross-entity view (same SQL as build_duckdb.py).
  .create_units_view(con)
}

# Mirror of build_duckdb._table_columns / _col_or_null. Needed because BNetzA
# ships heterogeneous schemas across entity types (e.g. kernkraft has ~45
# columns vs solar's ~70), and a single missing column in one branch of the
# UNION ALL would otherwise fail the whole view binder — the same bug that
# bit the server-side build in run #4. Keep this in sync with the Python.
.table_columns <- function(con, table) {
  res <- tryCatch(
    DBI::dbGetQuery(con, sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table)),
    error = function(e) NULL
  )
  if (is.null(res) || nrow(res) == 0) return(character(0))
  tolower(res$column_name)
}

.col_or_null <- function(cols, name, cast = NULL, alias = "t") {
  expr <- if (tolower(name) %in% cols) sprintf("%s.%s", alias, name) else "NULL"
  if (!is.null(cast)) sprintf("TRY_CAST(%s AS %s)", expr, cast) else expr
}

.create_units_view <- function(con) {
  unit_map <- c(
    solar         = "SolareStrahlungsenergie",
    wind          = "Wind",
    biomasse      = "Biomasse",
    wasser        = "Wasser",
    geothermie    = "GeothermieGrubenKlaerschlamm",
    kernkraft     = "Kernenergie",
    verbrennung   = "FossilOderSonstige",
    stromspeicher = "Speicher"
  )
  parts <- character(0)
  for (k in names(unit_map)) {
    cols <- .table_columns(con, k)
    if (length(cols) == 0L) next  # remote view never materialised
    eg <- unit_map[[k]]
    parts <- c(parts, sprintf("
      SELECT
        '%1$s'                                                     AS source_table,
        '%2$s'                                                     AS energietraeger,
        %3$s                                                       AS mastr_nr,
        %4$s                                                       AS bruttoleistung_kw,
        %5$s                                                       AS nettonennleistung_kw,
        %6$s                                                       AS bundesland_code,
        %7$s                                                       AS gemeinde,
        %8$s                                                       AS plz,
        %9$s                                                       AS lon,
        %10$s                                                      AS lat,
        %11$s                                                      AS inbetriebnahme_datum,
        %12$s                                                      AS betriebsstatus
      FROM %1$s t",
      k, eg,
      .col_or_null(cols, "EinheitMastrNummer"),
      .col_or_null(cols, "Bruttoleistung",     cast = "DOUBLE"),
      .col_or_null(cols, "Nettonennleistung",  cast = "DOUBLE"),
      .col_or_null(cols, "Bundesland"),
      .col_or_null(cols, "Gemeinde"),
      .col_or_null(cols, "Postleitzahl"),
      .col_or_null(cols, "Laengengrad",        cast = "DOUBLE"),
      .col_or_null(cols, "Breitengrad",        cast = "DOUBLE"),
      .col_or_null(cols, "Inbetriebnahmedatum", cast = "DATE"),
      .col_or_null(cols, "Betriebsstatus")
    ))
  }
  if (length(parts) == 0L) return(invisible())
  sql <- paste("CREATE OR REPLACE VIEW v_units_all AS",
               paste(parts, collapse = "\nUNION ALL\n"))
  try(dbExecute(con, sql), silent = TRUE)
}

# ----- query helpers ---------------------------------------------------------

#' Run a SQL query and return a data.frame. Memoised for the duration of the
#' R session so repeated reactive evaluations don't re-fetch.
mastr_query <- memoise::memoise(function(sql, params = list()) {
  con <- mastr_con()
  if (length(params)) {
    DBI::dbGetQuery(con, sql, params = params)
  } else {
    DBI::dbGetQuery(con, sql)
  }
})

#' Pull an entire (small) view/table as a data.frame. Intended for KPI tiles
#' and aggregate parquets; do NOT call on a full units table.
mastr_table <- function(name) {
  mastr_query(sprintf("SELECT * FROM %s", DBI::dbQuoteIdentifier(mastr_con(), name)))
}

#' List of Bundesländer (with code) — constant, fine to cache forever.
mastr_bundeslaender <- memoise::memoise(function() {
  mastr_query("
    SELECT DISTINCT bundesland_name AS name
    FROM v_units_all
    WHERE bundesland_name IS NOT NULL
    ORDER BY 1
  ")$name
})

mastr_energietraeger <- memoise::memoise(function() {
  mastr_query("SELECT DISTINCT energietraeger FROM v_units_all ORDER BY 1")$energietraeger
})

# ----- footer helper ---------------------------------------------------------

mastr_attribution <- function() {
  info <- tryCatch(mastr_release_info(), error = function(e) list(tag = "unknown"))
  sprintf(
    "Datenquelle: Marktstammdatenregister — \u00a9 Bundesnetzagentur (Stand: %s), bereitgestellt unter DL-DE-BY-2.0.",
    sub("^data-", "", info$tag %||% "unknown")
  )
}
