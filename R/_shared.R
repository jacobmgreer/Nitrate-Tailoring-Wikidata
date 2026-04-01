library(duckdb)
library(stringr)
library(readr)
library(httr)
library(jsonlite)

options(timeout = 10*60)

# Create necessary directories
dir.create(path = "release", showWarnings = FALSE)
dir.create(path = "json", showWarnings = FALSE)

# Helper: encode a SPARQL file into a query URL
sparql_url <- function(filename) {
  query_lines <- read_lines(filename)
  query_lines_nocomment <- str_replace(query_lines, "#.*$", "")
  query_text <- paste(query_lines_nocomment[query_lines_nocomment != ""], collapse = " ")
  query <- URLencode(str_squish(query_text))
  paste0("https://query.wikidata.org/sparql?query=", query, "&format=json")
}

# Robust fetch with retry logic (HTTP only)
robust_fetch <- function(url, retries = 3, delay = 500) {
  for (i in seq_len(retries)) {
    resp <- try(GET(url), silent = TRUE)
    if (inherits(resp, "response") && status_code(resp) == 200) {
      return(resp)
    } else {
      message(sprintf("Attempt %d failed for %s.", i, url))
      if (i < retries) Sys.sleep(delay)
    }
  }
  stop(sprintf("Failed to fetch SPARQL results after %d attempts for %s.", retries, url))
}

# Process a SPARQL file and save only `results.bindings`
process_sparql <- function(file, retries = 3, delay = 30) {
  url <- sparql_url(file)

  # First attempt group: try fetching & parsing up to `retries` times
  for (i in seq_len(retries)) {
    resp <- robust_fetch(url, retries = 1, delay = 0) # Only 1 HTTP fetch per outer loop
    parsing_success <- FALSE
    bindings <- NULL
    try({
      content <- content(resp, as = "parsed", type = "application/json")
      bindings <- content$results$bindings
      parsing_success <- TRUE
    }, silent = TRUE)
    if (parsing_success) {
      json_path <- paste0("json/", basename(dirname(file)))
      dir.create(json_path, showWarnings = F, recursive = T)
      write_json(
        x = bindings,
        path = paste0(json_path, "/", basename(file), ".json"),
        pretty = TRUE,
        auto_unbox = TRUE)
      message(sprintf("Saved bindings from %s", file))
      return(paste0(json_path, "/", basename(file), ".json"))
    } else {
      message(sprintf("Attempt %d failed to parse SPARQL results for %s.", i, file))
      if (i < retries) Sys.sleep(delay)
    }
  }

  # If here, all initial parsing attempts failed: wait, retry rescanning again
  message(sprintf("Waiting 5 minutes before retrying fetch & parse for %s...", file))
  Sys.sleep(5 * 60)

  for (j in seq_len(retries)) {
    resp <- robust_fetch(url, retries = 1, delay = 0)
    parsing_success <- FALSE
    bindings <- NULL
    try({
      content <- content(resp, as = "parsed", type = "application/json")
      bindings <- content$results$bindings
      parsing_success <- TRUE
    }, silent = TRUE)
    if (parsing_success) {
      json_path <- paste0("json/", basename(dirname(file)))
      dir.create(json_path, showWarnings = F, recursive = T)
      write_json(
        x = bindings,
        path = paste0(json_path, "/", basename(file), ".json"),
        pretty = TRUE,
        auto_unbox = TRUE)
      message(sprintf("Saved bindings from %s (after second retry group)", file))
      return(paste0(json_path, "/", basename(file), ".json"))
    } else {
      message(sprintf("Second group: attempt %d failed to parse SPARQL results for %s.", j, file))
      if (j < retries) Sys.sleep(delay)
    }
  }

  # If *all* attempts fail, consider this matrix job failed
  stop(sprintf("Failed to fetch and parse SPARQL results after retrying for %s.", file))
}

# --- DuckDB setup unchanged ---

con <- dbConnect(duckdb::duckdb())
dbExecute(con, "INSTALL json; LOAD json; SET preserve_insertion_order=false;")

dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS records (
      db VARCHAR,
      item VARCHAR,
      updated DATE
  );

  CREATE MACRO records(path, db) AS TABLE
  SELECT
    db AS db,
    CAST(parse_filename(item.value) AS VARCHAR) AS item,
    CAST(date.value AS DATE) AS updated
  FROM read_json_auto(path, maximum_object_size=1000000000);
")

dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS properties (
    source VARCHAR,
    property VARCHAR,
    item VARCHAR,
    updated DATE,
  );
  CREATE MACRO properties(path, source, prop) AS TABLE
    SELECT 
      source AS source,
      prop AS property,
      parse_filename(item.value) AS item,
      CAST(date.value AS DATE) AS updated
    FROM read_json_auto(path, maximum_object_size=1000000000, ignore_errors = T);
")

# dbExecute(con, "
#   CREATE TEMP TABLE IF NOT EXISTS instances (
#       source VARCHAR,
#       p31 VARCHAR,
#       p31_updated DATE,
#       n_records INTEGER
#     );
#   CREATE MACRO instances(path, source) AS TABLE
#     SELECT 
#       source AS source, 
#       parse_filename(p31.value) AS p31,
#       CAST(p31_updated.value AS DATE) AS p31_updated,
#       CAST(record_count.value AS INTEGER) AS n_records
#     FROM read_json_auto(path, maximum_object_size=1000000000)
#     ORDER BY n_records DESC;
# ")

# dbExecute(con, "
#   CREATE TEMP TABLE IF NOT EXISTS occupations (
#       source VARCHAR,
#       p106 VARCHAR,
#       p106_updated DATE
#     );
#   CREATE MACRO occupations(path, source) AS TABLE
#     SELECT 
#       source AS source, 
#       parse_filename(p106.value) AS p106,
#       CAST(p106_updated.value AS DATE) AS p106_updated
#     FROM read_json_auto(path, maximum_object_size=1000000000);
# ")

dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS members (
      source VARCHAR,
      content VARCHAR,
      prop VARCHAR,
      grouped VARCHAR,
      grouped_updated DATE,
      member VARCHAR,
      member_updated DATE
    );
  CREATE MACRO members(path, source) AS TABLE
    SELECT 
      source AS source,
      parse_filename(content.value) AS content,
      parse_filename(prop.value) AS prop,
      parse_filename(grouped.value) AS grouped,
      CAST(grouped_updated.value AS DATE) AS grouped_updated,
      parse_filename(associated.value) AS member,
      CAST(associated_updated.value AS DATE) AS member_updated
    FROM read_json_auto(path, maximum_object_size=1000000000);
")

dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS credits (
      source VARCHAR,
      property VARCHAR,
      content VARCHAR,
      content_updated DATE
    );
  CREATE MACRO credits(path, source, prop) AS TABLE
    SELECT 
      source AS source,
      prop AS property,
      parse_filename(content.value) AS content,
      CAST(updated.value AS DATE) AS content_updated
    FROM read_json_auto(path, maximum_object_size=1000000000);
")

dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS deprecated (
      source VARCHAR,
      item VARCHAR,
      property VARCHAR,
      value VARCHAR
    );
  CREATE MACRO deprecated(path, source) AS TABLE
    SELECT 
      source AS source,
      split_part(parse_filename(property.value), '-', 1) AS item,
      parse_filename(property.value) AS property,
      value.value AS value
    FROM read_json_auto(path, maximum_object_size=1000000000);
")