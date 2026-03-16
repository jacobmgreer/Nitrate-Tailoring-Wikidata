library(duckdb)
library(stringr)
library(readr)
library(httr)
library(jsonlite)

# Create necessary directories
dir.create(path = "release", showWarnings = FALSE)
dir.create(path = "json", showWarnings = FALSE)

# Helper function: encode a SPARQL file into a query URL
sparql_url <- function(filename) {
  query_lines <- read_lines(filename)
  query_lines_nocomment <- str_replace(query_lines, "#.*$", "")  # Remove comments
  query_text <- paste(query_lines_nocomment[query_lines_nocomment != ""], collapse = " ")
  query <- URLencode(str_squish(query_text))
  paste0("https://query.wikidata.org/sparql?query=", query, "&format=json")
}

# Function to process a SPARQL file and save only `results.bindings`
process_sparql <- function(file) {
  url <- sparql_url(file)
  resp <- GET(url)  # Send the SPARQL query
  
  if (status_code(resp) != 200) stop(sprintf("Failed to fetch SPARQL results for %s. Status: %d", file, status_code(resp)))

  # Explicitly parse as JSON to handle `application/sparql-results+json`
  content <- content(resp, as = "parsed", type = "application/json")
  bindings <- content$results$bindings  # Extract `results.bindings`

  # Save the relevant section of the JSON to a temporary file
  json_path <- paste0("json/", basename(file), ".json")
  write_json(bindings, json_path, pretty = TRUE, auto_unbox = TRUE)
  message(sprintf("Saved bindings to %s", json_path))
  
  # Return the path to the saved JSON file
  return(json_path)
}

# Set up DuckDB connection and extensions
con <- dbConnect(duckdb::duckdb())
dbExecute(con, "INSTALL json; LOAD json;")

# RECORDS MACRO
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

## PROPERTIES MACRO
dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS properties (
      source TEXT,
      item TEXT,
      prop TEXT,
      value TEXT,
      updated DATE
    );
  CREATE MACRO properties(path, source) AS TABLE
    SELECT 
      source AS source,
      parse_filename(item.value) AS item,
      parse_filename(prop.value) AS prop,
      value.value AS value,
      CAST(date.value AS DATE) AS updated
    FROM read_json_auto(path, maximum_object_size=1000000000)
    ORDER BY updated DESC;
")

## INSTANCES MACRO
dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS instances (
      source TEXT,
      p31 TEXT,
      p31_updated DATE,
      n_records INTEGER
    );
  CREATE MACRO instances(path, source) AS TABLE
    SELECT 
      source AS source, 
      parse_filename(p31.value) AS p31,
      CAST(p31_updated.value AS DATE) AS p31_updated,
      CAST(record_count.value AS INTEGER) AS n_records
    FROM read_json_auto(path, maximum_object_size=1000000000)
    ORDER BY n_records DESC;
")

## OCCUPATIONS MACRO
dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS occupations (
      source TEXT,
      prop TEXT,
      p106 TEXT,
      p106_updated DATE,
      n_credit INTEGER
    );
  CREATE MACRO occupations(path, source) AS TABLE
    SELECT 
      source AS source, 
      parse_filename(prop.value) AS prop,
      parse_filename(p106.value) AS p106,
      CAST(p106_updated.value AS DATE) AS p106_updated,
      CAST(credit_count.value AS INTEGER) AS n_credits
    FROM read_json_auto(path, maximum_object_size=1000000000);
")

## CREDIT-MEMBERS MACRO
dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS members (
      source TEXT,
      content TEXT,
      prop TEXT,
      grouped TEXT,
      grouped_updated DATE,
      member TEXT,
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

## CREDITS-BY-ROLE MACRO
dbExecute(con, "
  CREATE TEMP TABLE IF NOT EXISTS credits (
      source TEXT,
      prop TEXT,
      content TEXT,
      content_updated DATE,
      credit TEXT,
      credit_updated DATE
    );
  CREATE MACRO credits(path, source) AS TABLE
    SELECT 
      source AS source,
      parse_filename(prop.value) AS prop,
      parse_filename(content.value) AS content,
      CAST(content_updated.value AS DATE) AS content_updated,
      parse_filename(credit.value) AS credit,
      CAST(credit_updated.value AS DATE) AS credit_updated
    FROM read_json_auto(path, maximum_object_size=1000000000);
")