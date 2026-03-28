source(file = "R/_shared.R")

files <- c("SPARQL/special/wd_languages.sparql")

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)

    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      CREATE OR REPLACE TEMP TABLE languages AS
      SELECT 
        parse_filename(item.value) AS item,
        c.value AS shortCode,
        wdlabelen.value AS label,
        CAST(date.value AS DATE) AS updated,
      FROM read_json_auto('%s', maximum_object_size=1000000000)
      ORDER BY shortCode
    ", json_path))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY languages
  TO 'release/wd_languages.parquet' (FORMAT PARQUET)
")


files <- c("SPARQL/special/wd_origins.sparql")

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)

    message(json_path)
    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      CREATE OR REPLACE TEMP TABLE origins AS
      SELECT 
        parse_filename(origin.value) AS item,
        shortCode.value AS shortCode,
        CAST(date.value AS DATE) AS updated
      FROM read_json_auto('%s', maximum_object_size=1000000000)
      WHERE item ~ 'Q[0-9]+$'
      ORDER BY shortCode
    ", json_path))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY origins
  TO 'release/wd_origins.parquet' (FORMAT PARQUET)
")

files <- c("SPARQL/special/has_cast_credits.sparql")

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)

    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      CREATE OR REPLACE TEMP TABLE credits_cast AS
      SELECT 
        'special' AS source,
        'cast' AS property,
        parse_filename(content.value) AS content
      FROM read_json_auto('%s', maximum_object_size=1000000000)
    ", json_path))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY credits_cast
  TO 'release/has_cast_credits.parquet' (FORMAT PARQUET)
")

# Disconnect from DuckDB
dbDisconnect(con)
message("All done. Outputs are in 'release/'.")