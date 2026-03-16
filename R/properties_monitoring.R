source(file = "R/_shared.R")

# Locate all .sparql files
files <- list.files("SPARQL/properties/monitoring", pattern = "\\.sparql$", recursive = TRUE, full.names = TRUE)

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)

    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      INSERT INTO properties
        FROM properties('%s', '%s');
    ", json_path, substr(basename(file), 1, 2)))

    # Optional: Delete the temporary JSON file after processing
    unlink(json_path, force = TRUE)
  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY properties
  TO 'release/properties_monitoring.parquet' (FORMAT PARQUET);
")

dbExecute(con, "
  COPY (
    SELECT 
      SUBSTR(value, 1, 2) AS prefix,
      COUNT(*) AS n
    FROM properties
    WHERE source = 'imdb'
    GROUP BY prefix
    ORDER BY n DESC
  )
  TO 'release/monitoring_imdb_prefixes.csv' (FORMAT CSV)
")

dbExecute(con, "
  COPY (
    SELECT *
    FROM properties
    WHERE 
      source = 'imdb' AND
      SUBSTR(value, 1, 2) NOT IN ('tt', 'nm', 'ev', 'co', 'ch', 'li', 'ni')
    ORDER BY updated DESC
  )
  TO 'release/monitoring_imdb_awkward.csv' (FORMAT CSV)
")

# Disconnect from DuckDB
dbDisconnect(con)
message("All done. Outputs are in 'release/'.")