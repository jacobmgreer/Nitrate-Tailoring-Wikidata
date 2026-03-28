source(file = "R/_shared.R")

# Locate all .sparql files
files <- list.files("SPARQL/properties/imdb", pattern = "\\.sparql$", recursive = TRUE, full.names = TRUE)

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)
    fns = str_split(basename(file), "__", simplify = TRUE)
    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      INSERT INTO properties
        FROM properties('%s', '%s', '%s');
    ", json_path, fns[,1], str_remove(fns[,2], ".sparql")))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY properties
  TO 'release/properties_imdb.parquet' (FORMAT PARQUET);
")

dbExecute(con, "
  COPY (
    SELECT 
      property AS prefix,
      COUNT(*) AS n
    FROM properties
    GROUP BY property
    ORDER BY n DESC
  )
  TO 'release/monitoring_imdb_prefixes.csv' (FORMAT CSV)
")

# Disconnect from DuckDB
dbDisconnect(con)
message("All done. Outputs are in 'release/'.")