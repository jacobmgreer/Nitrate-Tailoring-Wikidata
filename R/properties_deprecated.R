source(file = "R/_shared.R")

# Locate all .sparql files
files <- list.files("SPARQL/properties/deprecated", pattern = "\\.sparql$", recursive = TRUE, full.names = TRUE)

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)

    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      INSERT INTO deprecated
        FROM deprecated('%s', '%s');
    ", json_path, str_remove(basename(file), ".sparql")))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY deprecated
  TO 'release/properties_deprecated.parquet' (FORMAT PARQUET);
")

# Disconnect from DuckDB
dbDisconnect(con)
message("All done. Outputs are in 'release/'.")