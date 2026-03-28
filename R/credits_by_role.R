source(file = "R/_shared.R")

# Locate all .sparql files
files <- list.files("SPARQL/credited/role", pattern = "\\.sparql$", recursive = TRUE, full.names = TRUE)

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)
    fns = str_split(basename(file), "__", simplify = TRUE)
    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      INSERT INTO credits
        FROM credits('%s', '%s', '%s');
    ", json_path, fns[,1], str_remove(fns[,2], ".sparql")))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY credits
  TO 'release/credited_by_role.parquet' (FORMAT PARQUET);
")

# Disconnect from DuckDB
dbDisconnect(con)
message("All done. Outputs are in 'release/'.")