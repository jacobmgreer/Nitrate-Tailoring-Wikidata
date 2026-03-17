source(file = "R/_shared.R")

files <- list.files("SPARQL/awards/nominees", pattern = "\\.sparql$", recursive = TRUE, full.names = TRUE)

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)

    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      CREATE OR REPLACE TEMP TABLE awards_nominees AS
      SELECT 
        CAST(parse_filename(nominee.value) AS VARCHAR) AS nominee,
        CAST(nominee_updated.value AS DATE) AS nominee_updated,
        CAST(parse_filename(award.value) AS VARCHAR) AS award,
        CAST(award_updated.value AS DATE) AS award_updated
      FROM read_json_auto('%s', maximum_object_size=1000000000)
    ", json_path))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY awards_nominees
  TO 'release/awards_nominees.parquet' (FORMAT PARQUET)
")



files <- c("SPARQL/awards/award-AMPAS_international_submissions.sparql")

# Process each SPARQL file
for (file in files) {
  tryCatch({
    # 1. Process and fetch the SPARQL query, saving JSON data
    json_path <- process_sparql(file)

    # 2. Insert the JSON data into the DuckDB `records` table
    dbExecute(con, sprintf("
      CREATE OR REPLACE TEMP TABLE awards_international_submissions AS
      SELECT 
        CAST(parse_filename(film.value) AS VARCHAR) AS film,
        CAST(filmLabel.value AS TEXT) AS filmLabel,
        CAST(submission_title.value AS TEXT) AS submissionTitle,
        CAST(year.value AS INTEGER) AS releaseYear,
        CAST(parse_filename(country.value) AS TEXT) AS country,
        CAST(countryLabel.value AS TEXT) AS countryLabel,
        CAST(parse_filename(ceremony.value) AS TEXT) AS ceremony,
        CAST(ceremonyLabel.value AS TEXT) AS ceremonyLabel,
        CAST(imdb.value AS TEXT) AS imdb,
        CAST(letterboxd.value AS TEXT) AS letterboxd,
        CAST(eidr.value AS TEXT) eidr
      FROM read_json_auto('%s', maximum_object_size=1000000000)
      ORDER BY ceremonyLabel, country
    ", json_path))

  }, error = function(e) {
    message(sprintf("Error processing file %s: %s", file, e$message))
  })
}

# Export results as a Parquet file
dbExecute(con, "
  COPY awards_international_submissions
  TO 'release/awards_AMPAS_international_feature_submissions.parquet' (FORMAT PARQUET)
")



# Disconnect from DuckDB
dbDisconnect(con)
message("All done. Outputs are in 'release/'.")