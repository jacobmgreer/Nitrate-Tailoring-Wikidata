## awards: instance_of

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE awards_instances AS
    SELECT 
      parse_filename(unnest.p31.value) AS p31,
      unnest.p31_updated.value AS p31_updated,
      CAST(unnest.nominee_count.value AS INTEGER) AS n_nominees
    FROM read_json_auto('json/awards/award-instance_of.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
    ORDER BY n_nominees DESC
")

dbExecute(con, "
    COPY awards_instances
    TO 'release/awards_instances.parquet' (FORMAT PARQUET)
")

## awards: nominees

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE awards_nominees AS
    SELECT 
      parse_filename(unnest.nominee.value) AS nominee,
      unnest.nominee_updated.value AS nominee_updated,
      parse_filename(unnest.award.value) AS award,
      unnest.award_updated.value AS award_updated,
    FROM read_json_auto('json/awards/award-nominee.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
")

dbExecute(con, "
    COPY awards_nominees
    TO 'release/awards_nominees.parquet' (FORMAT PARQUET)
")

## awards: international feature submissions

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE awards_international_submissions AS
    SELECT 
      parse_filename(unnest.film.value) AS film,
      unnest.filmLabel.value AS filmLabel,
      unnest.submission_title.value AS submissionTitle,
      CAST(unnest.year.value AS INTEGER) AS releaseYear,
      parse_filename(unnest.country.value) AS country,
      unnest.countryLabel.value AS countryLabel,
      parse_filename(unnest.ceremony.value) AS ceremony,
      unnest.ceremonyLabel.value AS ceremonyLabel,
      unnest.imdb.value AS imdb,
      unnest.letterboxd.value AS letterboxd,
      unnest.eidr.value AS eidr
    FROM read_json_auto('json/awards/award-international_feature_submissions.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
    ORDER BY ceremonyLabel, country
")

dbExecute(con, "
    COPY awards_international_submissions
    TO 'release/awards_AMPAS_international_feature_submissions.parquet' (FORMAT PARQUET)
")