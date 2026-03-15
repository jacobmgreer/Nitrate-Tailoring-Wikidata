## content: properties - screen

files <- list.files("json/content/properties-screen", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS content_properties_screen (
      file TEXT,
      item TEXT,
      prop TEXT,
      value TEXT,
      updated DATE
    );

    INSERT INTO content_properties_screen
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.item.value) AS item,
      parse_filename(unnest.prop.value) AS prop,
      parse_filename(unnest.value.value) AS value,
      unnest.date.value AS updated
    FROM read_json_auto('%s', maximum_object_size=1000000000),
      UNNEST(results.bindings)
    ORDER BY file, updated DESC
    ",
    file
  ))
}

dbExecute(con, "
    COPY content_properties_screen
    TO 'release/content_screen_properties.parquet' (FORMAT PARQUET)
")

## content: properties - music

files <- list.files("json/content/properties-music", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS content_properties_music (
      file TEXT,
      item TEXT,
      prop TEXT,
      value TEXT,
      updated DATE
    );

    INSERT INTO content_properties_music
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.item.value) AS item,
      parse_filename(unnest.prop.value) AS prop,
      parse_filename(unnest.value.value) AS value,
      unnest.date.value AS updated
    FROM read_json_auto('%s', maximum_object_size=1000000000),
      UNNEST(results.bindings)
    ORDER BY file, updated DESC
    ",
    file
  ))
}

dbExecute(con, "
    COPY content_properties_music
    TO 'release/content_music_properties.parquet' (FORMAT PARQUET)
")

## content: records-by-instance films
dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE films_records AS
    SELECT 
      parse_filename(unnest.item.value) AS item,
      unnest.date.value AS updated,
    FROM read_json_auto('json/content/records-by-instance/media-films.json', maximum_object_size=1000000000),
      UNNEST(results.bindings)
    ORDER BY updated DESC
")

dbExecute(con, "
    COPY films_records
    TO 'release/content_screen_records-by-instance.parquet' (FORMAT PARQUET)
")

## content: records-by-instance albums
dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE albums_records AS
    SELECT 
      parse_filename(unnest.item.value) AS item,
      unnest.date.value AS updated,
    FROM read_json_auto('json/content/records-by-instance/media-albums.json', maximum_object_size=1000000000),
      UNNEST(results.bindings)
    ORDER BY updated DESC
")

dbExecute(con, "
    COPY albums_records
    TO 'release/content_music_records-by-instance.parquet' (FORMAT PARQUET)
")

## content: credits-instances

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE content_credits_instances AS
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.p31.value) AS p31,
      unnest.p31_updated.value AS p31_updated,
      CAST(unnest.credit_count.value AS INTEGER) AS n_credit,
      CAST(unnest.content_count.value AS INTEGER) AS n_content,
    FROM read_json_auto('json/content/credits-instances/*.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
")

dbExecute(con, "
    COPY content_credits_instances
    TO 'release/content_credits-instances.parquet' (FORMAT PARQUET)
")

## content: instances-of

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE content_instances AS
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.p31.value) AS p31,
      unnest.p31_updated.value AS p31_updated,
      CAST(unnest.record_count.value AS INTEGER) AS n_records
    FROM read_json_auto('json/content/instances-of/*.json'),
      UNNEST(results.bindings)
    ORDER BY file, n_records DESC
")

dbExecute(con, "
    COPY content_instances 
    TO 'release/content_instances.parquet' (FORMAT PARQUET)
")