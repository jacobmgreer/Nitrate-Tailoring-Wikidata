## special: wd_languages

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE wd_languages AS
    SELECT 
      parse_filename(unnest.item.value) AS item,
      unnest.c.value AS shortCode,
      unnest.wdlabelen.value AS label,
      unnest.date.value AS updated,
    FROM read_json_auto('json/special_queries/wd_languages.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
    ORDER BY shortCode
")

dbExecute(con, "
    COPY wd_languages
    TO 'release/wd_languages.parquet' (FORMAT PARQUET)
")

## special: wd_origins

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE wd_origins AS
    SELECT 
      parse_filename(unnest.origin.value) AS item,
      unnest.shortCode.value AS shortCode,
      unnest.date.value AS updated,
    FROM read_json_auto('json/special_queries/wd_origins.json', maximum_object_size=1000000000),
      UNNEST(results.bindings)
    WHERE item ~ 'Q[0-9]+$'
    ORDER BY shortCode
")

dbExecute(con, "
    COPY wd_origins
    TO 'release/wd_origins.parquet' (FORMAT PARQUET)
")