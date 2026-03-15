## monitoring properties: imdb

dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE monitoring_imdb AS
  SELECT 
    parse_filename(unnest.item.value) AS item,
    parse_filename(unnest.prop.value) AS item,
    unnest.value.value AS value,
    unnest.date.value AS updated,
  FROM read_json_auto('json/monitoring-properties/imdb.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
  ORDER BY updated DESC
")

dbExecute(con, "
    COPY monitoring_imdb
    TO 'release/monitoring_imdb.parquet' (FORMAT PARQUET)
")

dbExecute(con, "
    COPY (
      SELECT 
        SUBSTR(value, 1, 2) AS prefix,
        COUNT(*) AS n
      FROM monitoring_imdb
      GROUP BY prefix
      ORDER BY n DESC
    )
    TO 'release/monitoring_imdb_prefixes.csv' (FORMAT CSV)
")

dbExecute(con, "
    COPY (
      SELECT *
      FROM monitoring_imdb
      WHERE SUBSTR(value, 1, 2) NOT IN ('tt', 'nm', 'ev', 'co', 'ch', 'li', 'ni')
      ORDER BY updated DESC
    )
    TO 'release/monitoring_imdb_awkward.csv' (FORMAT CSV)
")

## monitoring properties: youtube

dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE monitoring_youtube AS
  SELECT 
    parse_filename(unnest.item.value) AS item,
    parse_filename(unnest.prop.value) AS item,
    unnest.value.value AS value,
    unnest.date.value AS updated,
  FROM read_json_auto('json/monitoring-properties/youtube.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
  ORDER BY updated DESC
")

dbExecute(con, "
    COPY monitoring_youtube
    TO 'release/monitoring_youtube.parquet' (FORMAT PARQUET)
")

## monitoring properties: social-meta

dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE monitoring_social_meta AS
  SELECT 
    parse_filename(unnest.item.value) AS item,
    parse_filename(unnest.prop.value) AS item,
    unnest.value.value AS value,
    unnest.date.value AS updated,
  FROM read_json_auto('json/monitoring-properties/social-meta.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
  ORDER BY updated DESC
")

dbExecute(con, "
    COPY monitoring_social_meta
    TO 'release/monitoring_social-meta.parquet' (FORMAT PARQUET)
")

## monitoring properties: social-other

dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE monitoring_social_other AS
  SELECT 
    parse_filename(unnest.item.value) AS item,
    parse_filename(unnest.prop.value) AS item,
    unnest.value.value AS value,
    unnest.date.value AS updated,
  FROM read_json_auto('json/monitoring-properties/social-other.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
  ORDER BY updated DESC
")

dbExecute(con, "
    COPY monitoring_social_other
    TO 'release/monitoring_social-other.parquet' (FORMAT PARQUET)
")