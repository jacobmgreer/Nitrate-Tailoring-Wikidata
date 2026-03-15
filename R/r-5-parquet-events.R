## events: records-by-instance

files <- list.files("json/events/records-by-instance", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS events_records (
      item TEXT,
      updated DATE
    );

    INSERT INTO events_records
    SELECT 
        parse_filename(unnest.item.value) AS item,
        unnest.date.value AS updated
    FROM read_json_auto('%s', maximum_object_size=1000000000),
         UNNEST(results.bindings)
    ",
    file
  ))
}

dbExecute(con, "
    COPY (SELECT DISTINCT item, updated FROM events_records ORDER BY updated DESC)
    TO 'release/events_records-by-instance.parquet' (FORMAT PARQUET)
")

## events: properties

files <- list.files("json/events/properties", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS events_properties (
      file TEXT,
      item TEXT,
      prop TEXT,
      value TEXT,
      updated DATE
    );

    INSERT INTO events_properties
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
    COPY events_properties 
    TO 'release/events_properties.parquet' (FORMAT PARQUET)
")

## events: instance_of

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE events_instances AS
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.p31.value) AS p31,
      unnest.p31_updated.value AS p31_updated,
      CAST(unnest.record_count.value AS INTEGER) AS n_records
    FROM read_json_auto('json/events/instances-of/*.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
    ORDER BY file, n_records DESC
")

dbExecute(con, "
    COPY events_instances
    TO 'release/events_instances.parquet' (FORMAT PARQUET)
")