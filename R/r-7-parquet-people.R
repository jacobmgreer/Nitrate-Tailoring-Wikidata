## people: instances-of

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE people_instances AS
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.prop.value) AS prop,
      parse_filename(unnest.p31.value) AS p31,
      unnest.p31_updated.value AS p31_updated,
      CAST(unnest.record_count.value AS INTEGER) AS n_records
    FROM read_json_auto('json/people/instances-of/*.json'),
    UNNEST(results.bindings)
")

dbExecute(con, "
    COPY people_instances 
    TO 'release/people_instances.parquet' (FORMAT PARQUET)
")

## people: properties

files <- list.files("json/people/properties", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS people_properties (
      file TEXT,
      item TEXT,
      prop TEXT,
      value TEXT,
      updated DATE
    );

    INSERT INTO people_properties
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
    COPY people_properties 
    TO 'release/people_properties.parquet' (FORMAT PARQUET)
")

## people: records-by-instance

files <- list.files("json/people/records-by-instance", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS people_records (
      item TEXT,
      updated DATE
    );

    INSERT INTO people_records
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
    COPY (SELECT DISTINCT item, updated FROM people_records ORDER BY updated DESC)
    TO 'release/people_records-by-instance.parquet' (FORMAT PARQUET)
")

## people: credits-membered

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE people_membered AS
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.prop.value) AS prop,
      parse_filename(unnest.group.value) AS group,
      unnest.group_updated.value AS group_updated,
      parse_filename(unnest.associated.value) AS associated,
      unnest.associated_updated.value AS associated_updated,
    FROM read_json_auto('json/people/credits-membered/*.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
")

dbExecute(con, "
    COPY people_membered 
    TO 'release/people_credits-membered.parquet' (FORMAT PARQUET)
")

## people: credits-occupations

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE people_occupations AS
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.prop.value) AS prop,
      parse_filename(unnest.p106.value) AS p106,
      unnest.p106_updated.value AS p106_updated,
      unnest.credit_count.value AS n_credits
    FROM read_json_auto('json/people/credits-occupations/*.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
")

dbExecute(con, "
    COPY people_occupations 
    TO 'release/people_credits-occupations.parquet' (FORMAT PARQUET)
")

## people: credits-instances

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE people_credits_instances AS
    SELECT 
      parse_filename(filename, true) AS file, 
      parse_filename(unnest.prop.value) AS prop,
      parse_filename(unnest.p31.value) AS p31,
      unnest.p31_updated.value AS p31_updated,
      CAST(unnest.credit_count.value AS INTEGER) AS n_credit,
      CAST(unnest.content_count.value AS INTEGER) AS n_content,
    FROM read_json_auto('json/people/credits-instances/*.json', maximum_object_size=1000000000),
    UNNEST(results.bindings)
")

dbExecute(con, "
    COPY people_credits_instances
    TO 'release/people_credits-instances.parquet' (FORMAT PARQUET)
")

## people: credits-by-role

files <- list.files("json/people/credits-by-role", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS people_credits_by_role (
      prop TEXT,
      content TEXT,
      content_updated DATE,
      credit TEXT,
      credit_updated DATE
    );

    INSERT INTO people_credits_by_role
    SELECT 
        parse_filename(unnest.prop.value) AS prop,
        parse_filename(unnest.content.value) AS content,
        unnest.content_updated.value AS content_updated,
        parse_filename(unnest.credit.value) AS credit,
        unnest.credit_updated.value AS credit_updated
    FROM read_json_auto('%s', maximum_object_size=1000000000),
         UNNEST(results.bindings)
    ",
    file
  ))
}

dbExecute(con, "
    COPY people_credits_by_role
    TO 'release/people_credits-by-role.parquet' (FORMAT PARQUET)
")