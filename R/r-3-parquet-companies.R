## companies: properties

files <- list.files("json/companies/properties", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS companies_properties (
      file TEXT,
      item TEXT,
      prop TEXT,
      value TEXT,
      updated DATE
    );

    INSERT INTO companies_properties
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
    COPY companies_properties 
    TO 'release/companies_properties.parquet' (FORMAT PARQUET)
")

## companies: instance_of

dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE companies_instances AS
    SELECT 
      parse_filename(unnest.p31.value) AS p31,
      unnest.p31_updated.value AS p31_updated,
      CAST(unnest.credit_count.value AS INTEGER) AS n_credits,
      CAST(unnest.content_count.value AS INTEGER) AS n_content
    FROM read_json_auto('json/companies/company-instances.json', maximum_object_size=1000000000),
      UNNEST(results.bindings)
    ORDER BY n_content DESC
")

dbExecute(con, "
    COPY companies_instances
    TO 'release/companies_instances.parquet' (FORMAT PARQUET)
")

## companies: credits

dbExecute(con, "
  CREATE OR REPLACE TEMP TABLE company_credits AS
  SELECT 
      parse_filename(unnest.prop.value) AS prop,
      parse_filename(unnest.content.value) AS content,
      unnest.content_updated.value AS content_updated,
      parse_filename(unnest.credit.value) AS credit,
      unnest.credit_updated.value AS credit_updated
  FROM read_json_auto('json/companies/company-credits.json', maximum_object_size=1000000000),
        UNNEST(results.bindings)
")

dbExecute(con, "
    COPY company_credits
    TO 'release/companies_credits.parquet' (FORMAT PARQUET)
")

## companies: records-by-instance

files <- list.files("json/companies/records-by-instance", pattern = "\\.json$", full.names = TRUE)

for (file in files) {
  dbExecute(con, sprintf(
    "
    CREATE TEMP TABLE IF NOT EXISTS companies_records (
      item TEXT,
      updated DATE
    );

    INSERT INTO companies_records
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
    COPY (SELECT DISTINCT item, updated FROM companies_records ORDER BY updated DESC)
    TO 'release/companies_records-by-instance.parquet' (FORMAT PARQUET)
")