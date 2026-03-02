files <-
  list.files(
    path = "json/properties",
    pattern = ".json",
    recursive = T,
    full.names = T
  )

for (i in files) {
  tryCatch({
    write_parquet(
      x = 
        jsonlite::fromJSON(i)$results$bindings %>%
        reframe(
          QID = basename(item$value),
          value = value$value,
          updated = date$value
        ), 
      sink = paste0("release/property-", str_replace(basename(i), ".json", ".parquet"))
    )
  }, error = function(err){})
}