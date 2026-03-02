files <-
  list.files(
    path = "json/media",
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
          updated = date$value
        ), 
      sink = paste0("release/instances-", str_replace(basename(i), ".json", ".parquet"))
    )
  }, error = function(err){})
}