files <-
  list.files(
    path = "json/credits/person",
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
          content = basename(content$value),
          content_updated = content_updated$value,
          person = basename(person$value),
          person_updated = person_updated$value
        ), 
      sink = paste0("release/credit-", str_replace(basename(i), ".json", ".parquet"))
    )
  }, error = function(err){})
}

files <-
  list.files(
    path = "json/credits/companies",
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
          content = basename(content$value),
          content_updated = content_updated$value,
          company = basename(company$value),
          company_updated = company_updated$value
        ), 
      sink = paste0("release/credit-", str_replace(basename(i), ".json", ".parquet"))
    )
  }, error = function(err){})
}


files <-
  list.files(
    path = "json/credits/awards",
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
          nominee = basename(nominee$value),
          nominee_updated = nominee_updated$value,
          award = basename(award$value),
          award_updated = award_updated$value
        ), 
      sink = paste0("release/credit-", str_replace(basename(i), ".json", ".parquet"))
    )
  }, error = function(err){})
}