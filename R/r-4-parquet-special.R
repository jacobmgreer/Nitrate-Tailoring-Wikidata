wd_intl_submissions <- 
  jsonlite::fromJSON("json/special_queries/wd_intl_submissions.json")$results$bindings %>%
  reframe(
    QID = basename(film$value),
    filmLabel = basename(filmLabel$value),
    submissionTitle = submission_title$value,
    year = year$value,
    countryQID = basename(country$value),
    countryLabel = countryLabel$value,
    ceremonyQID = basename(ceremony$value),
    ceremonyLabel = ceremonyLabel$value,
    imdb = imdb$value,
    letterboxd = letterboxd$value,
    eidr = eidr$value
  )

write_parquet(
  x = wd_intl_submissions, 
  sink = "release/special-wd_intl_submissions.parquet"
)

wd_languages <-
  jsonlite::fromJSON( "json/special_queries/wd_languages.json")$results$bindings %>%
  reframe(
    lang_QID = basename(item$value),
    language = wdlabelen$value,
    code = c$value
  )

write_parquet(
  x = wd_languages, 
  sink = "release/special-wd_languages.parquet"
)

wd_origins <-
  jsonlite::fromJSON( "json/special_queries/wd_origins.json")$results$bindings %>%
  reframe(
    QID = basename(item$value),
    shortCode = shortCode$value,
  )

write_parquet(
  x = wd_origins, 
  sink = "release/special-wd_origins.parquet"
)

wd_genres <-
  jsonlite::fromJSON( "json/special_queries/wd_genres.json")$results$bindings %>%
  reframe(
    QID = basename(item$value)
  )

write_parquet(
  x = wd_genres, 
  sink = "release/special-wd_genres.parquet"
)