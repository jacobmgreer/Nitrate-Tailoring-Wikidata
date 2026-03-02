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

wd_lang_list <-
  jsonlite::fromJSON( "json/special_queries/wd_lang_list.json")$results$bindings %>%
  reframe(
    lang_QID = basename(item$value),
    language = wdlabelen$value,
    code = c$value
  )

write_parquet(
  x = wd_lang_list, 
  sink = "release/special-wd_lang_list.parquet"
)