files <-
  list.files(
    path = "SPARQL",
    pattern = ".sparql",
    recursive = T,
    full.names = T
  )

for (i in files) {
  filepath = str_remove(i, "SPARQL/")
  subpath = str_remove(filepath, basename(i))
  dir.create(
    path = paste0("json/", subpath),
    showWarnings = F,
    recursive = T
  )
  tryCatch({
    query <- URLencode(str_squish(str_replace_all(read_file(i), "[\r\n]" , " ")))
    download.file(
      url = paste0("https://query.wikidata.org/sparql?query=", query, "&format=json"),
      destfile = str_replace(paste0("json/", filepath), ".sparql", ".json"))
  }, error = function(err){})
}

