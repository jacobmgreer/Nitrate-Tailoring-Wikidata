library(stringr)
library(readr)

files <- list.files(
  path = "SPARQL",
  pattern = ".sparql",
  recursive = TRUE,
  full.names = TRUE
)
for (i in files) {
  filepath = str_remove(i, "SPARQL/")
  subpath = str_remove(filepath, basename(i))
  dir.create(
    path = paste0("json/", subpath),
    showWarnings = FALSE,
    recursive = TRUE
  )
  tryCatch({
    # Read lines and strip comments
    query_lines <- read_lines(i)
    query_lines_nocomment <- str_replace(query_lines, "#.*$", "")
    # Drop empty lines and join
    query_text <- paste(query_lines_nocomment[query_lines_nocomment != ""], collapse = " ")
    query <- URLencode(str_squish(query_text))
    download.file(
      url = paste0("https://query.wikidata.org/sparql?query=", query, "&format=json"),
      destfile = str_replace(paste0("json/", filepath), ".sparql", ".json"))
  }, error = function(err) {
    message("Error downloading query file: ", i)
  })
}