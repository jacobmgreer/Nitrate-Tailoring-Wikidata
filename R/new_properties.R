library(readr)
library(dplyr)
library(stringr)
library(tidyr)

props_df <- read_tsv("SPARQL/properties/_properties.tsv")

# Remove disabled properties
props_df <- props_df %>% filter(is.na(disabled) | disabled != 1)

# # Group props for combined (category) queries
# runs_categories <- props_df %>%
#   filter(query_type != "individual") %>%
#   group_by(table, category) %>%
#   reframe(
#     property = paste(property, collapse=" ")
#   )

runs_individual <- props_df %>%
  ##filter(query_type == "individual") %>%
  reframe(table, category, property)

runs <- bind_rows(runs_individual)

# Output a structured list for scripting
plan <- list(
  runs = split(runs, runs$table)
)

# Generates a SPARQL query for a set of properties
generate_sparql_query <- function(props) {
  sprintf(
    "SELECT ?item ?date WHERE {\n  VALUES ?prop { %s }\n  ?item ?prop [].\n  ?item schema:dateModified ?date.\n}", props
  )
}

base_dir <- "SPARQL/properties/generated"
unlink(base_dir, force = T, recursive = T)
dir.create(base_dir, showWarnings = FALSE, recursive = TRUE)

# Iterate over each table group to create subfolders and files
for (table_name in names(plan$runs)) {
  table_dir <- file.path(base_dir, table_name)
  dir.create(table_dir, showWarnings = FALSE, recursive = TRUE)

  table_batches <- plan$runs[[table_name]]
  for (i in seq_len(nrow(table_batches))) {
    batch <- table_batches[i, ]
    props <- unlist(str_split(batch$property, " "))
    sparql_query <- generate_sparql_query(paste(props, collapse = " "))
    sparql_file <- sprintf("%s/%s__%s.sparql", table_dir, batch$category, str_remove(batch$property, "wdt:"))
    writeLines(sparql_query, sparql_file)
  }
}