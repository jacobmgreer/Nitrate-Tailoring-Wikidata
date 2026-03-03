lapply(
  c("utils", "readr", "stringr", "jsonlite", "dplyr", "tidyr", "arrow"), 
  require, character.only = TRUE)
options(timeout = 10*60)

dir.create(path = "release", showWarnings = F)

source("R/r-1-pull-json.R")
source("R/r-2-parquet-monitoring.R")
source("R/r-3-parquet-properties.R")
source("R/r-4-parquet-credits.R")
source("R/r-5-parquet-imdb.R")
source("R/r-6-parquet-instances.R")
source("R/r-7-parquet-special.R")