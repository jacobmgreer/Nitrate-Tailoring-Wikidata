library(duckdb)
library(readr)
library(stringr)

options(timeout = 10*60)

source("R/r-1-pull-json.R")

con <- dbConnect(duckdb::duckdb())

dbExecute(con, "INSTALL json; LOAD json; SET preserve_insertion_order=false;")

dir.create(path = "release", showWarnings = F)

source("R/r-2-parquet-awards.R")
source("R/r-3-parquet-companies.R")
source("R/r-4-parquet-content.R")
source("R/r-5-parquet-events.R")
source("R/r-6-parquet-monitored.R")
source("R/r-7-parquet-people.R")
source("R/r-8-parquet-special.R")