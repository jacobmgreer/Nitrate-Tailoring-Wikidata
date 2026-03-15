library(duckdb)
library(readr)
library(stringr)

options(timeout = 10*60)

source("R/r-1-pull-json.R")

con <- dbConnect(duckdb::duckdb())

dbExecute(con, "INSTALL json; LOAD json; SET preserve_insertion_order=false;")

dir.create(path = "release", showWarnings = F)

message("1/7 Parqueting: awards!")
source("R/r-2-parquet-awards.R")

message("2/7 Parqueting: companies!")
source("R/r-3-parquet-companies.R")

message("3/7 Parqueting: content!")
source("R/r-4-parquet-content.R")

message("4/7 Parqueting: events!")
source("R/r-5-parquet-events.R")

message("5/7 Parqueting: monitored!")
source("R/r-6-parquet-monitored.R")

message("6/7 Parqueting: people!")
source("R/r-7-parquet-people.R")

message("7/7 Parqueting: special")
source("R/r-8-parquet-special.R")