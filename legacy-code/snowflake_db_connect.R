
library(dplyr, warn.conflicts = FALSE)
library(dbplyr, warn.conflicts = FALSE)
#usethis::edit_r_environ()
sort(unique(odbc::odbcListDrivers()$name))
#odbc::odbcListDataSources()
.snowflake <- DBI::dbConnect(
  odbc::odbc(),
  Driver = Sys.getenv("SNOWFLAKE_DRV"),
  Server = Sys.getenv("SNOWFLAKE_SERVER"),
  UID = Sys.getenv("SNOWFLAKE_UID"),
  PWD = Sys.getenv("SNOWFLAKE_PWD"),
  Database = "ANALYTICS"
)
# example
