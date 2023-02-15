getTestConnectionDetails <- function() {
  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "postgresql",
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = utils::URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD")),
    server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
    pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
  )
}


skip_if_results_db_not_available <- function() {
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "", message = "results db not available")
}
