library(testthat)

sqliteFile <- tempfile(fileext = "sqlite")
.varFile <- ".SQLITE_PATH"
unlink(.varFile)

writeLines(sqliteFile, .varFile)
unlink(sqliteFile)
withr::defer({
  unlink(sqliteFile)
  unlink(.varFile)
}, testthat::teardown_env())

connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite",
                                                                server = readLines(".SQLITE_PATH"))
connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::disconnect(connection)
options(rstudio.connectionObserver.errorsSuppressed = TRUE)

testSchema <- Sys.getenv("CDM5_POSTGRESQL_OHDSI_SCHEMA")

if (dir.exists(Sys.getenv("DATABASECONNECTOR_JAR_FOLDER"))) {
  jdbcDriverFolder <- Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
} else {
  jdbcDriverFolder <- "~/.jdbcDrivers"
  Sys.setenv(DATABASECONNECTOR_JAR_FOLDER = jdbcDriverFolder)
  dir.create(jdbcDriverFolder, showWarnings = FALSE)
  DatabaseConnector::downloadJdbcDrivers("postgresql", pathToDriver = jdbcDriverFolder)
  withr::defer(
  {
    unlink(jdbcDriverFolder, recursive = TRUE, force = TRUE)
  },
    testthat::teardown_env()
  )
}
