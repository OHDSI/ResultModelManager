library(testthat)

sqliteFile <- tempfile(fileext = "sqlite")
.varFile <- paste0(".SQLITE_PATH", Sys.getpid())
unlink(.varFile)

writeLines(sqliteFile, .varFile)
unlink(sqliteFile)
withr::defer(
  {
    unlink(sqliteFile)
    unlink(.varFile)
  },
  testthat::teardown_env()
)

connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = readLines(.varFile)
)
connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::disconnect(connection)
options(rstudio.connectionObserver.errorsSuppressed = TRUE)
options(connectionObserver = NULL)


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


if (Sys.getenv("CDM5_POSTGRESQL_SERVER") != "") {
  testSchema <- paste0("rmm", Sys.getpid(), gsub("[: -]", "", format(Sys.time(), "%s"), perl = TRUE), sample(1:100, 1))
  testDatabaseConnectionDetails <- getTestConnectionDetails()
  testDatabaseConnection <- DatabaseConnector::connect(testDatabaseConnectionDetails)

  sql <- "
    CREATE SCHEMA IF NOT EXISTS test_log;
    CREATE TABLE IF NOT EXISTS test_log.test_schema_creation (
      id SERIAL PRIMARY KEY,
      schema_name varchar(255) UNIQUE,
      created_at TIMESTAMPTZ DEFAULT Now()
    );

    INSERT INTO test_log.test_schema_creation (schema_name) VALUES ('@testSchema');
    CREATE SCHEMA @testSchema;"

  DatabaseConnector::renderTranslateExecuteSql(
    sql = sql,
    testSchema = testSchema,
    connection = testDatabaseConnection
  )

  withr::defer(
    {
      testDatabaseConnectionCleanup <- DatabaseConnector::connect(testDatabaseConnectionDetails)
      sql <- "
    DROP SCHEMA IF EXISTS @testSchema CASCADE;
    DELETE FROM test_log.test_schema_creation WHERE schema_name = '@testSchema';
    "
      DatabaseConnector::renderTranslateExecuteSql(
        sql = sql,
        testSchema = testSchema,
        connection = testDatabaseConnectionCleanup
      )

      DatabaseConnector::disconnect(testDatabaseConnectionCleanup)
      DatabaseConnector::disconnect(testDatabaseConnection)
    },
    testthat::teardown_env()
  )
}
