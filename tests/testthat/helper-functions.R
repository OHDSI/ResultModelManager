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

createResultsDataModel <-
  function(connection = NULL,
           connectionDetails = NULL,
           schema,
           sql) {
    if (is.null(connection)) {
      if (!is.null(connectionDetails)) {
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
      } else {
        stop("No connection or connectionDetails provided.")
      }
    }
    schemas <- unlist(
      DatabaseConnector::querySql(
        connection,
        "SELECT schema_name FROM information_schema.schemata;",
        snakeCaseToCamelCase = TRUE
      )[, 1]
    )
    if (!tolower(schema) %in% tolower(schemas)) {
      stop(
        "Schema '",
        schema,
        "' not found on database. Only found these schemas: '",
        paste(schemas, collapse = "', '"),
        "'"
      )
    }
    DatabaseConnector::executeSql(
      connection,
      sprintf("SET search_path TO %s;", schema),
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    DatabaseConnector::executeSql(connection, sql)
  }
