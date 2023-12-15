# get test results data model specifications data from the csv file in this package
getResultsDataModelSpecifications <- function() {
  pathToCsv <- file.path("settings", "resultsDataModelSpecification.csv")
  resultsDataModelSpecifications <-
    readr::read_csv(file = pathToCsv, col_types = readr::cols())
  colnames(resultsDataModelSpecifications) <- SqlRender::snakeCaseToCamelCase(colnames(resultsDataModelSpecifications))
  return(resultsDataModelSpecifications)
}

# get test results data model database tables creation SQL code from the SQL file in this package
getResultsDataModelCreationSql <- function() {
  pathToSql <- file.path("sql", "CreateResultsDataModel.sql")
  getResultsDataModelCreationSql <- readr::read_file(pathToSql)
  return(getResultsDataModelCreationSql)
}

test_that("results tables are created", {
  skip_if_results_db_not_available()
  createResultsDataModel(
    connectionDetails = testDatabaseConnectionDetails,
    schema = testSchema,
    sql = getResultsDataModelCreationSql()
  )

  specifications <- getResultsDataModelSpecifications()

  # Only works with postgres > 9.4
  .tableExists <- function(connection, schema, tableName) {
    return(!is.na(
      DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = "SELECT to_regclass('@schema.@table');",
        table = tableName,
        schema = schema
      )
    )[[1]])
  }

  for (tableName in unique(specifications$tableName)) {
    expect_true(
      .tableExists(
        connection = testDatabaseConnection,
        schema = testSchema,
        tableName = tableName
      )
    )
  }
  # Bad schema name
  expect_error(
    createResultsDataModel(connection = testDatabaseConnection, schema = "non_existant_schema")
  )
})


test_that("results are uploaded", {
  skip_if_results_db_not_available()
  specifications <- getResultsDataModelSpecifications()

  pathToZip1 <-
    file.path("testdata", "testzip1.zip")
  pathToZip2 <-
    file.path("testdata", "testzip2.zip")
  listOfZipFilesToUpload <- c(pathToZip1, pathToZip2)

  tempDir <- tempfile()

  for (i in seq_len(length(listOfZipFilesToUpload))) {
    unzipResults(
      zipFile = listOfZipFilesToUpload[[i]],
      resultsFolder = tempDir
    )

    uploadResults(
      connectionDetails = testDatabaseConnectionDetails,
      schema = testSchema,
      databaseIdentifierFile = "database.csv",
      resultsFolder = tempDir,
      specifications = specifications,
      purgeSiteDataBeforeUploading = FALSE,
      runCheckAndFixCommands = TRUE,
      warnOnMissingTable = FALSE
    )
    # Repeat to purge data
    uploadResults(
      connectionDetails = testDatabaseConnectionDetails,
      schema = testSchema,
      databaseIdentifierFile = "database.csv",
      resultsFolder = tempDir,
      specifications = specifications,
      purgeSiteDataBeforeUploading = i != 2,
      runCheckAndFixCommands = TRUE,
      forceOverWriteOfSpecifications = i == 2,
      warnOnMissingTable = FALSE
    )
    unlink(x = tempDir, recursive = TRUE, force = TRUE)
  }

  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(tableName == !!tableName &
        primaryKey == "Yes") %>%
      dplyr::select("columnName") %>%
      dplyr::pull()

    if ("database_id" %in% primaryKey) {
      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
      sql <- SqlRender::render(
        sql = sql,
        schema = testSchema,
        table_name = tableName,
        database_id = "test1"
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection = testDatabaseConnection, sql = sql)[, 1]
      expect_true(databaseIdCount > 0)
    }
  }

  # Test uploading after truncate
  uploadResults(
    connectionDetails = testDatabaseConnectionDetails,
    schema = testSchema,
    databaseIdentifierFile = "database.csv",
    resultsFolder = tempDir,
    specifications = specifications,
    purgeSiteDataBeforeUploading = FALSE,
    purgeDataModel = TRUE,
    runCheckAndFixCommands = TRUE,
    warnOnMissingTable = FALSE
  )

  expect_false(.removeDataUserCheck("N"))
  expect_false(.removeDataUserCheck("n"))
  expect_false(.removeDataUserCheck(""))
  expect_true(.removeDataUserCheck("Y"))
  expect_true(.removeDataUserCheck("y"))
})


test_that("appending results rows using primary keys works", {
  skip_if_results_db_not_available()
  specifications <- getResultsDataModelSpecifications()

  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(tableName == !!tableName &
        primaryKey == "Yes") %>%
      dplyr::select("columnName") %>%
      dplyr::pull()

    # append new data into table
    if (("database_id" %in% primaryKey) &&
      ("analysis3_id" %in% primaryKey)) {
      # read 2 rows of test data
      csvFilePathName <- file.path("testdata", "test_table_3.csv")
      data <- readr::read_csv(
        file = csvFilePathName,
        col_types = readr::cols(),
        progress = FALSE
      )

      # read 3 rows of test data to append
      # one row is a duplicate of one of the above two test data rows
      csvFilePathName <- file.path("testdata", "test_table_4.csv")
      newData <- readr::read_csv(
        file = csvFilePathName,
        col_types = readr::cols(),
        progress = FALSE
      )

      mergedData <- appendNewRows(
        data = data,
        newData = newData,
        tableName = tableName,
        specifications = specifications
      )

      # verify that the duplicate row was not appended (only the single existing row remains)
      rowCount <- mergedData %>%
        dplyr::filter(analysis3_id == "6542456") %>%
        dplyr::count() %>%
        dplyr::pull()
      expect_true(rowCount == 1)

      # verify that the two new rows were appended
      rowCount <- mergedData %>%
        dplyr::filter(analysis3_id == "3453111") %>%
        dplyr::count() %>%
        dplyr::pull()
      expect_true(rowCount == 2)
    }
  }
})

test_that("deleting results rows using data primary key works", {
  skip_if_results_db_not_available()
  specifications <- getResultsDataModelSpecifications()

  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(tableName == !!tableName &
        primaryKey == "Yes") %>%
      dplyr::select("columnName") %>%
      dplyr::pull()

    # delete rows from tables with primary key: database_id, analysis3_id
    if (("database_id" %in% primaryKey) &&
      ("analysis3_id" %in% primaryKey)) {
      deleteAllRowsForPrimaryKey(
        connection = testDatabaseConnection,
        schema = testSchema,
        tableName = tableName,
        keyValues = dplyr::tibble(
          database_id = "test1", analysis3_id =
            "6542456"
        )
      )

      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id' AND analysis3_id = @analysis3_id;"
      sql <- SqlRender::render(
        sql = sql,
        schema = testSchema,
        table_name = tableName,
        database_id = "test1",
        analysis3_id = "6542456"
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection = testDatabaseConnection, sql = sql)[, 1]
      expect_true(databaseIdCount == 0)
    }
  }
})

test_that("deleting results rows by database id works", {
  skip_if_results_db_not_available()
  specifications <- getResultsDataModelSpecifications()

  for (tableName in unique(specifications$tableName)) {
    primaryKey <- specifications %>%
      dplyr::filter(tableName == !!tableName &
        primaryKey == "Yes") %>%
      dplyr::select("columnName") %>%
      dplyr::pull()

    if ("database_id" %in% primaryKey) {
      deleteAllRowsForDatabaseId(
        connection = testDatabaseConnection,
        schema = testSchema,
        tableName = tableName,
        databaseId = "test2",
        idIsInt = FALSE
      )

      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
      sql <- SqlRender::render(
        sql = sql,
        schema = testSchema,
        table_name = tableName,
        database_id = "test2"
      )
      databaseIdCount <-
        DatabaseConnector::querySql(connection = testDatabaseConnection, sql = sql)[, 1]
      expect_true(databaseIdCount == 0)
    }
  }
})

test_that("checkAndFixColumnNames works correctly", {
  df <- data.frame(a = 1:3, b = 4:6, c = 7:9)
  specs <- data.frame(tableName = "test", columnName = c("a", "b", "c"), optional = c("no", "no", "no"))
  expect_equal(checkAndFixColumnNames(df, "test", ".", specs), df)
  df2 <- df[, -1] # Remove column 'a'
  expect_error(checkAndFixColumnNames(df2, "test", ".", specs), "Column names of table test in results folder . do not match specifications.")
  specs$optional[1] <- "yes" # Make column 'a' optional
  expect_equal(checkAndFixColumnNames(df2, "test", ".", specs), df2)
})
