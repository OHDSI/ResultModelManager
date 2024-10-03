test_that("test python postgres connection works", {
  skip_on_cran()
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "")
  # Should not throw error
  enablePythonUploads()
  expect_true(pyPgUploadEnabled())


  pyConnection <- .createPyConnection(testDatabaseConnection)
  on.exit(pyConnection$close(), add = TRUE)
  curr <- pyConnection$cursor()
  on.exit(curr$close(), add = TRUE)
  curr$execute("SELECT 1")
  res <- curr$fetchone()
  expect_equal(res[[1]], 1)
})


test_that("test python upload table from csv works", {
  skip_on_cran()
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "")
  enablePythonUploads()
  tfile <- tempfile(fileext = ".csv")
  pyConnection <- .createPyConnection(testDatabaseConnection)
  on.exit({
    pyConnection$close()
    unlink(tfile)
  }, add = TRUE)

  table <- paste0("test_", sample(1:10000, 1))
  # create csv
  testData <- data.frame(id = 1:10, test_string = 'some crazy vaLUEs;;a,.\t\n∑åˆø')
  readr::write_csv(testData, tfile)
  # upload csv
  result <- .pyEnv$upload_table(connection = pyConnection,
                                schema = testSchema,
                                table = table,
                                filepath = normalizePath(tfile))

  expect_equal(result$status, -1)
  # Test internal function
  expect_error(pyUploadCsv(testDatabaseConnection, schema = testSchema, table = table, filepath = tfile), "psycopg2 upload failed")

  # create table
  sql <- "CREATE TABLE @schema.@table (id int, test_string varchar)"
  DatabaseConnector::renderTranslateExecuteSql(testDatabaseConnection, sql, schema = testSchema, table = table)

  result <- .pyEnv$upload_table(connection = pyConnection,
                                schema = testSchema,
                                table = table,
                                filepath = normalizePath(tfile))
  expect_equal(result$status, 1)


  resultData <- DatabaseConnector::renderTranslateQuerySql(connection = testDatabaseConnection,
                                                           "SELECT * FROM @schema.@table",
                                                           schema = testSchema,
                                                           table = table,
                                                           snakeCaseToCamelCase = TRUE)
  expect_equal(nrow(testData), nrow(resultData))
  expect_true(all(c("id", "testString") %in% names(resultData)))

  # Test exported function
  DatabaseConnector::renderTranslateExecuteSql(testDatabaseConnection, "TRUNCATE TABLE @schema.@table;", schema = testSchema, table = table)
  pyUploadCsv(testDatabaseConnection, schema = testSchema, table = table, filepath = tfile)

  resultData <- DatabaseConnector::renderTranslateQuerySql(connection = testDatabaseConnection,
                                                           "SELECT * FROM @schema.@table",
                                                           schema = testSchema,
                                                           table = table,
                                                           snakeCaseToCamelCase = TRUE)
  expect_equal(nrow(testData), nrow(resultData))
  expect_true(all(c("id", "testString") %in% names(resultData)))
})


test_that("upload data.frame via string buffer", {
  skip_on_cran()
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "")
  # Should not throw error
  enablePythonUploads()
  expect_true(pyPgUploadEnabled())

  table <- paste0("test_", sample(1:10000, 1))
  sql <- "CREATE TABLE @schema.@table (id int, test_string varchar)"
  DatabaseConnector::renderTranslateExecuteSql(testDatabaseConnection, sql, schema = testSchema, table = table)

  pyConnection <- .createPyConnection(testDatabaseConnection)
  on.exit(pyConnection$close(), add = TRUE)
  buffer <- rawConnection(raw(0), "r+")

  on.exit(close(buffer), add = TRUE)
  testData <- data.frame(id = 1:100, test_string = 'some crazy vaLUEs;;a,.\t\n∑åˆø')
  readr::write_csv(testData, buffer)
  nchars <- seek(buffer, 0)
  result <- .pyEnv$upload_buffer_to_db(connection = pyConnection,
                                       csv_content = readChar(buffer, nchars = nchars),
                                       table = table,
                                       schema = testSchema)

  resultData <- DatabaseConnector::renderTranslateQuerySql(connection = testDatabaseConnection,
                                                           "SELECT * FROM @schema.@table",
                                                           schema = testSchema,
                                                           table = table,
                                                           snakeCaseToCamelCase = TRUE)
  expect_equal(nrow(testData), nrow(resultData))
  expect_true(all(c("id", "testString") %in% names(resultData)))
})