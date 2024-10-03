test_that("test python postgres connection works", {
  skip_on_cran()
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "")
  expect_true(pyPgUploadEnabled())
  # Should not throw error
  enablePythonUploads()

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