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


  server <- Sys.getenv("CDM5_POSTGRESQL_SERVER")
  hostServerDb <- strsplit(server, "/")[[1]]
  # Test with connection string
  testDatabaseConnection2 <- DatabaseConnector::connect(
    dbms = "postgresql",
    connectionString = paste0("jdbc:postgresql://", hostServerDb[1], ":5432/", hostServerDb[2]),
    user = Sys.getenv("CDM5_POSTGRESQL_USER"),
    password = utils::URLdecode(Sys.getenv("CDM5_POSTGRESQL_PASSWORD"))
  )

  on.exit(DatabaseConnector::disconnect(testDatabaseConnection2), add = TRUE)
  pyConnection2 <- .createPyConnection(testDatabaseConnection2)
  on.exit(pyConnection2$close(), add = TRUE)

  if(!interactive())
    expect_error(install_psycopg2(), "Session is not interactive. This is not how you want to install psycopg2")
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
  expect_equal(sum(resultData$id), sum(testData$id))
  expect_true(all(c("id", "testString") %in% names(resultData)))
})


test_that("upload data.frame via string buffer", {
  skip_on_cran()
  skip_if(Sys.getenv("CDM5_POSTGRESQL_SERVER") == "")
  # Should not throw error
  enablePythonUploads()
  expect_true(pyPgUploadEnabled())

  table <- paste0("test_", sample(1:10000, 1))
  sql <- "CREATE TABLE @schema.@table (id int, my_date date, my_date2 varchar, test_string1 varchar, test_string2 varchar)"
  DatabaseConnector::renderTranslateExecuteSql(testDatabaseConnection, sql, schema = testSchema, table = table)

  pyConnection <- .createPyConnection(testDatabaseConnection)
  on.exit(pyConnection$close(), add = TRUE)
  testData <- data.frame(id = 1:100,
                         test_string1 = 'Sjögren syndrome',
                         test_string2 = 'Merative MarketScan® Commercial Claims and Encounters Ωåß∂',
                         my_date = as.Date("1980-05-28"),
                         my_date2 = as.Date("1980-05-30"))

  # Note small buffer write size tests if buffering is functioning correctly
  .pgWriteDataFrame(data = testData,
                    pyConnection = pyConnection,
                    table = table,
                    schema = testSchema)
  pyConnection$commit()

  resultData <- DatabaseConnector::renderTranslateQuerySql(connection = testDatabaseConnection,
                                                           "SELECT * FROM @schema.@table",
                                                           schema = testSchema,
                                                           table = table,
                                                           snakeCaseToCamelCase = TRUE)
  expect_equal(nrow(resultData), nrow(testData))
  expect_equal(sum(resultData$id), sum(testData$id))
  expect_true(all(c("id", "testString2", "testString1", "myDate", "myDate2") %in% names(resultData)))

  DatabaseConnector::renderTranslateExecuteSql(testDatabaseConnection, "TRUNCATE TABLE @schema.@table", schema = testSchema, table = table)
  pyUploadDataFrame(testData, testDatabaseConnection, schema = testSchema, table = table)

  expect_equal(nrow(resultData), nrow(testData))
  expect_equal(sum(resultData$id), sum(testData$id))
  expect_true(all(c("id", "testString2", "testString1", "myDate", "myDate2") %in% names(resultData)))
})