# Implementations of database connection should function in the same way
genericTests <- function(connClass, classes, connectionClass) {
  testConnection <- DatabaseConnector::connect(connectionDetails)
  sql <- "
  DROP TABLE IF EXISTS main.concept;
  CREATE TABLE main.concept AS
  SELECT 1 as concept_id
  UNION
  SELECT 2 as concept_id
  UNION
  SELECT 3 as concept_id
  UNION
  SELECT 4 as concept_id
  UNION
  SELECT 5 as concept_id
  UNION
  SELECT 6 as concept_id;
  "
  DatabaseConnector::renderTranslateExecuteSql(testConnection, sql)

  on.exit({
    sql <- "DROP TABLE IF EXISTS main.concept;"
    DatabaseConnector::renderTranslateExecuteSql(testConnection, sql)
    DatabaseConnector::disconnect(testConnection)
  })

  conn <- connClass$new(connectionDetails)
  checkmate::expect_class(conn, classes)
  on.exit(
    {
      conn$finalize()
    },
    add = TRUE
  )

  checkmate::expect_class(conn, "ConnectionHandler")
  expect_true(conn$isActive)
  expect_true(DBI::dbIsValid(dbObj = conn$con))

  withr::with_envvar(new = c("LIMIT_ROW_COUNT" = 1), {
    data <- conn$queryDb("SELECT concept_id FROM main.concept;")
    expect_equal(nrow(data), 1)
  })

  withr::with_envvar(new = c("LIMIT_ROW_COUNT" = 5), {
    data <- conn$queryDb("SELECT concept_id FROM main.concept")
    expect_equal(nrow(data), 5)
  })

  data <- conn$queryDb("SELECT count(*) AS cnt_test FROM main.concept;")

  checkmate::expect_data_frame(data)
  expect_equal(data$cntTest, 6)

  data2 <- conn$queryDb("SELECT count(*) AS cnt_test FROM main.concept;", snakeCaseToCamelCase = FALSE)

  checkmate::expect_data_frame(data2)
  expect_equal(data2$CNT_TEST, 6)

  expect_error(conn$queryDb("SELECT 1 * WHERE;"))

  expect_equal(DatabaseConnector::dbms(testConnection), conn$dbms())
  conceptTbl <- conn$tbl("concept", databaseSchema = "main")

  expect_class(conceptTbl, c("tbl", "tbl_lazy"))

  conn$closeConnection()
  expect_false(conn$isActive)
  expect_false(conn$dbIsValid())
  conn$initConnection()
  expect_true(conn$isActive)

  expect_warning(conn$initConnection(), "Closing existing connection")
  checkmate::expect_class(conn$getConnection(), connectionClass)
  conn$closeConnection()
}

test_that("Database Connector Class works", {
  genericTests(ConnectionHandler,
    classes = c("ConnectionHandler"),
    connectionClass = "DatabaseConnectorDbiConnection"
  )
})

test_that("Pooled connector Class works", {
  genericTests(PooledConnectionHandler,
    classes = c("PooledConnectionHandler", "ConnectionHandler"),
    connectionClass = "DatabaseConnectorDbiConnection"
  )
})
