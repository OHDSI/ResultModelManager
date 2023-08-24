test_that("Type map function", {
  expect_equal(.getOpenApiSpecType("varchar"), "string")
  expect_equal(.getOpenApiSpecType("character"), "string")
  expect_equal(.getOpenApiSpecType("int"), "number")
  expect_equal(.getOpenApiSpecType("bigint"), "number")
  expect_equal(.getOpenApiSpecType("numeric"), "number")
  expect_equal(.getOpenApiSpecType("float"), "number")
  expect_equal(.getOpenApiSpecType("date"), "string")
})

connectionDetailsSetUp <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = ":memory:")
connectionHandler <- ConnectionHandler$new(connectionDetails = connectionDetailsSetUp)

tableSpecification <- data.frame(
  tableName = "cohort",
  columnName = c("cohort_definition_id", "cohort_name", "json", "sql"),
  primaryKey = c("yes", "no", "no", "no"),
  dataType = c("int", "varchar", "varchar", "varchar")
)

schemaSql <- generateSqlSchema(schemaDefinition = tableSpecification)
connectionHandler$executeSql(schemaSql, table_prefix = "cd_", database_schema = "main")

withr::defer(
  connectionHandler$finalize(),
  testthat::teardown_env()
)

makeFakeReq <- function(...) {
  list(body = list(...))
}

test_that("post handler", {
  req <- makeFakeReq(cohort_definition_id = 1)

  tableName <- "cohort"
  tablePrefix <- "cd_"

  columnSpecs <- tableSpecification %>% dplyr::filter(.data$tableName == tableName)

  qns <- createQueryNamespace(connectionHandler = connectionHandler,
                              tableSpecification = tableSpecification,
                              schema = "main",
                              tablePrefix = "cd_")

  resp <- .defaultPostHandler(req, qns, tableName, tablePrefix, columnSpecs)
  expect_data_frame(resp)

  req <- makeFakeReq(cohort_definition_id = c(1, 2))
  resp <- .defaultPostHandler(req, qns, tableName, tablePrefix, columnSpecs)
  expect_data_frame(resp)

  # Column name must exist in parameter
  req <- makeFakeReq(cohort_definition_id = 1, foo = "failure")
  expect_error(.defaultPostHandler(req, qns, tableName, tablePrefix, columnSpecs))
})

test_that("plumber router loads", {
  pr <- createPlumberRouter(connectionHandler = connectionHandler,
                            tableSpecification = tableSpecification,
                            schema = "main",
                            tablePrefix = "cd_")
  checkmate::expect_r6(pr, "Plumber")

  apiSpec <- pr$getApiSpec()
  for (tableName in unique(tableSpecification$tableName))
    expect_true(paste0('/', tableName) %in% names(apiSpec$paths))

  # Table spec should always be exposed
  expect_true("/table_spec" %in% names(apiSpec$paths))

  # Bad API query field
  tableSpecification$apiQueryField <- c("yeeees", "no", "no", "no")

  expect_error(createPlumberRouter(connectionHandler = connectionHandler,
                                   tableSpecification = tableSpecification,
                                   schema = "main",
                                   tablePrefix = "cd_"))
})

