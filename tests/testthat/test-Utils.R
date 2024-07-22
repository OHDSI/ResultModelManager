test_that("Grant permissions", {
  tableSpecification <- loadResultsDataModelSpecifications("settings/testSchemaDef.csv")

  expect_error(
    grantTablePermissions(
      connectionDetails = connectionDetails,
      tableSpecification = tableSpecification,
      databaseSchema = "main",
      tablePrefix = "",
      permissions = "SELECT",
      user = "fop"
    )
  )

  skip_if_results_db_not_available()
  testthat::skip_on_cran()

  sql <- generateSqlSchema(schemaDefinition = tableSpecification)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = testDatabaseConnection,
    sql = sql,
    database_schema = testSchema,
    table_prefix = "test_utils_"
  )

  # Not crashing is the test here
  grantTablePermissions(
    connection = testDatabaseConnection,
    tableSpecification = tableSpecification,
    databaseSchema = testSchema,
    tablePrefix = "test_utils_",
    permissions = "SELECT",
    user = testDatabaseConnectionDetails$user()
  )

  grantTablePermissions(
    connection = testDatabaseConnection,
    tableSpecification = tableSpecification,
    databaseSchema = testSchema,
    tablePrefix = "test_utils_",
    permissions = "INSERT",
    user = testDatabaseConnectionDetails$user()
  )

  grantTablePermissions(
    connectionDetails = testDatabaseConnectionDetails,
    tableSpecification = tableSpecification,
    databaseSchema = testSchema,
    tablePrefix = "test_utils_",
    permissions = "DELETE",
    user = testDatabaseConnectionDetails$user()
  )

  expect_error(
    grantTablePermissions(
      connection = testDatabaseConnection,
      tableSpecification = tableSpecification,
      databaseSchema = testSchema,
      tablePrefix = "test_utils_",
      permissions = "ALL",
      user = testDatabaseConnectionDetails$user()
    )
  )
})
