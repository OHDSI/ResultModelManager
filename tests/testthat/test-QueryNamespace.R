connectionHandler <- ConnectionHandler$new(connectionDetails = connectionDetails)

tableSpecification <- data.frame(
  tableName = "cohort",
  columnName = c("cohort_definition_id", "cohort_name", "json", "sql"),
  primaryKey = c("yes", "no", "no", "no"),
  dataType = c("int", "varchar", "varchar", "varchar")
)

schemaSql <- generateSqlSchema(schemaDefinition = tableSpecification)
connectionHandler$executeSql(schemaSql, table_prefix = "cd_", database_schema = "main")

test_that("Errors", {
  qn <- QueryNamespace$new(
    tableSpecification = tableSpecification,
    result_schema = "main",
    tablePrefix = "cd_"
  )
  on.exit(qn$finalize())
  expect_error(qn$getConnectionHandler())
  qn$addReplacementVariable("foo", "fii")
  expect_error(qn$addReplacementVariable("foo", "fii2"))
})


test_that("test setConnectionHandler and getConnectionHandler functions", {
  cohortNamespace <- QueryNamespace$new(
    connectionHandler = connectionHandler,
    tableSpecification = tableSpecification,
    result_schema = "main",
    tablePrefix = "cd_"
  )
  on.exit(cohortNamespace$finalize(), add = TRUE)

  checkmate::expect_r6(cohortNamespace$getConnectionHandler(), "ConnectionHandler")

  cohortNamespace2 <- QueryNamespace$new(
    tableSpecification = tableSpecification,
    result_schema = "main",
    tablePrefix = "cd_"
  )

  on.exit(cohortNamespace2$finalize(), add = TRUE)

  cohortNamespace2$setConnectionHandler(connectionHandler)
  expect_equal(cohortNamespace2$getConnectionHandler(), connectionHandler)


  expect_equal(cohortNamespace$tablePrefix, "cd_")
  expect_equal(cohortNamespace$render("@cohort"), "cd_cohort")
  sql <- "SELECT * FROM @result_schema.@cohort WHERE cohort_definition_id = @cohort_id"
  renderedSql <- cohortNamespace$render(sql, cohort_id = 1)
  expect_equal(renderedSql, "SELECT * FROM main.cd_cohort WHERE cohort_definition_id = 1")

  result <- cohortNamespace$queryDb("SELECT * FROM @result_schema.@cohort WHERE cohort_definition_id = @cohort_id", cohort_id = 1)
  expect_true(is.data.frame(result))

  result <- cohortNamespace$executeSql("SELECT * FROM @result_schema.@cohort WHERE cohort_definition_id = @cohort_id", cohort_id = 1)
  expect_type(result, "NULL")
})

test_that("create helper function works", {
  skip_on_cran()
  expect_error(createQueryNamespace())

  qns <- createQueryNamespace(
    connectionDetails = connectionDetails,
    usePooledConnection = FALSE,
    tableSpecification = loadResultsDataModelSpecifications("settings/resultsDataModelSpecification.csv"),
    resultModelSpecificationPath = NULL,
    tablePrefix = "",
    snakeCaseToCamelCase = TRUE,
    databaseSchema = "main"
  )

  vars <- qns$getVars()
  expect_true("databaseSchema" %in% names(vars))

  qns <- createQueryNamespace(
    connectionDetails = connectionDetails,
    usePooledConnection = TRUE,
    tableSpecification = NULL,
    resultModelSpecificationPath = c(
      "settings/resultsDataModelSpecification.csv",
      "settings/testSchemaDef.csv"
    ),
    tablePrefix = "",
    snakeCaseToCamelCase = TRUE,
    databaseSchema = "main"
  )

  vars <- qns$getVars()
  expect_true("cohort_counts" %in% names(vars))
  expect_true("cohort_definition" %in% names(vars))
  expect_true("cdm_source_info" %in% names(vars))
  expect_true("cosine_similarity" %in% names(vars))
  expect_true("covariate_definition" %in% names(vars))
  expect_true("covariate_mean" %in% names(vars))
  expect_true("test_table_1" %in% names(vars))

  expect_error(
    createQueryNamespace(
      connectionDetails = connectionDetails,
      usePooledConnection = TRUE,
      tableSpecification = NULL,
      resultModelSpecificationPath = c("fileDoesNotExist"),
      tablePrefix = "",
      snakeCaseToCamelCase = TRUE,
      databaseSchema = "main"
    )
  )

  expect_error(
    createQueryNamespace(
      connectionDetails = NULL,
      usePooledConnection = FALSE,
      tableSpecification = NULL,
      resultModelSpecificationPath = c("settings/resultsDataModelSpecification.csv"),
      tablePrefix = "",
      snakeCaseToCamelCase = TRUE,
      databaseSchema = "main"
    )
  )
})
