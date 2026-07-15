test_that("QueryNamespace handles namespaced table specifications", {
  connectionHandler <- ConnectionHandler$new(connectionDetails = connectionDetails)

  tableSpecification <- data.frame(
    namespace = "cg",
    tableName = "cohort",
    columnName = c("cohort_definition_id", "cohort_name", "json", "sql"),
    primaryKey = c("yes", "no", "no", "no"),
    dataType = c("int", "varchar", "varchar", "varchar")
  )

  schemaSql <- generateSqlSchema(schemaDefinition = tableSpecification)
  connectionHandler$executeSql(schemaSql, table_prefix = "cd_", database_schema = "main")

  qns <- QueryNamespace$new(
    connectionHandler = connectionHandler,
    tableSpecification = tableSpecification,
    result_schema = "main",
    tablePrefix = "cd_"
  )
  on.exit({
    qns$closeConnection()
  })

  expect_equal(qns$render("@cg_cohort"), "cd_cg_cohort")

  sql <- "SELECT * FROM @result_schema.@cg_cohort WHERE cohort_definition_id = @cohort_id"
  renderedSql <- qns$render(sql, cohort_id = 1)
  expect_equal(renderedSql, "SELECT * FROM main.cd_cg_cohort WHERE cohort_definition_id = 1")
})

test_that("QueryNamespace backward compat without namespace", {
  connectionHandler <- ConnectionHandler$new(connectionDetails = connectionDetails)

  tableSpecification <- data.frame(
    tableName = "cohort2",
    columnName = c("cohort_definition_id", "cohort_name", "json", "sql"),
    primaryKey = c("yes", "no", "no", "no"),
    dataType = c("int", "varchar", "varchar", "varchar")
  )

  schemaSql <- generateSqlSchema(schemaDefinition = tableSpecification)
  connectionHandler$executeSql(schemaSql, table_prefix = "cd_", database_schema = "main")

  qns <- QueryNamespace$new(
    connectionHandler = connectionHandler,
    tableSpecification = tableSpecification,
    result_schema = "main",
    tablePrefix = "cd_"
  )
  on.exit({
    qns$closeConnection()
  })

  expect_equal(qns$render("@cohort2"), "cd_cohort2")
})

test_that("QueryNamespace handles multiple namespaces from yaml", {
  connectionHandler <- ConnectionHandler$new(connectionDetails = connectionDetails)

  spec <- loadResultsDataModelSpecifications("settings/testSchemaDef.yaml")

  schemaSql <- generateSqlSchema(schemaDefinition = spec)
  connectionHandler$executeSql(schemaSql, table_prefix = "main_", database_schema = "main")

  qns <- QueryNamespace$new(
    connectionHandler = connectionHandler,
    tableSpecification = spec,
    result_schema = "main",
    tablePrefix = "main_"
  )
  on.exit({
    qns$closeConnection()
  })

  expect_equal(qns$render("@cg_cohort_definition"), "main_cg_cohort_definition")
  expect_equal(qns$render("@cg_cohort_counts"), "main_cg_cohort_counts")
  expect_equal(qns$render("@cg_cosine_similarity"), "main_cg_cosine_similarity")
})

test_that("createQueryNamespace works with yaml spec files", {
  skip_on_cran()

  qns <- createQueryNamespace(
    connectionDetails = connectionDetails,
    usePooledConnection = FALSE,
    resultModelSpecificationPath = "settings/testSchemaDef.yaml",
    tablePrefix = "test_",
    snakeCaseToCamelCase = TRUE,
    databaseSchema = "main"
  )

  vars <- qns$getVars()
  expect_true("cg_cohort_definition" %in% names(vars))
  expect_true("cg_cdm_source_info" %in% names(vars))
  expect_true("cg_cohort_counts" %in% names(vars))
  expect_equal(vars[["cg_cohort_definition"]], "test_cg_cohort_definition")
})

test_that("createQueryNamespace merges csv and yaml specs", {
  skip_on_cran()

  qns <- suppressWarnings(createQueryNamespace(
    connectionDetails = connectionDetails,
    usePooledConnection = FALSE,
    resultModelSpecificationPath = c(
      "settings/resultsDataModelSpecification.csv",
      "settings/testSchemaDef.yaml"
    ),
    tablePrefix = "",
    snakeCaseToCamelCase = TRUE,
    databaseSchema = "main"
  ))

  vars <- qns$getVars()
  expect_true("test_table_1" %in% names(vars))
  expect_true("cg_cohort_definition" %in% names(vars))
  expect_true("cg_cohort_counts" %in% names(vars))
})
