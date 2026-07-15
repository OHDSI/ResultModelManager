test_that("generateSqlSchema from yaml with platform config includes partitioning", {
  tfile <- tempfile(fileext = ".sql")
  on.exit(unlink(tfile))

  schema <- generateSqlSchema(
    csvFilepath = "settings/testSchemaDef.yaml",
    sqlOutputPath = tfile,
    platform = "postgresql"
  )

  checkmate::expect_file_exists(tfile)
  checkmate::expect_string(schema)

  expect_true(grepl("PARTITION BY RANGE \\(cohort_definition_id\\)", schema))
  expect_true(grepl("CREATE UNIQUE INDEX", schema))
})

test_that("generateSqlSchema from yaml with platform config includes indexes for sql_server", {
  tfile <- tempfile(fileext = ".sql")
  on.exit(unlink(tfile))

  schema <- generateSqlSchema(
    csvFilepath = "settings/testSchemaDef.yaml",
    sqlOutputPath = tfile,
    platform = "sql_server"
  )

  expect_true(grepl("CREATE INDEX", schema))
  expect_false(grepl("PARTITION BY", schema))
})

test_that("generateSqlSchema from yaml warns when unsupported platform has partitioning", {
  tmpYaml <- tempfile(fileext = ".yaml")
  on.exit(unlink(tmpYaml))

  yamlContent <- list(
    version = "1.0",
    namespace = list(
      test = list(
        tables = list(
          t1 = list(
            columns = list(
              list(name = "id", type = "bigint", primary_key = TRUE)
            )
          )
        )
      )
    ),
    platforms = list(
      duckdb = list(
        namespace = list(
          test = list(
            tables = list(
              t1 = list(partition_by = "RANGE (id)")
            )
          )
        )
      )
    )
  )
  yaml::write_yaml(yamlContent, tmpYaml)

  expect_warning(
    generateSqlSchema(
      csvFilepath = tmpYaml,
      platform = "duckdb"
    ),
    "Partitioning is not supported for platform"
  )
})

test_that("generateSqlSchema from yaml without platform parameter excludes platform features", {
  schema <- generateSqlSchema(csvFilepath = "settings/testSchemaDef.yaml")
  expect_false(grepl("PARTITION BY", schema))
  expect_false(grepl("CREATE.*INDEX", schema))
})

test_that("generateSqlSchema from csv still works unchanged with deprecation warning", {
  tfile <- tempfile()
  on.exit(unlink(tfile))

  expect_warning(
    schema <- generateSqlSchema(csvFilepath = "settings/testSchemaDef.csv", sqlOutputPath = tfile),
    "CSV-based schema definitions are deprecated"
  )
  checkmate::expect_file_exists(tfile)
  checkmate::expect_string(schema)
  expect_false(grepl("PARTITION BY", schema))
})

test_that("generateSqlSchema with namespaced data.frame and platform", {
  testCd <- DatabaseConnector::createConnectionDetails(server = "testPlatformSchema.db", dbms = "sqlite")
  connection <- DatabaseConnector::connect(testCd)
  on.exit({
    unlink("testPlatformSchema.db")
    DatabaseConnector::disconnect(connection)
  })

  spec <- loadResultsDataModelSpecifications("settings/testSchemaDef.yaml")

  schema <- generateSqlSchema(schemaDefinition = spec, platform = "sqlite")
  expect_true(grepl("@cg_cohort_definition", schema, fixed = TRUE))
  expect_true(grepl("@cg_cohort_counts", schema, fixed = TRUE))

  DatabaseConnector::renderTranslateExecuteSql(connection, schema, database_schema = "main")

  res <- DatabaseConnector::renderTranslateQuerySql(connection,
    "SELECT * FROM @table_name",
    table_name = "cg_cohort_definition"
  )
  expect_true(is.data.frame(res))
})
