test_that("loadResultsDataModelSpecifications supports yaml files", {
  spec <- loadResultsDataModelSpecifications("settings/resultsDataModelSpecification.yaml")
  expect_true("namespace" %in% colnames(spec))
  expect_true("tableName" %in% colnames(spec))
  expect_true("columnName" %in% colnames(spec))
  expect_true("dataType" %in% colnames(spec))
  expect_true("primaryKey" %in% colnames(spec))

  expect_equal(unique(spec$namespace), "test")
  expect_true("test_table_1" %in% spec$tableName)
  expect_true("test_table_2" %in% spec$tableName)
  expect_true("test_table_3" %in% spec$tableName)
})

test_that("loadResultsDataModelSpecifications still works with csv files and warns deprecation", {
  expect_warning(
    spec <- loadResultsDataModelSpecifications("settings/resultsDataModelSpecification.csv"),
    "CSV-based results data model specifications are deprecated"
  )
  expect_false("namespace" %in% colnames(spec))
  expect_true("tableName" %in% colnames(spec))
  expect_true("columnName" %in% colnames(spec))
  expect_equal(unique(spec$tableName), c("test_table_1", "test_table_2", "test_table_3"))
})

test_that("loadResultsDataModelFromYaml returns platform config", {
  result <- loadResultsDataModelFromYaml("settings/testSchemaDef.yaml")
  expect_true("specification" %in% names(result))
  expect_true("platforms" %in% names(result))

  spec <- result$specification
  expect_true("namespace" %in% colnames(spec))
  expect_equal(unique(spec$namespace), "cg")

  platforms <- result$platforms
  expect_true("postgresql" %in% names(platforms))
  expect_true("sql_server" %in% names(platforms))
  expect_true("duckdb" %in% names(platforms))
  expect_true("sqlite" %in% names(platforms))

  pgConfig <- platforms$postgresql$namespace$cg$tables$cohort_counts
  expect_equal(pgConfig$partition_by, "RANGE (cohort_definition_id)")
  expect_true(length(pgConfig$indexes) > 0)
})

test_that("yaml spec validation fails on invalid files", {
  tmpYaml <- tempfile(fileext = ".yaml")
  writeLines("namespace:\n  test:\n    tables:\n      t1:\n        columns:\n          - name: col1", tmpYaml)
  on.exit(unlink(tmpYaml))
  expect_error(loadResultsDataModelSpecifications(tmpYaml))

  writeLines("version: '1.0'\nnamespace:\n  test:\n    tables:\n      t1:\n        columns:\n          - type: int", tmpYaml)
  expect_error(loadResultsDataModelSpecifications(tmpYaml))
})

test_that("csv and yaml specs produce equivalent data for data model tables", {
  csvSpec <- suppressWarnings(
    loadResultsDataModelSpecifications("settings/resultsDataModelSpecification.csv")
  )
  yamlSpec <- loadResultsDataModelSpecifications("settings/resultsDataModelSpecification.yaml")

  csvTables <- unique(csvSpec$tableName)
  yamlTables <- unique(yamlSpec$tableName)
  expect_setequal(csvTables, yamlTables)

  for (tbl in csvTables) {
    csvCols <- csvSpec |>
      dplyr::filter(.data$tableName == tbl) |>
      dplyr::pull("columnName") |>
      sort()
    yamlCols <- yamlSpec |>
      dplyr::filter(.data$tableName == tbl) |>
      dplyr::pull("columnName") |>
      sort()
    expect_setequal(csvCols, yamlCols)
  }
})
