test_that("Test result export manager methods", {
  # Create a dummy table specification and set the export directory
  table_spec <- dplyr::tibble(
    tableName = c("table1", "table1"),
    columnName = c("col1", "col2"),
    primaryKey = c("yes", "no"),
    optional = c("no", "yes"),
    dataType = c("int", "varchar")
  )
  export_dir <- tempfile()
  on.exit(unlink(export_dir, recursive = TRUE, force = TRUE))

  # Initialize the ResultExportManager
  exportManager <- createResultExportManager(tableSpecification = table_spec, exportDir = export_dir)

  # Test that the properties are set correctly
  expect_equal(exportManager$getTableSpec("table1"), table_spec)
  expect_equal(exportManager$exportDir, export_dir)

  expect_false(exportManager$checkPrimaryKeys(data.frame(col1 = NA, col2 = "foo"), "table1"))
  expect_false(exportManager$checkPrimaryKeys(data.frame(col1 = c(1, 2, 1), col2 = "foo"), "table1"))

  checkmate::expect_list(exportManager$getManifestList())
  exportManager$writeManifest()
  expect_file_exists(file.path(export_dir, "manifest.json"))
})

# Test exporting a data frame
test_that("exportDataFrame method exports data frame correctly", {
  # Create a dummy data frame
  df <- dplyr::tibble(
    col1 = c(1, 2, 3),
    col2 = c("A", "B", "C")
  )

  # Create a dummy table specification and set the export directory
  table_spec <- dplyr::tibble(
    tableName = "table1",
    columnName = c("col1", "col2"),
    primaryKey = c("yes", "no"),
    optional = c("no", "no"),
    dataType = c("int", "varchar"),
    minCellCount = c("No", "No")
  )
  export_dir <- tempfile()
  on.exit(unlink(export_dir, recursive = TRUE, force = TRUE))
  # Initialize the ResultExportManager
  exportManager <- createResultExportManager(tableSpecification = table_spec, exportDir = export_dir)

  # Export the data frame
  exportManager$exportDataFrame(df, "table1")

  # Check that the file is created and contains the correct data
  output_file <- file.path(export_dir, "table1.csv")
  expect_true(file.exists(output_file))
  dfR <- readr::read_csv(output_file, , show_col_types = FALSE)
  expect_equal(dfR, df)

  df2 <- dplyr::tibble(
    col1 = c(4, 5, 6),
    col2 = c("D", "E", "F")
  )

  exportManager$exportDataFrame(df2, "table1", append = TRUE)

  dfR <- readr::read_csv(output_file, show_col_types = FALSE)

  expect_equal(dfR, rbind(df, df2))
  expect_error(exportManager$exportDataFrame(df2, "table1", append = TRUE), "Cannot write data - primary keys already written to cache")

  expect_error(exportManager$exportDataFrame(df2, "table999", append = TRUE), "Table not found in specifications")
  exportManager$writeManifest()
  expect_file_exists(file.path(export_dir, "manifest.json"))
})


testDbExport <- function(connectionDetails, schema, n = 100) {
  ch <- ConnectionHandler$new(connectionDetails)
  on.exit(ch$finalize())
  # 1 million random rows

  table_spec <- dplyr::tibble(
    tableName = "big_table",
    columnName = c("row_id", "value", "p_count"),
    primaryKey = c("yes", "no", "no"),
    optional = c("no", "no", "no"),
    dataType = c("int", "float", "int"),
    minCellCount = c("No", "No", "yes")
  )

  bigData <- data.frame(row_id = 1:n, value = stats::runif(n), p_count = 3)
  conn <- ch$getConnection()
  DatabaseConnector::insertTable(conn, data = bigData, tableName = "big_table", databaseSchema = schema)

  exportDir <- tempfile()
  on.exit(unlink(exportDir, recursive = TRUE, force = TRUE))

  exportManager <- createResultExportManager(tableSpecification = table_spec, exportDir = exportDir, minCellCount = 5)

  transformFunc <- function(rows, pos, test) {
    expect_data_frame(rows)
    expect_integerish(pos)
    expect_equal(test, 1234)
    return(rows)
  }

  transformFuncArgs <- list(test = 1234)
  exportManager$exportQuery(
    connection = conn,
    sql = "SELECT * FROM @schema.big_table",
    exportTableName = "big_table",
    schema = schema,
    transformFunction = transformFunc,
    transformFunctionArgs = transformFuncArgs
  )

  # Test min cell count
  output_file <- file.path(exportDir, "big_table.csv")
  result <- readr::read_csv(output_file, show_col_types = FALSE)
  expect_true(all(result$p_count == -5))
}

test_that("export via sql", {
  testDbC <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = ":memory:")
  testDbExport(testDbC, schema = "main", n = 1e5)
})


test_that("export from postgres", {
  skip_if_results_db_not_available()
  testDbExport(testDatabaseConnectionDetails, schema = testSchema)
})
