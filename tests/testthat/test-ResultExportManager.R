test_that("Test result export manager methods", {
  # Create a dummy table specification and set the export directory
  table_spec <- data.frame(
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
})

# Test exporting a data frame
test_that("exportDataFrame method exports data frame correctly", {
  # Create a dummy data frame
  df <- tibble::tibble(
    col1 = c(1, 2, 3),
    col2 = c("A", "B", "C")
  )

  # Create a dummy table specification and set the export directory
  table_spec <- tibble::tibble(
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

  df2 <- tibble::tibble(
    col1 = c(4, 5, 6),
    col2 = c("D", "E", "F")
  )

  exportManager$exportDataFrame(df2, "table1", append = TRUE)

  dfR <- readr::read_csv(output_file, show_col_types = FALSE)

  expect_equal(dfR, rbind(df, df2))
  expect_error(exportManager$exportDataFrame(df2, "table1", append = TRUE), "Cannot write data - primary keys already written to cache")

})
