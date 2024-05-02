test_that("results utility functions work", {
  expect_true(naToEmpty(NA) == "")
  expect_true(naToZero(NA) == 0)
})


test_that("Bad model format", {
  data <- data.frame(databaseId = 1, foo = c(22))
  expect_character(checkSpecificationColumns(data))

  junkSpec <- data.frame(
    tableName = "foo",
    columnName = c("databaseId", "charField", "dateField"),
    dataType = c("bigint", "varchar", "date"),
    isRequired = "Yes",
    primaryKey = "Yes",
    optional = "No"
  )
  expect_error(checkAndFixColumnNames(data, "foo", "foo.zip", junkSpec))

  data <- data.frame(databaseId = c("1.0", "1.0"), charField = c(22, 22), dateField = c("2022/12/21", "2022/12/21"))
  data <- checkAndFixDataTypes(data, "foo", "foo.zip", junkSpec)
  expect_warning(data <- checkAndFixDuplicateRows(data, "foo", "foo.zip", junkSpec))
  expect_true(nrow(data) == 1)
  checkmate::expect_numeric(data$databaseId)
  checkmate::expect_character(data$charField)
  checkmate::expect_date(data$dateField)
})

# Tests for when types need conversion
chunk_numeric <- data.frame(
  id = c(1, 2, 3),
  name = c("Alice", "Bob", "Charlie"),
  stringsAsFactors = FALSE
)

test_that("formatChunk converts non-matching types to character", {
  expect_identical(
    formatChunk(pkValuesInDb, chunk_numeric)$id,
    as.character(chunk_numeric$id),
    info = "Numeric columns in chunk should be converted to character."
  )
})

# Test for error when types cannot be converted
chunk_factor <- data.frame(
  id = c(1, 2, 3),
  name = factor(c("Alice", "Bob", "Charlie"))
)

test_that("formatChunk throws error for incompatible types", {
  expect_error(
    formatChunk(pkValuesInDb, chunk_factor),
    "id is of type numeric which cannot be converted between data frames pkValuesInDb and chunk"
  )
})