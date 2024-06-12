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


# Create test data
pkValuesInDb <- data.frame(
  id = c(1, 2, 3),
  name = c("Alice", "Bob", "Charlie")
)

chunk <- data.frame(
  id = c(1, 2, 3),
  name = c("Alice", "Bob", "Charlie")
)

# Tests for when types match
test_that("formatChunk converts data types correctly", {
  expect_identical(
    formatChunk(pkValuesInDb, chunk),
    chunk,
    info = "Data frames should remain unchanged when types match."
  )
})

# Tests for when types need conversion
char <- data.frame(
  id = c(1, 2, 3),
  name = c("Alice", "Bob", "Charlie")
)

pkValuesInDbChar <- data.frame(
  id = as.character(c(1, 2, 3)),
  name = c("Alice", "Bob", "Charlie")
)

test_that("formatChunk converts non-matching types to character", {
  expect_identical(
    formatChunk(pkValuesInDbChar, char)$id,
    as.character(char$id),
    info = "Numeric columns in chunk should be converted to character."
  )
})

# Test for error when types cannot be converted
chunk_factor <- data.frame(
  id = as.character(c(1, 2, 3)),
  name = c("Alice", "Bob", "Charlie")
)

test_that("formatChunk throws error for incompatible types", {
  expect_error(
    formatChunk(pkValuesInDb, chunk_factor),
    "id is of type numeric which cannot be converted between data frames pkValuesInDb and chunk"
  )
})

test_that("format chunk handles int/numeric type conversions ok", {
  class(pkValuesInDb$id) <- "numeric"
  class(chunk$id) <- "integer"
  chunk <- formatChunk(pkValuesInDb, chunk)
  checkmate::expect_data_frame(chunk)
  checkmate::expect_numeric(chunk$id)
})
