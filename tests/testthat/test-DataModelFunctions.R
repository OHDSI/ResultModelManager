
test_that("results utility functions work", {
  expect_true(naToEmpty(NA) == "")
  expect_true(naToZero(NA) == 0)
})


test_that("Bad model format", {
  data <- data.frame(databaseId = 1, foo = c(22))

  junkSpec <- data.frame(
    tableName = "foo",
    columnName = c("databaseId", "charField", "dateField"),
    dataType = c("bigint", "varchar", "date"),
    isRequired = "Yes",
    primaryKey = "Yes",
    optional = "No",
    emptyIsNa = "No"
  )
  expect_error(checkAndFixColumnNames(data, "foo", "foo.zip", junkSpec))

  data <- data.frame(databaseId = c("1.0", "1.0") , charField = c(22, 22), dateField = c("2022/12/21", "2022/12/21"))
  data <- checkAndFixDataTypes(data, "foo", "foo.zip", junkSpec)
  expect_warning(data <- checkAndFixDuplicateRows(data, "foo", "foo.zip", junkSpec))
  expect_true(nrow(data) == 1)
  checkmate::expect_numeric(data$databaseId)
  checkmate::expect_character(data$charField)
  checkmate::expect_date(data$dateField)
})
