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


test_that("Delete primary key rows function", {

  skip_if_results_db_not_available()
  sql <- "
  DROP TABLE IF EXISTS @schema.@test_table;
  CREATE TABLE @schema.@test_table (id int, id_2 int);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (1, 2);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (3, 4);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (5, 6);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (7, 8);
  "
  testTable <- "test_delete_table"
  DatabaseConnector::renderTranslateExecuteSql(testDatabaseConnection, sql, schema = testSchema, test_table = testTable)

  # Delete nothing
  deleteAllRowsForPrimaryKey(
    connection = testDatabaseConnection,
    schema = testSchema,
    tableName = testTable,
    keyValues = data.frame()
  )

  # Delete nothing
  deleteAllRowsForPrimaryKey(
    connection = testDatabaseConnection,
    schema = testSchema,
    tableName = testTable,
    keyValues = data.frame(ID = c(1), ID_2 = (99)) # one key is valid, one is not
  )

  result <- DatabaseConnector::renderTranslateQuerySql(testDatabaseConnection,
                                                       "SELECT * FROM @schema.@test_table",
                                                       schema = testSchema,
                                                       test_table = testTable)
  expect_equal(nrow(result), 4)


  deleteRows <- data.frame(
    ID = c(1, 3),
    ID_2 = c(2, 4)
  )

  keptRows <- data.frame(
    ID = c(5, 7),
    ID_2 = c(6, 8)
  )

  deleteAllRowsForPrimaryKey(
    connection = testDatabaseConnection,
    schema = testSchema,
    tableName = testTable,
    keyValues = deleteRows
  )

  result <- DatabaseConnector::renderTranslateQuerySql(testDatabaseConnection,
                                                       "SELECT * FROM @schema.@test_table",
                                                       schema = testSchema,
                                                       test_table = testTable)
  expect_identical(result, keptRows)

  deleteAllRowsForPrimaryKey(
    connection = testDatabaseConnection,
    schema = testSchema,
    tableName = testTable,
    keyValues = keptRows
  )

  result <- DatabaseConnector::renderTranslateQuerySql(testDatabaseConnection,
                                                       "SELECT * FROM @schema.@test_table",
                                                       schema = testSchema,
                                                       test_table = testTable)
  expect_equal(nrow(result), 0)
})


test_that("Delete primary key rows function SQLITE", {

  sqliteConn <- DatabaseConnector::connect(dbms = "sqlite", server = ":memory:")
  on.exit(DatabaseConnector::disconnect(sqliteConn))

  sql <- "
  DROP TABLE IF EXISTS @schema.@test_table;
  CREATE TABLE @schema.@test_table (id int, id_2 int);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (1, 2);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (3, 4);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (5, 6);
  INSERT INTO @schema.@test_table (id, id_2) VALUES (7, 8);
  "
  testTable <- "test_delete_table"
  DatabaseConnector::renderTranslateExecuteSql(sqliteConn, sql, schema = "main", test_table = testTable)

  # Delete nothing
  deleteAllRowsForPrimaryKey(
    connection = sqliteConn,
    schema = "main",
    tableName = testTable,
    keyValues = data.frame()
  )

  # Delete nothing
  deleteAllRowsForPrimaryKey(
    connection = sqliteConn,
    schema = "main",
    tableName = testTable,
    keyValues = data.frame(ID = c(1), ID_2 = (99)) # one key is valid, one is not
  )

  result <- DatabaseConnector::renderTranslateQuerySql(sqliteConn,
                                                       "SELECT * FROM @schema.@test_table",
                                                       schema = "main",
                                                       test_table = testTable)
  expect_equal(nrow(result), 4)


  deleteRows <- data.frame(
    ID = c(1, 3),
    ID_2 = c(2, 4)
  )

  keptRows <- data.frame(
    ID = c(5, 7),
    ID_2 = c(6, 8)
  )

  deleteAllRowsForPrimaryKey(
    connection = sqliteConn,
    schema = "main",
    tableName = testTable,
    keyValues = deleteRows
  )

  result <- DatabaseConnector::renderTranslateQuerySql(sqliteConn,
                                                       "SELECT * FROM @schema.@test_table",
                                                       schema = "main",
                                                       test_table = testTable)
  expect_identical(result, keptRows)

  deleteAllRowsForPrimaryKey(
    connection = sqliteConn,
    schema = "main",
    tableName = testTable,
    keyValues = keptRows
  )

  result <- DatabaseConnector::renderTranslateQuerySql(sqliteConn,
                                                       "SELECT * FROM @schema.@test_table",
                                                       schema = "main",
                                                       test_table = testTable)
  expect_equal(nrow(result), 0)
})