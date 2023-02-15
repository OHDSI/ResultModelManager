test_that("Schema gen from file", {
  testCd <- DatabaseConnector::createConnectionDetails(server = "testSchema.db", dbms = "sqlite")
  connection <- DatabaseConnector::connect(testCd)
  tfile <- tempfile()
  on.exit({
    unlink("testSchema.db")
    unlink(tfile)
    DatabaseConnector::disconnect(connection)
  })

  schema <- generateSqlSchema(csvFilepath = "settings/testSchemaDef.csv", sqlOutputPath = tfile)
  checkmate::expect_file_exists(tfile)

  schemaDetails <- readr::read_csv("settings/testSchemaDef.csv", show_col_types = FALSE)
  colnames(schemaDetails) <- SqlRender::snakeCaseToCamelCase(colnames(schemaDetails))
  checkmate::expect_string(schema)

  DatabaseConnector::renderTranslateExecuteSql(connection, schema, database_schema = "main")

  for (table in schemaDetails$tableName |> unique()) {
    res <- DatabaseConnector::renderTranslateQuerySql(connection,
      "SELECT * FROM @table_name",
      table_name = table
    )
  }
})
