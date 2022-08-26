test_that("Migrations manager loads", {
  manager <- DataMigrationManager$new(connectionDetails = connectionDetails,
                                      resultsDatabaseSchema = "main",
                                      tablePrefix = "",
                                      migrationPath = "migrations",
                                      packageName = "ResultModelManager")

  manager$executeMigrations()
  manager$finalize()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  migrations <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM migration")
  checkmate::expect_data_frame(migrations, nrows = 2)
})
