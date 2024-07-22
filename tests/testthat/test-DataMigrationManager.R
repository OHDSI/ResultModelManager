test_that("regexp pattern works", {
  expect_true(grepl(.defaultMigrationRegexp, "Migration_1-MyMigration.sql") > 0)
  expect_true(grepl(.defaultMigrationRegexp, "Migration_2-v3.2whaterver.sql") > 0)
  expect_true(grepl(.defaultMigrationRegexp, "Migration_4-TEST.sql") > 0)

  expect_false(grepl(.defaultMigrationRegexp, "Migration_4-.sql") > 0)
  expect_false(grepl(.defaultMigrationRegexp, "Migration_4-missing_letter.sl") > 0)
  expect_false(grepl(.defaultMigrationRegexp, "Migraton_4-a.sql") > 0)
  expect_false(grepl(.defaultMigrationRegexp, "Migration_2v3.2whaterver.sql") > 0)
  expect_false(grepl(.defaultMigrationRegexp, "foo.sql") > 0)
  expect_false(grepl(.defaultMigrationRegexp, "UpdateVersionNumber.sql") > 0)
})


test_that("Migrations manager runs in package mode", {
  unlink(sqliteFile)
  on.exit(unlink(sqliteFile))
  manager <- DataMigrationManager$new(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = "",
    migrationPath = "migrations",
    packageName = "ResultModelManager"
  )
  expect_true(manager$check())
  checkmate::expect_data_frame(manager$getStatus(), nrows = 2)
  expect_false(all(manager$getStatus()$executed))
  manager$executeMigrations()
  manager$finalize()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  migrations <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM migration")
  checkmate::expect_data_frame(migrations, nrows = 2)

  manager <- DataMigrationManager$new(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = "",
    migrationPath = "migrations",
    packageName = "ResultModelManager"
  )

  checkmate::expect_data_frame(manager$getStatus(), nrows = 2)
  expect_true(all(manager$getStatus()$executed))
  manager$finalize()
})

test_that("Migrations manager runs in folder mode", {
  unlink(connectionDetails$server())
  manager <- DataMigrationManager$new(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    packageTablePrefix = "mg_",
    migrationPath = "migrations",
    packageName = NULL
  )

  expect_true(manager$check())
  manager$executeMigrations()
  manager$finalize()

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  migrations <- DatabaseConnector::renderTranslateQuerySql(connection, "SELECT * FROM mg_migration")
  checkmate::expect_data_frame(migrations, nrows = 2)
})

test_that("Add migration and execute", {
  testthat::skip_on_cran() # Uses a file on disk
  mpath <- file.path("migrations", "sql_server", "Migration_3-test-add.sql")
  write("
  {DEFAULT @moo = moo}
  CREATE TABLE @database_schema.@table_prefix@moo (id INT);", mpath)
  on.exit(unlink(mpath))
  manager <- DataMigrationManager$new(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = "mg_",
    migrationPath = "migrations",
    packageName = NULL
  )
  checkmate::expect_data_frame(manager$getStatus(), nrows = 3)
  expect_false(all(manager$getStatus()$executed))
  manager$executeMigrations()
  expect_true(all(manager$getStatus()$executed))
  manager$finalize()
})


test_that("Add invalid filename", {
  testthat::skip_on_cran() # uses file on disk
  unlink(connectionDetails$server())
  write("", file.path("migrations", "sql_server", "foo.sql"))
  on.exit(unlink(file.path("migrations", "sql_server", "foo.sql")))
  manager <- DataMigrationManager$new(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = "",
    migrationPath = "migrations",
    packageName = NULL
  )
  expect_false(manager$check())
  manager$finalize()
})

test_that("Empty project works", {
  unlink(connectionDetails$server())
  manager <- DataMigrationManager$new(
    connectionDetails = connectionDetails,
    databaseSchema = "main",
    tablePrefix = "",
    migrationPath = tempfile(),
    packageName = NULL
  )
  checkmate::expect_data_frame(manager$getStatus(), nrows = 0)
  expect_true(manager$check())
  manager$finalize()
})
