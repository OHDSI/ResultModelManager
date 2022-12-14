library(testthat)

sqliteFile <- tempfile(fileext = "sqlite")
keyring::key_set_with_value("rmm_test_db", password = sqliteFile)

unlink(sqliteFile)
withr::defer(unlink(sqliteFile), testthat::teardown_env())

connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite", server = keyring::key_get("rmm_test_db"))
connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::disconnect(connection)
options(rstudio.connectionObserver.errorsSuppressed = TRUE)
