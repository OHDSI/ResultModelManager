library(testthat)

sqliteFile <- tempfile(fileext = "sqlite")
unlink(sqliteFile)
withr::defer(unlink(sqliteFile), testthat::teardown_env())

connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite", server = sqliteFile)
connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::disconnect(connection)
options(rstudio.connectionObserver.errorsSuppressed = TRUE)
