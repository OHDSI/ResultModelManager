library(testthat)

sqliteFile <- tempfile(fileext = "sqlite")
connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite", server = sqliteFile)
connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::disconnect(connection)
