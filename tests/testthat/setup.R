library(testthat)

sqliteFile <- tempfile(fileext = "sqlite")
.varFile <- ".SQLITE_PATH"
unlink(.varFile)

writeLines(sqliteFile, .varFile)

unlink(sqliteFile)
withr::defer(unlink(sqliteFile), testthat::teardown_env())

connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite",
                                                                server = readLines(".SQLITE_PATH"))
connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::disconnect(connection)
options(rstudio.connectionObserver.errorsSuppressed = TRUE)
