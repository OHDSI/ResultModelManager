library(testthat)

keyList <- keyring::keyring_list()

if (!"hades-test-keyring" %in% keyList$keyring)
  keyring::keyring_create("hades-test-keyring", password = "")

sqliteFile <- tempfile(fileext = "sqlite")
keyring::key_set_with_value("rmm_test_db", password = sqliteFile, keyring = "hades-test-keyring")

unlink(sqliteFile)
withr::defer(unlink(sqliteFile), testthat::teardown_env())

connectionDetails <- DatabaseConnector::createConnectionDetails("sqlite",
                                                                server = keyring::key_get("rmm_test_db",
                                                                                          keyring = "test-keyring"))
connection <- DatabaseConnector::connect(connectionDetails)
DatabaseConnector::disconnect(connection)
options(rstudio.connectionObserver.errorsSuppressed = TRUE)
