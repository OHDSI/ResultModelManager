test_that("internal connection handlers", {
  cd <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = ":memory:")
  args <- .DBCToDBIArgs$jdbc(cd)
  expect_list(args)
  conn <- do.call(pool::dbPool, args)
  expect_class(conn, "Pool")
  pool::poolClose(pool = conn)

  args <- .DBCToDBIArgs$sqlite(cd)
  expect_list(.DBCToDBIArgs$sqlite(cd))
  conn <- do.call(pool::dbPool, args)
  expect_class(conn, "Pool")
  pool::poolClose(pool = conn)


  cd <- DatabaseConnector::createConnectionDetails(dbms = "duckdb", server = ":memory:")
  args <- .DBCToDBIArgs$duckdb(cd)
  expect_list(args)
  conn <- do.call(pool::dbPool, args)
  expect_class(conn, "Pool")
  pool::poolClose(pool = conn)
})

test_that("Maintained connection consistency", {
  testthat::expect_null(attr(parent.frame(n = 1), "RMMcheckedOutConnection1"))
  pch <- PooledConnectionHandler$new(testDatabaseConnectionDetails)
  conn <- pch$getConnection()
  # This test is a proxy for when the frame exits the pooled object will be returned
  # Note, this is not guaranteed
  testthat::expect_identical(conn, attr(parent.frame(n = 1), "RMMcheckedOutConnection1"))
})

test_that("Abort Connections", {
  skip_if_results_db_not_available()
  pch <- PooledConnectionHandler$new(testDatabaseConnectionDetails)
  expect_error(pch$queryDb("BROKEN"))
  expect_equal(pch$queryDb("SELECT 1 as c")$c, 1)
  expect_error(pch$executeSql("MORE BROKEN"))
  pch$executeSql("CREATE TABLE #@rnd (id int); DROP TABLE #@rnd;", rnd = testSchema)
})
