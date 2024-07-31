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
  testthat::expect_identical(conn, attr(parent.frame(n = 1), "RMMcheckedOutConnection1"))
})
