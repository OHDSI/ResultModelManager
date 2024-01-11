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

  if (Sys.getenv("CDM5_POSTGRESQL_SERVER") != "") {
    args <- .DBCToDBIArgs$postgresql(testDatabaseConnectionDetails)
    expect_list(args)
  }
})
