# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


requiredPackage <- function(packageName) {
  packageLoc <- tryCatch(
    find.package(packageName),
    errorr = function(...) {
      return(NULL)
    }
  )

  if (is.null(packageLoc)) {
    stop(paste("Package", packageName, "is required please use install.packages to install"))
  }
}

# map a ConnectionDetails objects and translates them to DBI pool args
.DBCToDBIArgs <- list(
  "sqlite" = function(cd) {
    requiredPackage("RSQLite")
    list(
      drv = RSQLite::SQLite(),
      dbname = cd$server()
    )
  },
  "postgresql" = function(cd) {
    requiredPackage("RPostgres")
    host <- strsplit(cd$server(), "/")[[1]][1]
    dbname <- strsplit(cd$server(), "/")[[1]][2]
    list(
      drv = RPostgres::Postgres(),
      dbname = dbname,
      host = host,
      user = cd$user(),
      port = cd$port(),
      password = cd$password(),
      options = "sslmode=require"
    )
  },
  "duckdb" = function(cd) {
    requiredPackage("duckdb")
    list(
      drv = duckdb::duckdb(),
      dbdir = cd$server()
    )
  },
  "jdbc" = function(cd) {
    ParallelLogger::logInfo("Using DatabaseConnector jdbc driver.")
    # Set java stack size on linux to workaround overflow problem
    if (is.null(getOption("java.parameters")) & .Platform$OS.type == "linux") {
      ParallelLogger::logInfo("Unix system detected and no java.parameters option set. Fixing stack with -Xss5m")
      options(java.parameters = "-Xss5m")
    }

    list(
      drv = DatabaseConnector::DatabaseConnectorDriver(),
      dbms = cd$dbms,
      server = cd$server(),
      port = cd$port(),
      user = cd$user(),
      password = cd$password(),
      connectionString = cd$connectionString()
    )
  }
)


#' Pooled Connection Handler
#'
#' @description
#' Transparently works the same way as a standard connection handler but stores pooled connections.
#' Useful for long running applications that serve multiple concurrent requests.
#'
#' @importFrom pool dbPool poolClose
#' @importFrom DBI dbIsValid
#' @importFrom withr defer
#'
#' @export PooledConnectionHandler
PooledConnectionHandler <- R6::R6Class(
  classname = "PooledConnectionHandler",
  inherit = ConnectionHandler,
  private = list(
    dbConnectArgs = NULL
  ),
  public = list(
    #' @param connectionDetails             DatabaseConnector::connectionDetails class
    #' @param loadConnection                Boolean option to load connection right away
    #' @param snakeCaseToCamelCase          (Optional) Boolean. return the results columns in camel case (default)
    #' @param dbConnectArgs                 Optional arguments to call pool::dbPool overrides default usage of connectionDetails
    #' @param forceJdbcConnection           Force JDBC connection (requires using DatabaseConnector ConnectionDetails)
    initialize = function(connectionDetails = NULL,
                          snakeCaseToCamelCase = TRUE,
                          loadConnection = TRUE,
                          dbConnectArgs = NULL,
                          forceJdbcConnection = TRUE) {
      checkmate::assertList(dbConnectArgs, null.ok = TRUE)
      checkmate::assertClass(connectionDetails, "ConnectionDetails", null.ok = TRUE)

      if (is.null(connectionDetails) && is.null(dbConnectArgs)) {
        stop("Must either specify usage of poolArgs or DatabaseConnector connection details object")
      }

      if (is.null(dbConnectArgs)) {
        if (!forceJdbcConnection && connectionDetails$dbms %in% names(.DBCToDBIArgs)) {
          fun <- .DBCToDBIArgs[[connectionDetails$dbms]]
        } else {
          fun <- .DBCToDBIArgs$jdbc
        }
        dbConnectArgs <- fun(connectionDetails)
      }

      private$dbConnectArgs <- dbConnectArgs
      self$connectionDetails <- connectionDetails
      self$snakeCaseToCamelCase <- snakeCaseToCamelCase
      if (loadConnection) {
        self$initConnection()
      }
    },
    #' initialize pooled db connection
    #' @description
    #' Overrides ConnectionHandler Call
    initConnection = function() {
      if (self$isActive) {
        warning("Closing existing connection")
        self$closeConnection()
      }
      ParallelLogger::logInfo("Initalizing pooled connection")
      self$con <- do.call(pool::dbPool, private$dbConnectArgs)

      self$isActive <- TRUE
    },

    #' Get Connection
    #' @description
    #' Returns a connection from the pool
    #' When the desired frame exits, the connection will be returned to the pool
    #' @param .deferedFrame  defaults to the parent frame of the calling block.
    getConnection = function(.deferedFrame = parent.frame(n = 2)) {
      conn <- pool::poolCheckout(super$getConnection())
      withr::defer(pool::poolReturn(conn), envir = .deferedFrame)
      return(conn)
    },

    #' get dbms
    #' @description Get the dbms type of the connection
    dbms = function() {
      conn <- self$getConnection()
      DatabaseConnector::dbms(conn)
    },

    #' Close Connection
    #' @description
    #' Overrides ConnectionHandler Call
    closeConnection = function() {
      if (self$dbIsValid()) {
        pool::poolClose(pool = self$con)
      }
      self$isActive <- FALSE
      self$con <- NULL
    },

    #' query Function
    #' @description
    #' Overrides ConnectionHandler Call. Does not translate or render sql.
    #' @param sql                                   sql query string
    #' @param snakeCaseToCamelCase                  (Optional) Boolean. return the results columns in camel case (default)
    queryFunction = function(sql, snakeCaseToCamelCase = self$snakeCaseToCamelCase) {
      conn <- self$getConnection()
      data <- DatabaseConnector::dbGetQuery(conn, sql, translate = FALSE)
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      } else {
        colnames(data) <- toupper(colnames(data))
      }
      return(data)
    },

    #' query Function
    #' @description
    #' Overrides ConnectionHandler Call. Does not translate or render sql.
    #' @param sql                                   sql query string
    executeFunction = function(sql) {
      conn <- self$getConnection()
      DatabaseConnector::dbExecute(conn, sql, translate = FALSE)
    }
  )
)
