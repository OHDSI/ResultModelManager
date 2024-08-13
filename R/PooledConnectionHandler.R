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
#' Note that a side effect of using this is that each call to this increments the .GlobalEnv attribute `RMMPooledHandlerCount`
#' @importFrom pool dbPool poolClose
#' @importFrom DBI dbIsValid
#' @importFrom withr defer
#'
#' @export PooledConnectionHandler
PooledConnectionHandler <- R6::R6Class(
  classname = "PooledConnectionHandler",
  inherit = ConnectionHandler,
  private = list(
    dbConnectArgs = NULL,
    .handlerId = 1,
    activeConnectionRefs = list(),
    .returnPooledObject = function(frame) {
      if (!is.null(attr(frame, self$getCheckedOutConnectionPath(), exact = TRUE))) {
        pool::poolReturn(attr(frame, self$getCheckedOutConnectionPath(), exact = TRUE))
        attr(frame, self$getCheckedOutConnectionPath()) <- NULL
      }
    }
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
      handlerCount <- attr(.GlobalEnv, "RMMPooledHandlerCount", exact = TRUE)
      private$.handlerId <- ifelse(is.null(handlerCount), 1, handlerCount + 1)

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

    #' Used for getting a checked out connection from a given environment (if one exists)
    #' @param .deferedFrame  defaults to the parent frame of the calling block.
    getCheckedOutConnectionPath = function() {
      return(paste0("RMMcheckedOutConnection", private$.handlerId))
    },

    #' Get Connection
    #' @description
    #' Returns a connection from the pool
    #' When the desired frame exits, the connection will be returned to the pool
    #' As a side effect, the connection is stored as an attribute within the calling frame (e.g. the same function) to prevent multiple
    #' connections being spawned, which limits performance.
    #'
    #' If you call this somewhere you need to think about returning the object or you may create a connection that
    #' is never returned to the pool.
    #' @param .deferedFrame  defaults to the parent frame of the calling block.
    getConnection = function(.deferedFrame = parent.frame(n = 2)) {
      checkmate::assertEnvironment(.deferedFrame)
      if (!self$dbIsValid()) {
        self$initConnection()
      }

      # Equivalent to pool::localCheckout but uses only a single connection in tha calling frame
      if (is.null(attr(.deferedFrame, self$getCheckedOutConnectionPath(), exact = TRUE))) {
        attr(.deferedFrame, self$getCheckedOutConnectionPath()) <- pool::poolCheckout(pool = self$con)

        # Store reference to active frame
        private$activeConnectionRefs[[length(private$activeConnectionRefs) + 1]] <- .deferedFrame
        withr::defer(
          {
            private$.returnPooledObject(.deferedFrame)
          },
          envir = .deferedFrame
        )
      }

      return(attr(.deferedFrame, self$getCheckedOutConnectionPath(), exact = TRUE))
    },

    #' get dbms
    #' @description Get the dbms type of the connection
    dbms = function() {
      return(self$connectionDetails$dbms)
    },

    #' Close Connection
    #' @description
    #' Overrides ConnectionHandler Call - closes all active connections called with getConnection
    closeConnection = function() {
      if (self$dbIsValid()) {
        # Return any still active pooled objects
        lapply(private$activeConnectionRefs, private$.returnPooledObject)
        pool::poolClose(pool = self$con)
      }
      self$isActive <- FALSE
      self$con <- NULL
    },


    #' queryDb
    #' @description
    #' query database and return the resulting data.frame
    #'
    #' If environment variable LIMIT_ROW_COUNT is set Returned rows are limited to this value (no default)
    #' Limit row count is intended for web applications that may cause a denial of service if they consume too many
    #' resources.
    #' @param sql                                   sql query string
    #' @param snakeCaseToCamelCase                  (Optional) Boolean. return the results columns in camel case (default)
    #' @param overrideRowLimit                      (Optional) Boolean. In some cases, where row limit is enforced on the system
    #'                                              You may wish to ignore it.
    #' @param ...                                   Additional query parameters
    #' @returns boolean TRUE if connection is valid
    queryDb = function(sql, snakeCaseToCamelCase = self$snakeCaseToCamelCase, overrideRowLimit = FALSE, ...) {
      # Limit row count is intended for web applications that may cause a denial of service if they consume too many
      # resources.
      if (!self$isActive) {
        self$initConnection()
      }

      sql <- .limitRowCount(sql, overrideRowLimit)
      sql <- self$renderTranslateSql(sql, ...)
      conn <- pool::poolCheckout(self$con)
      on.exit(pool::poolReturn(conn))
      tryCatch(
        {
          data <- self$queryFunction(sql, snakeCaseToCamelCase = snakeCaseToCamelCase, connection = conn)
        },
        error = function(error) {
          if (self$dbms() %in% c("postgresql", "redshift")) {
            self$executeFunction("ABORT;", conn)
          }
          stop(paste0(sql, "\n\n", error))
        }
      )

      return(data)
    },

    #' executeSql
    #' @description
    #' execute set of database queries
    #' @param sql                                   sql query string
    #' @param ...                                   Additional query parameters
    executeSql = function(sql, ...) {
      if (!self$isActive) {
        self$initConnection()
      }

      sql <- self$renderTranslateSql(sql, ...)
      conn <- pool::poolCheckout(self$con)
      on.exit(pool::poolReturn(conn))
      tryCatch(
        {
          self$executeFunction(sql, conn)
        },
        error = function(error) {
          if (self$dbms() %in% c("postgresql", "redshift")) {
            self$executeFunction("ABORT;", conn)
          }
          stop(paste0(sql, "\n\n", error))
        }
      )

      return(invisible(NULL))
    },


    #' query Function
    #' @description
    #' Overrides ConnectionHandler Call. Does not translate or render sql.
    #' @param sql                                   sql query string
    #' @param connection                                   db connection assumes pooling is handled outside of call
    #' @param snakeCaseToCamelCase                  (Optional) Boolean. return the results columns in camel case (default)
    queryFunction = function(sql, snakeCaseToCamelCase = self$snakeCaseToCamelCase, connection) {
      data <- DatabaseConnector::dbGetQuery(connection, sql, translate = FALSE)
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
    #' @param connection                                  DatabaseConnector connection. Assumes pooling is handled outside of call
    executeFunction = function(sql, connection) {
      DatabaseConnector::dbExecute(connection, sql, translate = FALSE)
    }
  )
)
