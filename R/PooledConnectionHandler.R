# Copyright 2023 Observational Health Data Sciences and Informatics
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


#' Pooled Connection Handler
#'
#' @description
#' Transparently works the same way as a standard connection handler but stores pooled connections.
#' Useful for long running applications that serve multiple concurrent requests.
#'
#' @importFrom pool dbPool poolClose
#' @importFrom DBI dbIsValid
#'
#' @export PooledConnectionHandler
PooledConnectionHandler <- R6::R6Class(
  classname = "PooledConnectionHandler",
  inherit = ConnectionHandler,
  private = list(
    poolArgs = NULL
  ),
  public = list(
    #' @param ...                           Elisis @seealso[ConnectionHandler]
    #' @param poolArgs                      Optional arguments to call pool::dbPool overrides default usage of connectionDetails
    initialize = function(..., poolArgs = NULL) {
      private$poolArgs <- poolArgs
      super$initialize(...)
    },
    #' initialize pooled db connection
    #' @description
    #' Overrides ConnectionHandler Call
    initConnection = function() {
      if (self$isActive) {
        warning("Closing existing connection")
        self$closeConnection()
      }

      if (is.null(private$poolArgs)) {
        self$con <- do.call(pool::dbPool, poolArgs)
      } else {
        self$con <- pool::dbPool(
          drv = private$driver,
          dbms = self$connectionDetails$dbms,
          server = self$connectionDetails$server(),
          port = self$connectionDetails$port(),
          user = self$connectionDetails$user(),
          password = self$connectionDetails$password(),
          connectionString = self$connectionDetails$connectionString()
        )
      }
      self$isActive <- TRUE
    },

    #' get dbms
    #' @description Get the dbms type of the connection
    dbms = function() {
      conn <- pool::poolCheckout(self$getConnection())
      on.exit(pool::poolReturn(conn))
      DatabaseConnector::dbms(self$getConnection())
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
      on.exit(pool::poolReturn(conn))
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
      on.exit(pool::poolReturn(conn))
      DatabaseConnector::dbExecute(conn, sql, translate = FALSE)
    }
  )
)
