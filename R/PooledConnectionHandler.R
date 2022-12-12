# Copyright 2022 Observational Health Data Sciences and Informatics
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
  public = list(
    #' @param ...                           Elisis @seealso[ConnectionHandler]
    initialize = function(...) {
      ## Note this function is just a dummy because of roxygen
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

      self$con <- pool::dbPool(
        drv = DatabaseConnector::DatabaseConnectorDriver(),
        dbms = self$connectionDetails$dbms,
        server = self$connectionDetails$server(),
        port = self$connectionDetails$port(),
        user = self$connectionDetails$user(),
        password = self$connectionDetails$password()
      )
      self$isActive <- TRUE
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
    queryFunction = function(sql, snakeCaseToCamelCase = TRUE) {
      data <- DatabaseConnector::dbGetQuery(self$getConnection(), sql, translate = FALSE)
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      } else {
        colnames(data) <- toupper(colnames(data))
      }
      return(data)
    },

    #' execute Function
    #' @description
    #' exec query Function that can be overriden with subclasses (e.g. use different base function or intercept query)
    #' Does not translate or render sql.
    #' @param sql                                   sql query string
    #' @param translate                             translate the sql query or not
    executeFunction = function(sql, translate = FALSE) {
      DatabaseConnector::dbExecute(self$getConnection(), sql, translate = translate)
    }
  )
)
