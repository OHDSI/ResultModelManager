# Copyright 2022 Observational Health Data Sciences and Informatics
#
# This file is part of CemConnector
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

#' ConnectionHandler
#' @description
#' Class for handling DatabaseConnector:connection objects with consistent R6 interfaces for pooled and non-pooled connections.
#' Allows a connection to cleanly be opened and closed and stored within class/object variables
#'
#' @field connectionDetails             DatabaseConnector connectionDetails object
#' @field con                           DatabaseConnector connection object
#' @field isActive                      Is connection active or not
#' @import checkmate
#' @import R6
#' @importFrom DBI dbIsValid
#' @importFrom SqlRender render translate
#'
#'
#'
#' @export
ConnectionHandler <- R6::R6Class(
  "ConnectionHandler",
  public = list(
    connectionDetails = NULL,
    con = NULL,
    isActive = FALSE,

    #' Initialize Class
    #' @param connectionDetails             DatabaseConnector::connectionDetails class
    #' @param loadConnection                Boolean option to load connection right away
    initialize = function(connectionDetails, loadConnection = TRUE) {
      checkmate::assertClass(connectionDetails, "connectionDetails")
      self$connectionDetails <- connectionDetails

      if (loadConnection) {
        self$initConnection()
      }
    },

    #' Render Translate Sql.
    #' @description
    #' Masked call to SqlRender
    #' @param sql                     Sql query string
    #' @param ...                     Elipsis
    renderTranslateSql = function(sql, ...) {
      sql <- SqlRender::render(sql = sql, ...)
      SqlRender::translate(sql, targetDialect = self$connectionDetails$dbms)
    },

    #' initConnection
    #' @description
    #' Load connection
    initConnection = function() {
      if (self$isActive) {
        warning("Closing existing connection")
        self$closeConnection()
      }
      self$con <- DatabaseConnector::connect(connectionDetails = self$connectionDetails)
      self$isActive <- TRUE
    },

    #' Get Connection
    #' @description
    #' Returns connection for use with standard DatabaseConnector calls.
    #' Connects automatically if it isn't yet loaded
    #' @returns DatabaseConnector Connection instance
    getConnection = function() {
      if (is.null(self$con))
        self$initConnection()

      if (!self$dbIsValid())
        self$initConnection()

      return(self$con)
    },

    #' close Connection
    #' @description
    #' Closes connection (if active)
    closeConnection = function() {
      if (self$dbIsValid()) {
        DatabaseConnector::disconnect(self$con)
      }
      self$isActive <- FALSE
      self$con <- NULL
    },

    #' close Connection
    #' @description
    #' Closes connection (if active)
    finalize = function() {
      if (self$isActive & self$dbIsValid()) {
        self$closeConnection()
      }
    },

    #' db Is Valid
    #' @description
    #' Masks call to DBI::dbIsValid. Returns False if connection is NULL
    #' @returns boolean TRUE if connection is valid
    dbIsValid = function() {
      if (is.null(con)) {
        return(FALSE)
      }
      return(DBI::dbIsValid(dbObj = con))
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
    queryDb = function(sql, snakeCaseToCamelCase = TRUE, overrideRowLimit = FALSE, ...) {
      # Limit row count is intended for web applications that may cause a denial of service if they consume too many
      # resources.
      limitRowCount <- as.integer(Sys.getenv("LIMIT_ROW_COUNT"))
      if (!is.na(limitRowCount) & limitRowCount > 0 & !overrideRowLimit) {
        sql <- SqlRender::render("SELECT TOP @limit_row_count * FROM (@query) result;",
                                 query = gsub(";$", "", sql), # Remove last semi-colon
                                   limit_row_count = limitRowCount
        )
      }
      sql <- self$renderTranslateSql(sql, ...)

      tryCatch(
        {
          data <- self$queryFunction(sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
        },
        error = function(error) {
          if (self$connectionDetails$dbms %in% c("postgresql", "redshift")) {
            DatabaseConnector::dbExecute(self$getConnection(), "ABORT;")
          }
          print(sql)
          stop(error)
        }
      )

      return(dplyr::as_tibble(data))
    },
    #' executeSql
    #' @description
    #' execute set of database queries
    #' @param sql                                   sql query string
    #' @param ...                                   Additional query parameters
    executeSql = function(sql, ...) {
      sql <- self$renderTranslateSql(sql, ...)

      tryCatch(
        {
          data <- self$executeFunction(sql)
        },
        error = function(error) {
          if (self$connectionDetails$dbms %in% c("postgresql", "redshift")) {
            DatabaseConnector::dbExecute(self$getConnection(), "ABORT;")
          }
          print(sql)
          stop(error)
        }
      )

      return(invisible(NULL))
    },


    #' query Function
    #' @description
    #' queryFunction that can be overriden with subclasses (e.g. use different base function or intercept query)
    #' @param sql                                   sql query string
    #' @param snakeCaseToCamelCase                  (Optional) Boolean. return the results columns in camel case (default)
    queryFunction = function(sql, snakeCaseToCamelCase = TRUE) {
      DatabaseConnector::querySql(self$getConnection(), sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
    },

    #' execute Function
    #' @description
    #' exec query Function that can be overriden with subclasses (e.g. use different base function or intercept query)
    #' @param sql                                   sql query string
    executeFunction = function(sql) {
      DatabaseConnector::executeSql(self$getConnection(), sql)
    }
  )
)

#' Pooled Connection Handler
#' Transparently works the same way as a standard connection handler but stores pooled connections.
#' Useful for long running applications that serve multiple concurrent requests.
#'
#' @importFrom pool dbPool poolClose
#' @importFrom DBI dbIsValid
#'
#'
#' @export
PooledConnectionHandler <- R6::R6Class(
  "PooledConnectionHandler",
  inherit = ConnectionHandler,
  public = list(
    #' Init connection
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
    #' Overrides ConnectionHandler Call
    #' @param sql                                   sql query string
    #' @param snakeCaseToCamelCase                  (Optional) Boolean. return the results columns in camel case (default)
    queryFunction = function(sql, snakeCaseToCamelCase = TRUE) {
      data <- DatabaseConnector::dbGetQuery(self$getConnection(), sql)
      if (snakeCaseToCamelCase) {
        colnames(data) <- SqlRender::snakeCaseToCamelCase(colnames(data))
      } else {
        colnames(data) <- toupper(colnames(data))
      }
      return(data)
    }
  )
)
