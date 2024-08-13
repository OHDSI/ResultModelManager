# Copyright 2024 Observational Health Data Sciences and Informatics
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

# Limit row count is intended for web applications that may cause a denial of service
.limitRowCount <- function(sql, overrideRowLimit) {
  limitRowCount <- as.integer(Sys.getenv("LIMIT_ROW_COUNT"))
  if (!is.na(limitRowCount) &
    limitRowCount > 0 &
    !overrideRowLimit) {
    sql <- SqlRender::render("SELECT TOP @limit_row_count * FROM (@query) result;",
      query = gsub(";$", "", sql), # Remove last semi-colon
      limit_row_count = limitRowCount
    )
  }
  return(sql)
}

#' ConnectionHandler
#' @description
#' Class for handling DatabaseConnector:connection objects with consistent R6 interfaces for pooled and non-pooled connections.
#' Allows a connection to cleanly be opened and closed and stored within class/object variables
#'
#' @field connectionDetails             DatabaseConnector connectionDetails object
#' @field con                           DatabaseConnector connection object
#' @field isActive                      Is connection active or not#'
#' @field snakeCaseToCamelCase          (Optional) Boolean. return the results columns in camel case (default)
#'
#' @import checkmate
#' @import R6
#' @importFrom DBI dbIsValid
#' @importFrom SqlRender render translate
#' @importFrom dbplyr in_schema
#'
#' @export ConnectionHandler
ConnectionHandler <- R6::R6Class(
  classname = "ConnectionHandler",
  private = list(
    .dbms = ""
  ),
  public = list(
    connectionDetails = NULL,
    con = NULL,
    isActive = FALSE,
    snakeCaseToCamelCase = TRUE,
    #'
    #' @param connectionDetails             DatabaseConnector::connectionDetails class
    #' @param loadConnection                Boolean option to load connection right away
    #' @param snakeCaseToCamelCase          (Optional) Boolean. return the results columns in camel case (default)
    initialize = function(connectionDetails, loadConnection = TRUE, snakeCaseToCamelCase = TRUE) {
      checkmate::assertClass(connectionDetails, "ConnectionDetails")
      self$connectionDetails <- connectionDetails
      self$snakeCaseToCamelCase <- snakeCaseToCamelCase
      if (loadConnection) {
        self$initConnection()
      }
      private$.dbms <- connectionDetails$dbms
    },

    #' get dbms
    #' @description Get the dbms type of the connection
    dbms = function() {
      private$.dbms
    },

    #' get table
    #' @description get a dplyr table object (i.e. lazy loaded)
    #' @param table                     table name
    #' @param databaseSchema            databaseSchema to which table belongs
    tbl = function(table, databaseSchema = NULL) {
      checkmate::assertString(table)
      checkmate::assertString(databaseSchema, null.ok = TRUE)
      if (!is.null(databaseSchema)) {
        table <- dbplyr::in_schema(databaseSchema, table)
      }

      dplyr::tbl(self$getConnection(), table)
    },
    #' Render Translate Sql.
    #' @description
    #' Masked call to SqlRender
    #' @param sql                     Sql query string
    #' @param ...                     Elipsis
    renderTranslateSql = function(sql, ...) {
      mustTranslate <- TRUE
      if (isTRUE(attr(sql, "sqlDialect", TRUE) == gsub(" ", "_", self$dbms()))) {
        mustTranslate <- FALSE
      }

      sql <- SqlRender::render(sql = sql, ...)
      # Only translate if translate is needed.
      if (mustTranslate) {
        sql <- SqlRender::translate(sql, targetDialect = self$dbms())
      }
      return(sql)
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
      if (!self$dbIsValid()) {
        self$initConnection()
      }

      return(self$con)
    },

    #' close Connection
    #' @description
    #' Closes connection (if active)
    closeConnection = function() {
      if (self$dbIsValid()) {
        ParallelLogger::logInfo("Closing database connection")
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
      if (is.null(self$con)) {
        return(FALSE)
      }
      return(DBI::dbIsValid(dbObj = self$con))
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
      sql <- .limitRowCount(sql, overrideRowLimit)
      sql <- self$renderTranslateSql(sql, ...)

      tryCatch(
        {
          data <- self$queryFunction(sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
        },
        error = function(error) {
          if (self$dbms() %in% c("postgresql", "redshift")) {
            DatabaseConnector::dbExecute(self$getConnection(), "ABORT;")
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
      sql <- self$renderTranslateSql(sql, ...)

      tryCatch(
        {
          self$executeFunction(sql)
        },
        error = function(error) {
          if (self$dbms() %in% c("postgresql", "redshift")) {
            self$executeFunction("ABORT;")
          }
          stop(paste0(sql, "\n\n", error))
        }
      )

      return(invisible(NULL))
    },


    #' query Function
    #' @description
    #' queryFunction that can be overriden with subclasses (e.g. use different base function or intercept query)
    #' Does not translate or render sql.
    #' @param sql                                   sql query string
    #' @param snakeCaseToCamelCase                  (Optional) Boolean. return the results columns in camel case (default)
    #' @param connection                            (Optional) connection object
    queryFunction = function(sql, snakeCaseToCamelCase = self$snakeCaseToCamelCase, connection = self$getConnection()) {
      DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = snakeCaseToCamelCase)
    },

    #' execute Function
    #' @description
    #' exec query Function that can be overriden with subclasses (e.g. use different base function or intercept query)
    #' Does not translate or render sql.
    #' @param sql                                   sql query string
    #' @param connection                            connection object
    executeFunction = function(sql, connection = self$getConnection()) {
      DatabaseConnector::executeSql(connection, sql)
    }
  )
)
