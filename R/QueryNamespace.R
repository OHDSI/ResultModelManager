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

#' QueryNamespace
#' @export
#' @description
#' Given a results specification and ConnectionHandler instance - this class allow queries to be namespaced within
#' any tables specified within a list of pre-determined tables. This allows the encapsulation of queries, using specific
#' table names in a consistent manner that is striaghtforward to maintain over time.
#' @importFrom fastmap fastmap
#' @examples
#'
#' library(ResultModelManager)
#'
#' # Create some junk test data
#' connectionDetails <-
#'   DatabaseConnector::createConnectionDetails(
#'     server = "test_db.sqlite",
#'     dbms = "sqlite"
#'   )
#'
#' conn <- DatabaseConnector::connect(connectionDetails)
#' DatabaseConnector::insertTable(
#'   connection = conn,
#'   tableName = "cd_cohort",
#'   data = data.frame(
#'     cohort_id = c(1, 2, 3),
#'     cohort_name = c("cohort one", "cohort two", "cohort three"),
#'     json = "{}",
#'     sql = "SELECT 1"
#'   )
#' )
#' DatabaseConnector::disconnect(conn)
#'
#' connectionHandler <- ConnectionHandler$new(connectionDetails = connectionDetails)
#' tableSpecification <- data.frame(
#'   tableName = "cohort",
#'   columnName = c(
#'     "cohort_id",
#'     "cohort_name",
#'     "json",
#'     "sql"
#'   ),
#'   primaryKey = c(TRUE, FALSE, FALSE, FALSE),
#'   dataType = c("int", "varchar", "varchar", "varchar")
#' )
#'
#' cohortNamespace <- QueryNamespace$new(
#'   connectionHandler = connectionHandler,
#'   tableSpecification = tableSpecification,
#'   result_schema = "main",
#'   tablePrefix = "cd_"
#' )
#' sql <- "SELECT * FROM @result_schema.@cohort WHERE cohort_id = @cohort_id"
#' # Returns : "SELECT * FROM main.cd_cohort WHERE cohort_id = @cohort_id"
#' print(cohortNamespace$render(sql))
#' # Returns query result
#' result <- cohortNamespace$queryDb(sql, cohort_id = 1)
#' # cleanup test data
#' unlink("test_db.sqlite")
QueryNamespace <- R6::R6Class(
  classname = "QueryNamespace",
  private = list(
    replacementVars = NULL,
    tableSpecifications = list(),
    connectionHandler = NULL
  ),
  public = list(
    #' @field tablePrefix tablePrefix to use
    tablePrefix = "",
    #' @description
    #' initialize class
    #'
    #' @param connectionHandler  ConnectionHandler instance @seealso[ConnectionHandler]
    #' @param tableSpecification tableSpecification data.frame
    #' @param tablePrefix constant string to prefix all tables with
    #' @param ... additional replacement variables e.g. database_schema, vocabulary_schema etc
    initialize = function(connectionHandler = NULL, tableSpecification = NULL, tablePrefix = "", ...) {
      checkmate::assertString(tablePrefix)
      self$tablePrefix <- tablePrefix
      private$replacementVars <- fastmap::fastmap()
      if (!is.null(connectionHandler)) {
        self$setConnectionHandler(connectionHandler)
      }

      if (!is.null(tableSpecification)) {
        self$addTableSpecification(tableSpecification)
      }
      replaceVars <- list(...)

      if (length(replaceVars)) {
        for (k in names(replaceVars)) {
          self$addReplacementVariable(k, replaceVars[[k]])
        }
      }

      self
    },

    #' Set Connection Handler
    #' @description set connection handler object for object
    #' @param connectionHandler ConnectionHandler instance
    setConnectionHandler = function(connectionHandler) {
      checkmate::assertR6(connectionHandler, "ConnectionHandler")
      private$connectionHandler <- connectionHandler
      invisible(NULL)
    },

    #' Get connection handler
    #' @description get connection handler obeject or throw error if not set
    getConnectionHandler = function() {
      if (is.null(private$connectionHandler)) {
        stop("ConnectionHandler not set")
      }

      return(private$connectionHandler)
    },

    #' @description
    #' add a variable to automatically be replaced in query strings (e.g. @database_schema.@table_name becomes 'database_schema.table_1')
    #' @param key variable name string (without @) to be replaced, eg. "table_name"
    #' @param value atomic value for replacement
    #' @param replace if a variable of the same key is found, overwrite it
    addReplacementVariable = function(key, value, replace = FALSE) {
      checkmate::assertString(key, min.chars = 1)
      checkmate::assertAtomic(value)
      if (!replace && !is.null(private$replacementVars$get(key))) {
        stop(paste("Key", key, "already found in namespace"))
      }

      private$replacementVars$set(key, value)
      invisible(NULL)
    },

    #' add table specification
    #' @description
    #' add a variable to automatically be replaced in query strings (e.g. @database_schema.@table_name becomes
    #' 'database_schema.table_1')
    #' @param tableSpecification table specification data.frame conforming to column names tableName, columnName, dataType and primaryKey
    #' @param useTablePrefix prefix the results with the tablePrefix (TRUE)
    #' @param tablePrefix prefix string - defaults to class variable set during initialization
    #' @param replace replace existing variables of the same name
    addTableSpecification = function(tableSpecification, useTablePrefix = TRUE, tablePrefix = self$tablePrefix, replace = TRUE) {
      checkmate::assertString(tablePrefix)
      assertSpecificationColumns(colnames(tableSpecification))
      for (tableName in tableSpecification$tableName %>% unique()) {
        replacementVar <- tableName
        if (useTablePrefix) {
          replacementVar <- paste0(tablePrefix, replacementVar)
        }

        self$addReplacementVariable(tableName, replacementVar, replace = replace)
      }
      invisible(NULL)
    },

    #' Render
    #' @description
    #' Call to SqlRender::render replacing names stored in this class
    #' @param sql query string
    #' @param ... additional variables to be passed to SqlRender::render - will overwrite anything in namespace
    render = function(sql, ...) {
      params <- private$replacementVars$as_list()

      addVars <- list(...)

      for (k in names(addVars)) {
        params[[k]] <- addVars[[k]]
      }

      params$sql <- sql
      params$warnOnMissingParameters <- FALSE
      do.call(SqlRender::render, params)
    },

    #' query Sql
    #' @description
    #' Call to
    #' @param sql query string
    #' @param ... additional variables to send to SqlRender::render
    queryDb = function(sql, ...) {
      ch <- self$getConnectionHandler()
      sql <- self$render(sql, ...)
      ch$queryDb(sql)
    },

    #' execute Sql
    #' @description
    #' Call to execute sql within namespaced queries
    #' @param sql query string
    #' @param ... additional variables to send to SqlRender::render
    executeSql = function(sql, ...) {
      ch <- self$getConnectionHandler()
      sql <- self$render(sql, ...)
      ch$executeSql(sql)
    },

    #' get vars
    #' @description
    #' returns full list of variables that will be replaced
    getVars = function() {
      return(private$replacementVars$as_list())
    },

    #' Destruct object
    #' @description
    #' Close connections etc
    finalize = function() {
      private$connectionHandler$finalize()
      invisible(NULL)
    }
  )
)

#' Create query namespace
#'
#' @export
#' @description
#' Create a QueryNamespace instance from either a connection handler or a connectionDetails object
#' Allows construction with various options not handled by QueryNamespace$new
#'
#' Note - currently not supported is having multiple table prefixes for multiple table namespaces
#' @inheritParams uploadResults
#' @param resultModelSpecificationPath (optional) csv file or files for tableSpecifications - must conform to table spec
#'                                     format.
#' @param usePooledConnection          Use Pooled database connection instead of standard DatabaseConnector single
#'                                     connection.
#' @param connectionHandler            ResultModelManager ConnectionHandler or PooledConnectionHandler instance
#' @param tableSpecification           Table specfication data.frame
#' @param snakeCaseToCamelCase         convert snakecase results to camelCase field names (TRUE by default)
#' @param ...                          Elipsis - use for any additional string keys to replace
createQueryNamespace <- function(connectionDetails = NULL,
                                 connectionHandler = NULL,
                                 usePooledConnection = FALSE,
                                 tableSpecification = NULL,
                                 resultModelSpecificationPath = NULL,
                                 tablePrefix = "",
                                 snakeCaseToCamelCase = TRUE,
                                 ...) {
  checkmate::assertClass(connectionDetails, "ConnectionDetails", null.ok = TRUE)
  checkmate::assertClass(connectionHandler, "ConnectionHandler", null.ok = TRUE)
  checkmate::assertLogical(usePooledConnection)
  checkmate::assertDataFrame(tableSpecification, null.ok = TRUE)
  checkmate::assertString(tablePrefix)

  if (!is.null(tableSpecification)) {
    assertSpecificationColumns(colnames(tableSpecification))
  }

  if (is.null(connectionDetails) && is.null(connectionHandler)) {
    stop("Must provide ConnectionDetails or ConnectionHandler instance")
  }

  if (!is.null(resultModelSpecificationPath)) {
    if (is.null(tableSpecification)) {
      tableSpecification <- data.frame()
    }

    # Create a merged specification from all spec files provided.
    tableSpecification <- lapply(
      resultModelSpecificationPath,
      loadResultsDataModelSpecifications
    ) %>%
      dplyr::bind_rows(tableSpecification)
  }

  if (!is.null(connectionDetails)) {
    if (usePooledConnection) {
      connectionHandler <- PooledConnectionHandler$new(connectionDetails)
    } else {
      connectionHandler <- ConnectionHandler$new(connectionDetails)
    }
  }
  connectionHandler$snakeCaseToCamelCase <- snakeCaseToCamelCase

  qns <- QueryNamespace$new(connectionHandler,
    tableSpecification = tableSpecification,
    tablePrefix = tablePrefix,
    ...
  )

  return(qns)
}
