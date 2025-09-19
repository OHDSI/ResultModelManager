# Copyright 2025 Observational Health Data Sciences and Informatics
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


.typeCheckVariable <- function(varName, value, typeStr, isArray) {
  # Map type strings to checking functions
  typeChecks <- list(
    INT = function(x) is.numeric(x) && all(x == as.integer(x)),
    BIGINT = function(x) is.numeric(x) && all(x %% 1 == 0),
    CHAR = function(x) is.character(x),
    VARCHAR = function(x) is.character(x),
    TEXT = function(x) is.character(x),
    NUMERIC = function(x) is.numeric(x),
    BOOLEAN = function (x) is.logical(x) || (is.numeric(x) && all(x %in% c(0, 1)))
  )

  # Check type exists
  baseType <- gsub("\\[\\]", "", typeStr)
  if (!baseType %in% names(typeChecks)) {
    stop(sprintf("Unknown type check: %s", typeStr))
  }
  checkFun <- typeChecks[[baseType]]

  # Check presence
  if (is.null(value)) {
    stop(sprintf("Required variable '%s' (type check %s) not supplied to render().", varName, typeStr))
  }
  # Check NA
  if (any(is.na(value))) {
    stop(sprintf("Variable '%s' (type check %s) contains NA values, which are not allowed.", varName, typeStr))
  }

  # Array vs scalar
  if (isArray) {
    if (!checkFun(value)) {
      stop(sprintf("Variable '%s' must be an array of type %s.", varName, baseType))
    }
    # Must be vector of length >= 1
    if (!(is.vector(value) && length(value) >= 1)) {
      stop(sprintf("Variable '%s' must be a non-empty vector of type %s.", varName, baseType))
    }
  } else {
    if (!checkFun(value) || length(value) != 1) {
      stop(sprintf("Variable '%s' must be a scalar of type %s.", varName, baseType))
    }
  }
}


#' QueryNamespace
#' @export
#' @description
#' Given a results specification and ConnectionHandler instance - this class allow queries to be namespaced within
#' any tables specified within a list of pre-determined tables. This allows the encapsulation of queries, using specific
#' table names in a consistent manner that is striaghtforward to maintain over time.
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
#'
QueryNamespace <- R6::R6Class(
  classname = "QueryNamespace",
  lock_objects = FALSE,
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
    #' @param queryFiles vector of file paths to sql files that can be automatically loaded and translated as additional methods for this module
    #' @param executeFiles vector of file paths to sql files that can be automatically loaded and translated as additional methods for this module. Queries will executed and not return a result
    #' @param snakeCaseToCamelCaseFileNames - if true the method name will turn snake case file names in queryFiles in to camel case (e.g. my_query.sql becomes qns$myQuery)
    #' @param ... additional replacement variables e.g. database_schema, vocabulary_schema etc
    initialize = function(connectionHandler = NULL, tableSpecification = NULL, tablePrefix = "", queryFiles = NULL, executeFiles = NULL, snakeCaseToCamelCaseFileNames = FALSE, ...) {
      checkmate::assertString(tablePrefix)
      checkmate::assertCharacter(queryFiles, null.ok = TRUE)
      checkmate::assertCharacter(executeFiles, null.ok = TRUE)

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

      lapply(queryFiles, self$addQueryFile, snakeCaseToCamelCaseFileNames = snakeCaseToCamelCaseFileNames)
      lapply(executeFiles, self$addQueryFile, snakeCaseToCamelCaseFileNames = snakeCaseToCamelCaseFileNames, execFunction = TRUE)

      self
    },

    #' Add query file
    #' @description
    #' Add an sql file query as a method to this namespace.
    #' This allows you to run the query with any settings that are encapsulated within this object.
    #' Package maintainers should take note that these functions will not be exposed and will include no validation
    #' checks on user input. Use with care.
    #' @param filepath  path to sql file - this will determine the name of the function
    #' @param execFunction  default false - instead of returning a result, execute a query or set of queries
    #' @param snakeCaseToCamelCaseFileNames - if true the method name will turn snake case names in to camel case (e.g. my_query.sql becomes qns$myQuery)
    addQueryFile = function(filepath, snakeCaseToCamelCaseFileNames = FALSE, execFunction = FALSE) {
      checkmate::assertFileExists(filepath)
      # Ensure file has .sql extension
      if (tolower(tools::file_ext(filepath)) != "sql") {
        stop("File must have a .sql extension")
      }

      # Derive function name from file name
      baseName <- basename(filepath)
      functionName <- sub("\\.sql$", "", baseName)
      if (snakeCaseToCamelCaseFileNames) {
        # Convert snake_case to camelCase
        functionName <- SqlRender::snakeCaseToCamelCase(functionName)
      }

      if (functionName %in% names(self)) {
        stop(paste("Cannot add existing name", functionName, "to object"))
      }

      # Define the callback function that runs the query
      callback <- function(...) {
        sql <- SqlRender::readSql(filepath)
        # Render SQL with replacement variables and any additional arguments
        renderedSql <- self$render(sql, ...)
        # Query the database and return results
        if (execFunction){
          return(self$executeSql(sql))
        }

        return(self$queryDb(renderedSql))
      }

      # Dynamically add the function as a method to this instance
      self[[functionName]] <- callback

      invisible(NULL)
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
      for (tableName in tableSpecification$tableName |> unique()) {
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

      # Regex for {TYPEC TYPE[@] @var_name}
      typecPattern <- "\\{TYPEC\\s+([A-Z]+(\\[\\])?)\\s+@([a-zA-Z0-9_]+)\\}"
      typecMatches <- gregexpr(typecPattern, sql, perl = TRUE)
      matchPositions <- regmatches(sql, typecMatches)[[1]]

      if (length(matchPositions) > 0) {
        for (matchStr in matchPositions) {
          m <- regexec(typecPattern, matchStr, perl = TRUE)
          parts <- regmatches(matchStr, m)[[1]]
          if (length(parts) >= 4) {
            typeStr <- parts[2]         # e.g. INT, INT[], CHAR, CHAR[], NUMERIC, NUMERIC[]
            isArray <- grepl("\\[\\]", typeStr)
            varName <- parts[4]         # <-- Should be parts[4], not parts[3]
            value <- params[[varName]]
            .typeCheckVariable(varName, value, typeStr, isArray)
          }
        }
        # Remove all {TYPEC ...} lines from SQL
        sql <- gsub(typecPattern, "", sql, perl = TRUE)
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

    #' closeConnection
    #' @description
    #' close connection, if active
    closeConnection = function() {
      if (!is.null(private$connectionHandler)) {
        private$connectionHandler$closeConnection()
        private$connectionHandler <- NULL
      }
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
    ) |>
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
