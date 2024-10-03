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

.createPyConnection <- function(connection) {
  stopifnot(DatabaseConnector::dbms(connection) == "postgresql")
  hostServerDb <- strsplit(attr(connection, "server")(), "/")[[1]]
  port <- attr(connection, "port")()
  user <- attr(connection, "user")()
  password <- attr(connection, "password")()

  message("Connecting to PostgreSQL (python)...")
  psycopg2 <- reticulate::import("psycopg2", delay_load = TRUE)
  pgConnection <- psycopg2$connect(dbname = hostServerDb[2],
                                   user = user,
                                   password = password,
                                   host = hostServerDb[1],
                                   port = port)
  return(pgConnection)
}

.pyEnv <- new.env()

.loadPsycopg2Functions <- function() {
  if (identical(Sys.getenv("RMM_USE_PYTHON_UPLOADS"), "TRUE")) {
    reticulate::source_python(system.file("pg_upload.py", package = utils::packageName()), envir = .pyEnv)
  }
}

#' install psycopg2
#' @description
#' Install psycopg2-binary python package into the specified python virtualenv
#' @param envname python virtual environment name. Can be set with system environment variable "RMM_PYTHON_ENV", default is rmm-uploads
#' @param method method paramter for reticulate::py_install (defualt is auto)
#' @param ... Extra parameters for reticulate::py_install
#' @export
install_psycopg2 <- function(envname = Sys.getenv("RMM_PYTHON_ENV", unset = "rmm-uploads"), method = "auto", ...) {
  if (!interactive())
    stop("Session is not interactive. This is not how you want to install psycopg2")

  if (!reticulate::virtualenv_exists(envname)) {
    msg <- paste("No virtualenv configured. Create virtualenv", envname, " (set with envrionment valirable \"RMM_PYTHON_ENV\")")
    createnv <- utils::askYesNo(msg)
    if (createnv)
      reticulate::virtualenv_create(envname)
    else
      stop("Virtual env does not exist")
  }

  installBinary <- utils::askYesNo("Install psycopg2-binary python package into virtualenv?")
  if (!installBinary)
    stop("Virtual env does not exist")

  reticulate::py_install("psycopg2-binary", envname = envname, method = method, ...)
  invisible(NULL)
}


#' Enable Python Postgres Uploads
#' @description
#' Step by step install to enable python uploads
#' @param  ... parameters to pass to py_install
#' @export
enablePythonUploads <- function(...) {
  if (pyPgUploadEnabled())
    return(invisible(NULL))

  # Check reticulate is installed
  reticulateVersion <- tryCatch(utils::packageVersion("reticulate"), error = function(e) { return(NULL) })
  installed <- !is.null(reticulateVersion)
  if (!installed && interactive()) {
    if (isTRUE(utils::askYesNo("reticulate is required for this functionality - would you like to enable it?"))) {
      utils::install.packages("reticulate")
      installed <- TRUE
    }
  }

  if (!isTRUE(installed)) {
    stop("Cannot continue - reticulate package is not installed on this system install with  install.packages('reticulate')")
  }

  pyPostgresInstalled <- reticulate::py_module_available("psycopg2")

  if (!interactive() && !pyPostgresInstalled) {
    stop("psycopg2 is not installed in the specifed python environment for this system")
  }

  # Check package installed
  if (!pyPostgresInstalled) {
    install_psycopg2(...)
  }

  Sys.setenv("RMM_USE_PYTHON_UPLOADS" = "TRUE")
  .loadPsycopg2Functions()
  return(invisible(NULL))
}


#' are python postgresql uploads enabled?
#' @export
pyPgUploadEnabled <- function() {
  reticulateVersion <- tryCatch(utils::packageVersion("reticulate"), error = function(e) { return(NULL) })
  pySetupComplete <- FALSE
  if (!is.null(reticulateVersion)) {
    pySetupComplete <- reticulate::py_module_available("psycopg2")
  }
  return(identical(Sys.getenv("RMM_USE_PYTHON_UPLOADS"), "TRUE") && pySetupComplete)
}

#' Py Upload CSV
#' @description
#' Wrapper to python function to upload a csv using Postgres Copy functionality
#' @param connection DatabaseConnector connection instance
#' @param table Table in database
#' @param filepath path to csv
#' @param schema database schema containing table reference
#' @param disableConstraints (not reccomended) disable constraints prior to upload to speed up process
#' @examples
#' \dontrun{
#'   connection <- DabaseConnector::connect(dbms = "postgreql",
#'                                          server = "myserver.com",
#'                                          port = 5432,
#'                                          password = "s",
#'                                          user = "me",
#'                                          database = "some_db")
#'  ResultModelManager::pyUploadCsv(connection,
#'                                  table = "my_table",
#'                                  filepath = "my_massive_csv.csv",
#'                                  schema = "my_schema")
#' }
#' @export
pyUploadCsv <- function(connection, table, filepath, schema, disableConstraints = FALSE) {
  stopifnot(pyPgUploadEnabled())
  checkmate::assertFileExists(filepath)
  checkmate::assertString(table)
  checkmate::assertString(schema)
  checkmate::assertLogical(disableConstraints)
  DatabaseConnector::dbIsValid(connection)
  checkmate::assertTRUE(DatabaseConnector::dbms(connection) == 'postgresql')

  pyConnection <- .createPyConnection(connection)
  on.exit(pyConnection$close(), add = TRUE)

  result <- .pyEnv$upload_table(connection = pyConnection,
                                table = table,
                                filepath = normalizePath(filepath),
                                schema = schema,
                                disable_constraints = disableConstraints)
  # Handle errors
  if (result$status == -1) {
    ParallelLogger::logError("Error uploading filepath to table")
    ParallelLogger::logError(result$msg)
    stop("psycopg2 upload failed")
  }

  invisible()
}