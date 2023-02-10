# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of OHdsiSharing
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
#


#' Check and fix column names
#'
#' @param table             Data table
#' @param tableName         Database table name
#' @param zipFileName       Name zip file
#' @param specifications    Specifications data table
#'
#' @return
#' table
#'
#' @noRd
checkAndFixColumnNames <-
  function(table,
           tableName,
           zipFileName,
           specifications) {
    observeredNames <- tolower(colnames(table)[order(colnames(table))])

    tableSpecs <- specifications %>%
      dplyr::filter(tableName == !!tableName)

    optionalNames <- tableSpecs %>%
      dplyr::filter(tolower(optional) == "yes") %>%
      dplyr::select("columnName")

    expectedNames <- tableSpecs %>%
      dplyr::select("columnName") %>%
      dplyr::anti_join(dplyr::filter(optionalNames, !columnName %in% observeredNames),
                       by = "columnName") %>%
      dplyr::arrange("columnName") %>%
      dplyr::pull()

    if (!(all(expectedNames %in% observeredNames))) {
      stop(
        sprintf(
          "Column names of table %s in zip file %s do not match specifications.\n- Observed columns: %s\n- Expected columns: %s",
          tableName,
          zipFileName,
          paste(observeredNames, collapse = ", "),
          paste(expectedNames, collapse = ", ")
        )
      )
    }

    colnames(table) <- tolower(colnames(table))
    return(table[, expectedNames])
  }

#' Check and fix data types
#'
#' @param table             Data table
#' @param tableName         Database table name
#' @param zipFileName       Name zip file
#' @param specifications    Specifications data table
#'
#' @return
#' table
#'
#' @noRd
checkAndFixDataTypes <-
  function(table,
           tableName,
           zipFileName,
           specifications) {
    tableSpecs <- specifications %>%
      dplyr::filter(tableName == !!tableName)

    observedTypes <- sapply(table, class)
    for (i in seq_len(length(observedTypes))) {
      columnName <- names(observedTypes)[i]
      expectedType <-
        gsub("\\(.*\\)", "", tolower(tableSpecs$dataType[tableSpecs$columnName == columnName]))

      if (expectedType == "bigint" || expectedType == "float") {
        if (observedTypes[i] != "numeric" && observedTypes[i] != "double") {
          ParallelLogger::logDebug(
            sprintf(
              "Column %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- dplyr::mutate_at(table, i, as.numeric)
        }
      } else if (expectedType == "int") {
        if (observedTypes[i] != "integer") {
          ParallelLogger::logDebug(
            sprintf(
              "Column %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- dplyr::mutate_at(table, i, as.integer)
        }
      } else if (expectedType == "varchar") {
        if (observedTypes[i] != "character") {
          ParallelLogger::logDebug(
            sprintf(
              "Column %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- dplyr::mutate_at(table, i, as.character)
        }
      } else if (expectedType == "date") {
        if (observedTypes[i] != "Date") {
          ParallelLogger::logDebug(
            sprintf(
              "Column %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )

          dateFunc <- as.Date
          if (is.numeric(table[,i]))
            dateFunc <- as.Date.POSIXct

          table <- dplyr::mutate_at(table, i, dateFunc)
        }
      }
    }
    return(table)
  }


#' Check and fix duplicate rows
#'
#' @param table             Data table
#' @param tableName         Database table name
#' @param zipFileName       Name zip file
#' @param specifications    Specifications data table
#'
#' @return
#' table
#'
#' @noRd
checkAndFixDuplicateRows <-
  function(table,
           tableName,
           zipFileName,
           specifications) {
    primaryKeys <- specifications %>%
      dplyr::filter(tableName == !!tableName &
                      tolower(primaryKey) == "yes") %>%
      dplyr::select("columnName") %>%
      dplyr::pull()
    duplicatedRows <- duplicated(table[, primaryKeys])
    if (any(duplicatedRows)) {
      warning(
        sprintf(
          "Table %s in zip file %s has duplicate rows. Removing %s records.",
          tableName,
          zipFileName,
          sum(duplicatedRows)
        )
      )
      return(table[!duplicatedRows,])
    } else {
      return(table)
    }
  }

#' Append new data rows to existing data rows using primary keys
#'
#' @param data              Data table
#' @param newData           Data table to append
#' @param tableName         Database table name
#' @param specifications    Specifications data table
#'
#' @return
#' table of combined data rows
#'
#' @noRd
#'
appendNewRows <-
  function(data,
           newData,
           tableName,
           specifications) {
    if (nrow(data) > 0) {
      primaryKeys <- specifications %>%
        dplyr::filter(tableName == !!tableName &
                        tolower(primaryKey) == "yes") %>%
        dplyr::select("columnName") %>%
        dplyr::pull()
      newData <- newData %>%
        dplyr::anti_join(data, by = primaryKeys)
    }
    return(dplyr::bind_rows(data, newData))
  }

#' Create the results data model tables on a database server.
#'
#' @details
#' Only PostgreSQL servers are supported.
#'
#' @param connection            DatabaseConnector connection instance or null
#' @param connectionDetails     DatabaseConnector connectionDetails instance or null
#' @param schema                The schema on the postgres server where the tables will be created.
#' @param sql                   The postgres sql with the results data model DDL.
#'
#' @export
createResultsDataModel <-
  function(connection = NULL,
           connectionDetails = NULL,
           schema,
           sql) {
    if (is.null(connection)) {
      if (!is.null(connectionDetails)) {
        connection <- DatabaseConnector::connect(connectionDetails)
        on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
      } else {
        stop("No connection or connectionDetails provided.")
      }
    }
    schemas <- unlist(
      DatabaseConnector::querySql(
        connection,
        "SELECT schema_name FROM information_schema.schemata;",
        snakeCaseToCamelCase = TRUE
      )[, 1]
    )
    if (!tolower(schema) %in% tolower(schemas)) {
      stop(
        "Schema '",
        schema,
        "' not found on database. Only found these schemas: '",
        paste(schemas, collapse = "', '"),
        "'"
      )
    }
    DatabaseConnector::executeSql(
      connection,
      sprintf("SET search_path TO %s;", schema),
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    DatabaseConnector::executeSql(connection, sql)
  }

naToEmpty <- function(x) {
  x[is.na(x)] <- ""
  return(x)
}

naToZero <- function(x) {
  x[is.na(x)] <- 0
  return(x)
}

# Aims to handle infinite values by replacing
# as NaN
formatDouble <- function(x) {
  val <- as.character(x)
  val[tolower(val) == 'inf' | tolower(val) == '-inf'] <- 'NaN'
  val <- as.numeric(val)
  return(val)
}

#' Upload results to the database server.
#'
#' @description
#' Requires the results data model tables have been created using the \code{\link{createResultsDataModel}} function.
#'
#' Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable
#' bulk upload (recommended).
#' @param connection          An object of type \code{connection} as created using the
#'                            \code{\link[DatabaseConnector]{connect}} function in the
#'                            DatabaseConnector package. Can be left NULL if \code{connectionDetails}
#'                            is provided, in which case a new connection will be opened at the start
#'                            of the function, and closed when the function finishes.
#' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                            DatabaseConnector package.
#' @param schema         The schema on the postgres server where the tables have been created.
#' @param resultsFolder  The path to the folder containing the results to upload.
#'                       See \code{unzipResults} for more information.
#' @param tablePrefix    String to prefix table names with - default is empty string
#' @param forceOverWriteOfSpecifications  If TRUE, specifications of the phenotypes, cohort definitions, and analysis
#'                       will be overwritten if they already exist on the database. Only use this if these specifications
#'                       have changed since the last upload.
#' @param purgeSiteDataBeforeUploading If TRUE, before inserting data for a specific databaseId all the data for
#'                       that site will be dropped. This assumes the input zip file contains the full data for that
#'                       data site.
#' @param runCheckAndFixCommands If TRUE, the upload code will attempt to fix column names, data types and
#'                       duplicate rows. This parameter is kept for legacy reasons - it is strongly recommended
#'                       that you correct errors in your results where those results are assembled instead of
#'                       relying on this option to try and fix it during upload.
#' @param tempFolder     A folder on the local file system where the zip files are extracted to. Will be cleaned
#'                       up when the function is finished. Can be used to specify a temp folder on a drive that
#'                       has sufficient space if the default system temp space is too limited.
#' @param specifications   A tibble data frame object with specifications.
#'
#' @param cdmSourceFile  File contained within zip that references databaseId field (used for purging data)
#'
#' @export
uploadResults <- function(connection = NULL,
                          connectionDetails = NULL,
                          schema,
                          resultsFolder,
                          tablePrefix = "",
                          forceOverWriteOfSpecifications = FALSE,
                          purgeSiteDataBeforeUploading = TRUE,
                          cdmSourceFile = "cdm_source_info.csv",
                          runCheckAndFixCommands = FALSE,
                          tempFolder = tempdir(),
                          specifications) {
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  if (connection@dbms == "sqlite" & schema != "main") {
    stop("Invalid schema for sqlite, use schema = 'main'")
  }

  start <- Sys.time()

  # TODO: Seems specific to CD? What if the cdmSourceFile does not exist?
  # I think the logic for obtaining the databaseId should be in the uploadTable
  # function.
  # if (purgeSiteDataBeforeUploading) {
  #   database <-
  #     readr::read_csv(file = file.path(unzipFolder, cdmSourceFile),
  #                     col_types = readr::cols())
  #   colnames(database) <-
  #     SqlRender::snakeCaseToCamelCase(colnames(database))
  #   databaseId <- database$databaseId
  # }

  uploadTable <- function(tableName) {
    csvFileName <- paste0(tableName, ".csv")
    if (csvFileName %in% list.files(resultsFolder)) {
      rlang::inform(paste0("Uploading file: ", csvFileName, " to table: ", tableName))

      primaryKey <- specifications %>%
        dplyr::filter(tableName == !!tableName &
                        tolower(primaryKey) == "yes") %>%
        dplyr::select("columnName") %>%
        dplyr::pull()

      # Create an environment variable to hold
      # the information about the target table for
      # uploading the data
      env <- new.env()
      env$schema <- schema
      env$tableName <- paste0(tablePrefix, tableName)
      env$specTableName <- tableName
      env$primaryKey <- primaryKey
      if (purgeSiteDataBeforeUploading &&
        "database_id" %in% primaryKey) {
        # Get the databaseId by reading the 1st row of data
        results <- readr::read_csv(
          file = file.path(resultsFolder, csvFileName),
          n_max = 1,
          show_col_types = FALSE)

        type <- specifications %>%
          dplyr::filter(tableName == !!tableName &
                          columnName == "database_id") %>%
          dplyr::select("dataType") %>%
          dplyr::pull()

        # TODO: The databaseId may be missing if there are
        # no results in the current file and then purging will
        # not happen properly. There may be a better way to obtain
        # the databaseId, potentially think of making this a
        # parameter of the upload function.
        if (nrow(results) == 1) {
          databaseId <- results$database_id[1]
          # Remove the existing data for the databaseId
          deleteAllRowsForDatabaseId(
            connection = connection,
            schema = schema,
            tableName = paste0(tablePrefix, tableName),
            databaseId = databaseId,
            idIsInt = type %in% c("int", "bigint")
          )
        }
        # Set primaryKeyValuesInDb to NULL
        # to indicate that the primary key
        # value need not be checked since we've
        # purged the database data ahead of loading
        # results from the file
        env$primaryKeyValuesInDb <- NULL
      } else if (length(primaryKey) > 0) {
        sql <- "SELECT DISTINCT @primary_key FROM @schema.@table_name;"
        sql <- SqlRender::render(
          sql = sql,
          primary_key = primaryKey,
          schema = schema,
          table_name = env$tableName
        )
        primaryKeyValuesInDb <-
          DatabaseConnector::querySql(connection, sql)
        colnames(primaryKeyValuesInDb) <-
          tolower(colnames(primaryKeyValuesInDb))
        env$primaryKeyValuesInDb <- primaryKeyValuesInDb
      }


      uploadChunk <- function(chunk, pos) {
        ParallelLogger::logInfo("- Preparing to upload rows ",
                                pos,
                                " through ",
                                pos + nrow(chunk) - 1)

        # Ensure all column names are in lowercase
        colnames(chunk) <- tolower(colnames(chunk))

        if (runCheckAndFixCommands) {
          chunk <- checkAndFixColumnNames(
            table = chunk,
            tableName = env$specTableName,
            zipFileName = zipFileName,
            specifications = specifications
          )
          chunk <- checkAndFixDataTypes(
            table = chunk,
            tableName = env$specTableName,
            zipFileName = zipFileName,
            specifications = specifications
          )
          chunk <- checkAndFixDuplicateRows(
            table = chunk,
            tableName = env$specTableName,
            zipFileName = zipFileName,
            specifications = specifications
          )

          # Primary key fields cannot be NULL, so for some tables convert NAs to empty or zero:
          toEmpty <- specifications %>%
            dplyr::filter(
              tableName == env$specTableName &
                tolower(emptyIsNa) != "yes" &
                grepl("varchar", dataType)
            ) %>%
            dplyr::select("columnName") %>%
            dplyr::pull()
          if (length(toEmpty) > 0) {
            chunk <- chunk %>%
              dplyr::mutate_at(toEmpty, naToEmpty)
          }

          toZero <- specifications %>%
            dplyr::filter(
              tableName == env$specTableName &
                tolower(emptyIsNa) != "yes" &
                dataType %in% c("int", "bigint", "float")
            ) %>%
            dplyr::select("columnName") %>%
            dplyr::pull()
          if (length(toZero) > 0) {
            chunk <- chunk %>%
              dplyr::mutate_at(toZero, naToZero)
          }
        }

        # Ensure dates are formatted properly
        toDate <- specifications %>%
          filter(
            .data$tableName == env$tableName &
              tolower(.data$dataType) == "date"
          ) %>%
          select(.data$columnName) %>%
          pull()
        if (length(toDate) > 0) {
          chunk <- chunk %>%
            dplyr::mutate_at(toDate, lubridate::as_date)
        }

        toTimestamp <- specifications %>%
          filter(
            .data$tableName == env$tableName &
              grepl("timestamp", tolower(.data$dataType))
          ) %>%
          select(.data$columnName) %>%
          pull()
        if (length(toTimestamp) > 0) {
          chunk <- chunk %>%
            dplyr::mutate_at(toTimestamp, lubridate::as_datetime)
        }

        toDouble <- specifications %>%
          filter(
            .data$tableName == env$tableName &
              tolower(.data$dataType) %in% c("decimal", "numeric", "float")
          ) %>%
          select(.data$columnName) %>%
          pull()
        if (length(toDouble) > 0) {
          chunk <- chunk %>%
            dplyr::mutate_at(toDouble, formatDouble)
        }


        # Check if inserting data would violate primary key constraints:
        if (!is.null(env$primaryKeyValuesInDb)) {
          primaryKeyValuesInChunk <- unique(chunk[env$primaryKey])
          duplicates <-
            dplyr::inner_join(env$primaryKeyValuesInDb,
                              primaryKeyValuesInChunk,
                              by = env$primaryKey)
          if (nrow(duplicates) != 0) {
            if ("database_id" %in% env$primaryKey ||
              forceOverWriteOfSpecifications) {
              ParallelLogger::logInfo(
                "- Found ",
                nrow(duplicates),
                " rows in database with the same primary key ",
                "as the data to insert. Deleting from database before inserting."
              )
              deleteAllRowsForPrimaryKey(
                connection = connection,
                schema = env$schema,
                tableName = env$tableName,
                keyValues = duplicates
              )

            } else {
              ParallelLogger::logInfo(
                "- Found ",
                nrow(duplicates),
                " rows in database with the same primary key ",
                "as the data to insert. Removing from data to insert."
              )
              chunk <- chunk %>%
                dplyr::anti_join(duplicates, by = env$primaryKey)
            }
            # Remove duplicates we already dealt with:
            env$primaryKeyValuesInDb <-
              env$primaryKeyValuesInDb %>%
                dplyr::anti_join(duplicates, by = env$primaryKey)
          }
        }
        if (nrow(chunk) == 0) {
          ParallelLogger::logInfo("- No data left to insert")
        } else {
          insertTableStatus <- tryCatch(expr = {
            DatabaseConnector::insertTable(
              connection = connection,
              tableName = env$tableName,
              databaseSchema = env$schema,
              data = chunk,
              dropTableIfExists = FALSE,
              createTable = FALSE,
              tempTable = FALSE,
              progressBar = TRUE
            )
          }, error = function(e) e)
          if (inherits(insertTableStatus, "error")) {
            stop(insertTableStatus$message)
          }
        }
      }

      readr::read_csv_chunked(
        file = file.path(resultsFolder, csvFileName),
        callback = uploadChunk,
        chunk_size = 1e7,
        col_types = readr::cols(),
        guess_max = 1e6,
        progress = FALSE
      )
    }
    else {
      warning(paste(csvFileName, "not found"))
    }
  }

  invisible(lapply(unique(specifications$tableName), uploadTable))
  delta <- Sys.time() - start
  writeLines(paste("Uploading data took", signif(delta, 3), attr(delta, "units")))
}


#' Delete results rows for primary key values from database server tables
#'
#' @details
#' Only PostgreSQL servers are supported.
#'
#' @param connection        DatabaseConnector connection instance
#' @param schema            The schema on the postgres server where the results table exists
#' @param tableName         Database table name
#' @param keyValues         Key values of results rows to be deleted
#'
#' @export
deleteAllRowsForPrimaryKey <-
  function(connection, schema, tableName, keyValues) {

    createSqlStatement <- function(i) {
      sql <- paste0(
        "DELETE FROM ",
        schema,
        ".",
        tableName,
        "\nWHERE ",
        paste(paste0(
          colnames(keyValues), " = '", keyValues[i,], "'"
        ), collapse = " AND "),
        ";"
      )
      return(sql)
    }

    batchSize <- 1000
    for (start in seq(1, nrow(keyValues), by = batchSize)) {
      end <- min(start + batchSize - 1, nrow(keyValues))
      sql <- sapply(start:end, createSqlStatement)
      sql <- paste(sql, collapse = "\n")
      DatabaseConnector::executeSql(
        connection,
        sql,
        progressBar = FALSE,
        reportOverallTime = FALSE,
        runAsBatch = TRUE
      )
    }
  }


#' Delete all rows for database id
#'
#' @details
#' Only PostgreSQL servers are supported.
#'
#' @param connection                                     DatabaseConnector connection instance
#' @param schema            The schema on the postgres server where the results table exists
#' @param tableName         Database table name
#' @param databaseId        Results source database identifier
#' @param idIsInt           Identified is a numeric type? If not character is used
#'
#' @export
deleteAllRowsForDatabaseId <-
  function(connection,
           schema,
           tableName,
           databaseId,
           idIsInt = TRUE) {

    if (idIsInt) {
      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = @database_id;"
    } else {
      sql <-
        "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
    }
    sql <- SqlRender::render(
      sql = sql,
      schema = schema,
      table_name = tableName,
      database_id = databaseId
    )
    databaseIdCount <-
      DatabaseConnector::querySql(connection, sql)[, 1]
    if (databaseIdCount != 0) {
      ParallelLogger::logInfo(
        sprintf(
          "- Found %s rows in  database with database ID '%s'. Deleting all before inserting.",
          databaseIdCount,
          databaseId
        )
      )
      if (idIsInt) {
        sql <-
          "DELETE FROM @schema.@table_name WHERE database_id = @database_id;"
      } else {
        sql <-
          "DELETE FROM @schema.@table_name WHERE database_id = '@database_id';"
      }

      sql <- SqlRender::render(
        sql = sql,
        schema = schema,
        table_name = tableName,
        database_id = databaseId
      )
      DatabaseConnector::executeSql(connection,
                                    sql,
                                    progressBar = FALSE,
                                    reportOverallTime = FALSE)
    }
  }

#' Unzips a results.zip file and enforces standards required by
#' \code{uploadResults}
#'
#' @description
#' This function will unzip the zipFile to the resultsFolder and assert
#' that the file resultsDataModelSpecification.csv exists in the resultsFolder
#' to ensure that it will work with \code{uploadResults}
#'
#' @param zipFile   The location of the .zip file that holds the results to upload
#'
#' @param resultsFolder The folder to use when unzipping the .zip file. If this folder
#'                    does not exist, this function will attempt to create the folder.
#'
#' @export
unzipResults <- function(zipFile,
                         resultsFolder) {
  checkmate::assert_file_exists(zipFile)
  if (!dir.exists(resultsFolder)) {
    dir.create(path = resultsFolder, recursive = TRUE)
  }
  rlang::inform(paste0("Unzipping ", basename(zipFile), " to ", resultsFolder))
  zip::unzip(zipFile, exdir = resultsFolder)
  checkmate::assert_file_exists(file.path(resultsFolder, "resultsDataModelSpecification.csv"))
}
