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
    observeredNames <- colnames(table)[order(colnames(table))]

    tableSpecs <- specifications %>%
      dplyr::filter(tableName == !!tableName)

    optionalNames <- tableSpecs %>%
      dplyr::filter(optional == "Yes") %>%
      dplyr::select("fieldName")

    expectedNames <- tableSpecs %>%
      dplyr::select("fieldName") %>%
      dplyr::anti_join(dplyr::filter(optionalNames, !fieldName %in% observeredNames),
                       by = "fieldName") %>%
      dplyr::arrange("fieldName") %>%
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
#' @importFrom rlang .data
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
      fieldName <- names(observedTypes)[i]
      expectedType <-
        gsub("\\(.*\\)", "", tolower(tableSpecs$type[tableSpecs$fieldName == fieldName]))
      if (expectedType == "bigint" || expectedType == "float") {
        if (observedTypes[i] != "numeric" && observedTypes[i] != "double") {
          ParallelLogger::logDebug(
            sprintf(
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
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
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
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
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
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
              "Field %s in table %s in zip file %s is of type %s, but was expecting %s. Attempting to convert.",
              fieldName,
              tableName,
              zipFileName,
              observedTypes[i],
              expectedType
            )
          )
          table <- dplyr::mutate_at(table, i, as.Date)
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
#' @importFrom rlang .data
#'
#' @noRd
checkAndFixDuplicateRows <-
  function(table,
           tableName,
           zipFileName,
           specifications) {
    primaryKeys <- specifications %>%
      dplyr::filter(tableName == !!tableName &
                      primaryKey == "Yes") %>%
      dplyr::select("fieldName") %>%
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
      return(table[!duplicatedRows, ])
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
#' @importFrom rlang .data
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
        dplyr::filter(tableName== !!tableName &
                        primaryKey == "Yes") %>%
        dplyr::select("fieldName") %>%
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
        on.exit(DatabaseConnector::disconnect(connection))
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


#' Upload results to the database server.
#'
#' @description
#' Requires the results data model tables have been created using the \code{\link{createResultsDataModel}} function.
#'
#' Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable
#' bulk upload (recommended).
#'
#' @param connectionDetails   An object of type \code{connectionDetails} as created using the
#'                            \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                            DatabaseConnector package.
#' @param schema         The schema on the postgres server where the tables have been created.
#' @param zipFileName    The name of the zip file.
#' @param forceOverWriteOfSpecifications  If TRUE, specifications of the phenotypes, cohort definitions, and analysis
#'                       will be overwritten if they already exist on the database. Only use this if these specifications
#'                       have changed since the last upload.
#' @param purgeSiteDataBeforeUploading If TRUE, before inserting data for a specific databaseId all the data for
#'                       that site will be dropped. This assumes the input zip file contains the full data for that
#'                       data site.
#' @param tempFolder     A folder on the local file system where the zip files are extracted to. Will be cleaned
#'                       up when the function is finished. Can be used to specify a temp folder on a drive that
#'                       has sufficient space if the default system temp space is too limited.
#' @param specifications   A tibble data frame object with specifications.
#'
#' @export
uploadResults <-   function(connectionDetails = NULL,
                            schema,
                            zipFileName,
                            forceOverWriteOfSpecifications = FALSE,
                            purgeSiteDataBeforeUploading = TRUE,
                            tempFolder = tempdir(),
                            specifications) {
  start <- Sys.time()
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  unzipFolder <- tempfile("unzipTempFolder", tmpdir = tempFolder)
  dir.create(path = unzipFolder, recursive = TRUE)
  on.exit(unlink(unzipFolder, recursive = TRUE), add = TRUE)

  ParallelLogger::logInfo("Unzipping ", zipFileName)
  zip::unzip(zipFileName, exdir = unzipFolder)

  if (purgeSiteDataBeforeUploading) {
    database <-
      readr::read_csv(file = file.path(unzipFolder, "database.csv"),
                      col_types = readr::cols())
    colnames(database) <-
      SqlRender::snakeCaseToCamelCase(colnames(database))
    databaseId <- database$databaseId
  }

  uploadTable <- function(tableName) {
    ParallelLogger::logInfo("Uploading table ", tableName)

    primaryKey <- specifications %>%
      dplyr::filter(tableName == !!tableName &
                      primaryKey == "Yes") %>%
      dplyr::select("fieldName") %>%
      dplyr::pull()

    if (purgeSiteDataBeforeUploading &&
        "database_id" %in% primaryKey) {
      deleteAllRowsForDatabaseId(
        connection = connection,
        schema = schema,
        tableName = tableName,
        databaseId = databaseId
      )
    }

    csvFileName <- paste0(tableName, ".csv")
    if (csvFileName %in% list.files(unzipFolder)) {
      env <- new.env()
      env$schema <- schema
      env$tableName <- tableName
      env$primaryKey <- primaryKey
      if (purgeSiteDataBeforeUploading &&
          "database_id" %in% primaryKey) {
        env$primaryKeyValuesInDb <- NULL
      } else {
        sql <- "SELECT DISTINCT @primary_key FROM @schema.@table_name;"
        sql <- SqlRender::render(
          sql = sql,
          primary_key = primaryKey,
          schema = schema,
          table_name = tableName
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

        chunk <- checkAndFixColumnNames(
          table = chunk,
          tableName = env$tableName,
          zipFileName = zipFileName,
          specifications = specifications
        )
        chunk <- checkAndFixDataTypes(
          table = chunk,
          tableName = env$tableName,
          zipFileName = zipFileName,
          specifications = specifications
        )
        chunk <- checkAndFixDuplicateRows(
          table = chunk,
          tableName = env$tableName,
          zipFileName = zipFileName,
          specifications = specifications
        )

        # Primary key fields cannot be NULL, so for some tables convert NAs to empty or zero:
        toEmpty <- specifications %>%
          dplyr::filter(
            tableName == env$tableName &
              emptyIsNa == "No" &
              grepl("varchar", "type")
          ) %>%
          dplyr::select("fieldName") %>%
          dplyr::pull()
        if (length(toEmpty) > 0) {
          chunk <- chunk %>%
            dplyr::mutate_at(toEmpty, naToEmpty)
        }

        tozero <- specifications %>%
          dplyr::filter(
            tableName == env$tableName &
              emptyIsNa == "No" &
              type %in% c("int", "bigint", "float")
          ) %>%
          dplyr::select("fieldName") %>%
          dplyr::pull()
        if (length(tozero) > 0) {
          chunk <- chunk %>%
            dplyr::mutate_at(tozero, naToZero)
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
          DatabaseConnector::insertTable(
            connection = connection,
            tableName = paste(env$schema, env$tableName, sep = "."),
            data = chunk,
            dropTableIfExists = FALSE,
            createTable = FALSE,
            tempTable = FALSE,
            progressBar = TRUE
          )
        }
      }
      readr::read_csv_chunked(
        file = file.path(unzipFolder, csvFileName),
        callback = uploadChunk,
        chunk_size = 1e7,
        col_types = readr::cols(),
        guess_max = 1e6,
        progress = FALSE
      )

      # chunk <- readr::read_csv(file = file.path(unzipFolder, csvFileName),
      # col_types = readr::cols(),
      # guess_max = 1e6)

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
          colnames(keyValues), " = '", keyValues[i, ], "'"
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
#'
#' @export
deleteAllRowsForDatabaseId <-
  function(connection,
           schema,
           tableName,
           databaseId) {
    sql <-
      "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id = '@database_id';"
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
      sql <-
        "DELETE FROM @schema.@table_name WHERE database_id = '@database_id';"
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
