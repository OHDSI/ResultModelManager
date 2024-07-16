# Copyright 2024 Observational Health Data Sciences and Informatics
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
#' @param resultsFolder     The results folder location
#' @param specifications    Specifications data table
#'
#' @return
#' table
#'
#' @noRd
checkAndFixColumnNames <-
  function(table,
           tableName,
           resultsFolder,
           specifications) {
    observeredNames <- tolower(colnames(table)[order(colnames(table))])

    tableSpecs <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName)

    # Set all fields to requried if optional isn't specified
    if (!"optional" %in% colnames(tableSpecs)) {
      tableSpecs$optional <- "no"
    }

    optionalNames <- tableSpecs %>%
      dplyr::filter(tolower(.data$optional) == "yes") %>%
      dplyr::select("columnName")

    expectedNames <- tableSpecs %>%
      dplyr::select("columnName") %>%
      dplyr::anti_join(dplyr::filter(optionalNames, !.data$columnName %in% observeredNames),
        by = "columnName"
      ) %>%
      dplyr::arrange("columnName") %>%
      dplyr::pull()

    if (!(all(expectedNames %in% observeredNames))) {
      stop(
        sprintf(
          "Column names of table %s in results folder %s do not match specifications.\n- Observed columns: %s\n- Expected columns: %s",
          tableName,
          resultsFolder,
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
#' @param resultsFolder     The results folder location
#' @param specifications    Specifications data table
#'
#' @return
#' table
#'
#' @noRd
checkAndFixDataTypes <-
  function(table,
           tableName,
           resultsFolder,
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
              "Column %s in table %s in results folder %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              resultsFolder,
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
              "Column %s in table %s in results folder %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              resultsFolder,
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
              "Column %s in table %s in results folder %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              resultsFolder,
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
              "Column %s in table %s in results folder %s is of type %s, but was expecting %s. Attempting to convert.",
              columnName,
              tableName,
              resultsFolder,
              observedTypes[i],
              expectedType
            )
          )

          dateFunc <- as.Date
          if (is.numeric(table[, i])) {
            dateFunc <- as.Date.POSIXct
          }

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
#' @param resultsFolder     The results folder location
#' @param specifications    Specifications data table
#'
#' @return
#' table
#'
#' @noRd
checkAndFixDuplicateRows <-
  function(table,
           tableName,
           resultsFolder,
           specifications) {
    primaryKeys <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName &
        tolower(.data$primaryKey) == "yes") %>%
      dplyr::select("columnName") %>%
      dplyr::pull()
    duplicatedRows <- duplicated(table[, primaryKeys])
    if (any(duplicatedRows)) {
      warning(
        sprintf(
          "Table %s in zip file %s has duplicate rows. Removing %s records.",
          tableName,
          resultsFolder,
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
#' @noRd
#'
appendNewRows <-
  function(data,
           newData,
           tableName,
           specifications) {
    if (nrow(data) > 0) {
      primaryKeys <- specifications %>%
        dplyr::filter(.data$tableName == !!tableName &
          tolower(.data$primaryKey) == "yes") %>%
        dplyr::select("columnName") %>%
        dplyr::pull()
      newData <- newData %>%
        dplyr::anti_join(data, by = primaryKeys)
    }
    return(dplyr::bind_rows(data, newData))
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
  val[tolower(val) == "inf" | tolower(val) == "-inf"] <- "NaN"
  val <- as.numeric(val)
  return(val)
}

.truncateTable <- function(tableName, connection, schema, tablePrefix) {
  DatabaseConnector::renderTranslateExecuteSql(connection,
    "TRUNCATE TABLE @schema.@table_prefix@table;",
    table_prefix = tablePrefix,
    schema = schema,
    table = tableName
  )
  invisible(NULL)
}


.removeDataUserCheck <- function(inp = readline(prompt = "Warning - this will delete all data in this model, would you like to proceed? [y/N] ")) {
  userInput <- tolower(inp)
  while (!userInput %in% c("y", "n", "")) {
    userInput <- tolower(readline(prompt = "Please enter Y or N "))
  }

  if (userInput %in% c("n", "")) {
    message("stopping")
    return(FALSE)
  }
  return(TRUE)
}


uploadChunk <- function(chunk, pos, env, specifications, resultsFolder, connection, runCheckAndFixCommands, forceOverWriteOfSpecifications) {
  ParallelLogger::logInfo(
    "- Preparing to upload rows ",
    pos,
    " through ",
    pos + nrow(chunk) - 1
  )

  # Ensure all column names are in lowercase
  colnames(chunk) <- tolower(colnames(chunk))

  if (runCheckAndFixCommands) {
    chunk <- checkAndFixColumnNames(
      table = chunk,
      tableName = env$specTableName,
      resultsFolder = resultsFolder,
      specifications = specifications
    )
    chunk <- checkAndFixDataTypes(
      table = chunk,
      tableName = env$specTableName,
      resultsFolder = resultsFolder,
      specifications = specifications
    )

    chunk <- checkAndFixDuplicateRows(
      table = chunk,
      tableName = env$specTableName,
      resultsFolder = resultsFolder,
      specifications = specifications
    )
  }

  # Ensure dates are formatted properly
  toDate <- specifications %>%
    dplyr::filter(
      .data$tableName == env$tableName &
        tolower(.data$dataType) == "date"
    ) %>%
    dplyr::select("columnName") %>%
    dplyr::pull()

  if (length(toDate) > 0) {
    chunk <- chunk %>%
      dplyr::mutate_at(toDate, lubridate::as_date)
  }

  toTimestamp <- specifications %>%
    dplyr::filter(
      .data$tableName == env$tableName &
        grepl("timestamp", tolower(.data$dataType))
    ) %>%
    dplyr::select("columnName") %>%
    dplyr::pull()
  if (length(toTimestamp) > 0) {
    chunk <- chunk %>%
      dplyr::mutate_at(toTimestamp, lubridate::as_datetime)
  }

  toDouble <- specifications %>%
    dplyr::filter(
      .data$tableName == env$tableName &
        tolower(.data$dataType) %in% c("decimal", "numeric", "float")
    ) %>%
    dplyr::select("columnName") %>%
    dplyr::pull()
  if (length(toDouble) > 0) {
    chunk <- chunk %>%
      dplyr::mutate_at(toDouble, formatDouble)
  }


  # Check if inserting data would violate primary key constraints:
  if (!is.null(env$primaryKeyValuesInDb)) {
    chunk <- formatChunk(
      pkValuesInDb = env$primaryKeyValuesInDb,
      chunk = chunk
    )
    primaryKeyValuesInChunk <- unique(chunk[env$primaryKey])
    duplicates <-
      dplyr::inner_join(env$primaryKeyValuesInDb,
        primaryKeyValuesInChunk,
        by = env$primaryKey
      )

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


uploadTable <- function(tableName,
                        connection,
                        schema,
                        tablePrefix,
                        databaseId,
                        resultsFolder,
                        specifications,
                        runCheckAndFixCommands,
                        forceOverWriteOfSpecifications,
                        purgeSiteDataBeforeUploading,
                        warnOnMissingTable) {
  csvFileName <- paste0(tableName, ".csv")
  if (csvFileName %in% list.files(resultsFolder)) {
    rlang::inform(paste0("Uploading file: ", csvFileName, " to table: ", tableName))

    primaryKey <- specifications %>%
      dplyr::filter(.data$tableName == !!tableName &
        tolower(.data$primaryKey) == "yes") %>%
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
    env$purgeSiteDataBeforeUploading <- purgeSiteDataBeforeUploading
    if (purgeSiteDataBeforeUploading && "database_id" %in% primaryKey) {
      type <- specifications %>%
        dplyr::filter(.data$tableName == !!tableName &
          .data$columnName == "database_id") %>%
        dplyr::select("dataType") %>%
        dplyr::pull()
      # Remove the existing data for the databaseId
      deleteAllRowsForDatabaseId(
        connection = connection,
        schema = schema,
        tableName = paste0(tablePrefix, tableName),
        databaseId = databaseId,
        idIsInt = type %in% c("int", "bigint")
      )

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


    readr::read_csv_chunked(
      file = file.path(resultsFolder, csvFileName),
      callback = function(chunk, pos) uploadChunk(chunk, pos, env, specifications, resultsFolder, connection, runCheckAndFixCommands, forceOverWriteOfSpecifications),
      chunk_size = 1e7,
      col_types = readr::cols(),
      guess_max = 1e6,
      progress = FALSE
    )
  } else if (warnOnMissingTable) {
    warning(paste(csvFileName, "not found"))
  }
}

#' Upload results to the database server.
#'
#' @description
#' Requires the results data model tables have been created using following the specifications, generateSqlSchema function.
#'
#' Results files should be in the snake_case format for table headers and not camelCase
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
#'                       that site will be dropped. This assumes the results folder contains the full data for that
#'                       data site.
#' @param runCheckAndFixCommands If TRUE, the upload code will attempt to fix column names, data types and
#'                       duplicate rows. This parameter is kept for legacy reasons - it is strongly recommended
#'                       that you correct errors in your results where those results are assembled instead of
#'                       relying on this option to try and fix it during upload.
#' @param databaseIdentifierFile  File contained that references databaseId field (used when purgeSiteDataBeforeUploading == TRUE). You may
#'                       specify a relative path for the cdmSourceFile and the function will assume it resides in the resultsFolder.
#'                       Alternatively, you can provide a path outside of the resultsFolder for this file.
#'
#' @param purgeDataModel  This function will purge all data from the tables in the specification prior to upload.
#'                              Use with care. If interactive this will require further input.
#' @param specifications A tibble data frame object with specifications.
#' @param warnOnMissingTable Boolean, print a warning if a table file is missing.
#'
#' @export
uploadResults <- function(connection = NULL,
                          connectionDetails = NULL,
                          schema,
                          resultsFolder,
                          tablePrefix = "",
                          forceOverWriteOfSpecifications = FALSE,
                          purgeSiteDataBeforeUploading = TRUE,
                          databaseIdentifierFile = "cdm_source_info.csv",
                          runCheckAndFixCommands = FALSE,
                          warnOnMissingTable = TRUE,
                          purgeDataModel = FALSE,
                          specifications) {
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  if (DatabaseConnector::dbms(connection) == "sqlite" & schema != "main") {
    stop("Invalid schema for sqlite, use schema = 'main'")
  }

  # Check specifications for required columns
  assertSpecificationColumns(colnames(specifications))

  start <- Sys.time()

  if (purgeDataModel) {
    if (rlang::is_interactive()) {
      if (!.removeDataUserCheck()) {
        return(invisible(NULL))
      }
    }

    ParallelLogger::logInfo("Removing all records for tables within specification")

    invisible(lapply(unique(specifications$tableName),
      .truncateTable,
      connection = connection,
      schema = schema,
      tablePrefix = tablePrefix
    ))
  }

  # Retrieve the databaseId from the cdmSourceFile if the file exists
  # and we're purging site data before uploading. First check to see if the
  # cdmSourceFile is a relative path and set it to the current resultsFolder
  if (purgeSiteDataBeforeUploading) {
    if (!(grepl(pattern = "/", x = databaseIdentifierFile) || grepl(pattern = "\\\\", x = databaseIdentifierFile))) {
      databaseIdentifierFile <- file.path(resultsFolder, databaseIdentifierFile)
    }
    if (file.exists(databaseIdentifierFile)) {
      database <-
        readr::read_csv(
          file = databaseIdentifierFile,
          col_types = readr::cols()
        )
      colnames(database) <-
        SqlRender::snakeCaseToCamelCase(colnames(database))
      databaseId <- database$databaseId
    } else {
      stop(
        sprintf(
          "databaseIdentifierFile %s not found. This file location must be specified when purgeSiteDataBeforeUploading == TRUE",
          databaseIdentifierFile
        )
      )
    }
  }

  for (tableName in unique(specifications$tableName)) {
    uploadTable(
      tableName,
      connection,
      schema,
      tablePrefix,
      databaseId,
      resultsFolder,
      specifications,
      runCheckAndFixCommands,
      forceOverWriteOfSpecifications,
      purgeSiteDataBeforeUploading,
      warnOnMissingTable
    )
  }

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
#' @param idIsInt           Identified is a numeric type? If not character is used
#'
#' @export
deleteAllRowsForDatabaseId <-
  function(connection,
           schema,
           tableName,
           databaseId,
           idIsInt = TRUE) {
    sql <-
      "SELECT COUNT(*) FROM @schema.@table_name WHERE database_id IN (@database_id);"

    if (!idIsInt) {
      databaseId <- paste0("'", databaseId, "'")
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

      sql <-
        "DELETE FROM @schema.@table_name WHERE database_id IN (@database_id);"

      sql <- SqlRender::render(
        sql = sql,
        schema = schema,
        table_name = tableName,
        database_id = databaseId
      )
      DatabaseConnector::executeSql(connection,
        sql,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
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
}

#' Custom checkmate assertion for ensuring the specification columns are properly
#' specified
#'
#' @description
#' This function is used to provide a more informative message when ensuring
#' that the columns in the results data model specification are properly specified.
#' This function is then bootstrapped upon package initialization (code in
#' ResultModelManager.R) to allow for it to work with the other checkmate
#' assertions as described in: https://mllg.github.io/checkmate/articles/checkmate.html.
#' The assertion function is called assertSpecificationColumns.
#'
#' @param columnNames The name of the columns found in the results data model specification
#'
#' @return
#' Returns TRUE if all required columns are found otherwise it returns an error
#' @noRd
#' @keywords internal
checkSpecificationColumns <- function(columnNames) {
  requiredFields <- c("tableName", "columnName", "dataType", "primaryKey")
  res <- all(requiredFields %in% columnNames)
  if (!isTRUE(res)) {
    errorMessage <- paste0("The results data model specification requires the following columns: ", paste(shQuote(requiredFields), collapse = ", "), ". The following columns were found: ", paste(shQuote(columnNames), collapse = ", "))
    return(errorMessage)
  } else {
    return(TRUE)
  }
}


#' Get specifications from a given file path
#' @param filePath path to a valid csv file
#' @return
#' A tibble data frame object with specifications
#'
#' @export
loadResultsDataModelSpecifications <- function(filePath) {
  checkmate::assertFileExists(filePath)
  spec <- readr::read_csv(file = filePath, col_types = readr::cols())
  colnames(spec) <- SqlRender::snakeCaseToCamelCase(colnames(spec))
  assertSpecificationColumns(colnames(spec))
  return(spec)
}


#' This helper function will convert the data in the
#' primary key values in the `chunk` which is read from
#' the csv file to the format of the primary key data
#' retrieved from the database (`pkValuesInDb`). The assumption made
#' by this function is that the `pkValuesInDb` reflect the proper data
#' types while `chunk` is the best guess from the readr
#' package. In the future, if we adopt strongly-types data.frames
#' this will no longer be necessary.
#'
#' Another assumption of this function is that we're only attempting to
#' recast to a character data type and not try to handle different type
#' conversions.
#' @noRd
formatChunk <- function(pkValuesInDb, chunk) {
  for (columnName in names(pkValuesInDb)) {
    if (inherits(pkValuesInDb[[columnName]], "integer")) {
      pkValuesInDb[[columnName]] <- as.numeric(pkValuesInDb[[columnName]])
    }

    if (inherits(chunk[[columnName]], "integer")) {
      chunk[[columnName]] <- as.numeric(chunk[[columnName]])
    }


    if (class(pkValuesInDb[[columnName]]) != class(chunk[[columnName]])) {
      if (inherits(pkValuesInDb[[columnName]], "character")) {
        chunk <- chunk |> dplyr::mutate_at(columnName, as.character)
      } else {
        errorMsg <- paste0(
          columnName,
          " is of type ",
          class(pkValuesInDb[[columnName]]),
          " which cannot be converted between data frames pkValuesInDb and chunk"
        )
        stop(errorMsg)
      }
    }
  }

  return(chunk)
}
