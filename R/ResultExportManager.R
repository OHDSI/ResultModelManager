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

#' Result Set Export Manager
#' @export
#' @description
#'
#' EXPERIMENTAL - this feature is still in design stage and it is not reccomended that you implement this for your
#' package at this stage.
#' Utility for simplifying export of results to files from sql queries
#'
#' Note that this utility is not strictly thread safe though seperate processes can export separate tables
#' without issue. When exporting a the same table across multiple threads primary key checks may create
#' issues.
#'
#' @field exportDir direcotry path to export files to
ResultExportManager <- R6::R6Class(
  classname = "ResultExportManager",
  private = list(
    tables = NULL,
    tableSpecification = NULL,
    databaseId = NULL,
    connection = NULL,
    minCellCount = 5,
    .colTypeValidators = list(
      "numeric" = checkmate::checkNumeric,
      "int" = checkmate::checkIntegerish,
      "varchar" = checkmate::checkCharacter,
      "bigint" = function(x, ...) {
        checkmate::checkNumeric(x = x, ...) && x %% 1 == 0
      },
      "float" = checkmate::checkNumeric,
      "character" = checkmate::checkCharacter,
      "date" = checkmate::checkDate
    ),
    getPrimaryKeyCache = function(exportTableName) {
      file.path(tempdir(), paste0(private$databaseId, "-", Sys.getpid(), "-", exportTableName, ".csv"))
    },
    checkPkeyCache = function(keys, exportTableName, invalidateCache) {
      cacheFile <- private$getPrimaryKeyCache(exportTableName)
      if (invalidateCache) {
        unlink(cacheFile)
      }

      if (!file.exists(cacheFile)) {
        return(TRUE)
      }

      isValid <- TRUE
      # Scan file to see if any keys are duplicated
      # Stop if they are and handle the error by setting valid to false
      tryCatch(
        {
          readr::read_csv_chunked(cacheFile, callback = function(rows, pos) {
            mergedRows <- dplyr::bind_rows(keys, rows)
            if (any(duplicated(mergedRows))) {
              stop("Duplicate keys found")
            }
            return(NULL)
          }, show_col_types = FALSE)
        },
        error = function(err) {
          if (grepl("Duplicate keys found", err)) {
            isValid <<- FALSE
          } else {
            stop(err)
          }
        }
      )

      return(isValid)
    },

    # Assumes check pkeyCache has been called first as no unqiueness is assesed
    addToPkeyCache = function(keys, exportTableName) {
      cacheFile <- private$getPrimaryKeyCache(exportTableName)
      readr::write_csv(keys, cacheFile, append = file.exists(cacheFile))
      return(TRUE)
    },
    removePrimaryKeyCache = function() {
      lapply(self$listTables(), function(table) {
        cacheFile <- private$getPrimaryKeyCache(table)
        unlink(cacheFile, force = TRUE)
      })

      return(invisible(TRUE))
    },
    getMigrationFiles = function(migrationPath, packageName, migrationRegexp) {
      if (is.null(packageName)) {
        path <- file.path(migrationPath, "sql_server")
      } else {
        path <- system.file(file.path("sql", "sql_server", migrationPath), package = packageName)
      }

      return(list.files(path, migrationRegexp))
    },
    validateColType = function(coltype, rowData, dataSize, null.ok) {
      params <- list(
        x = rowData,
        null.ok = null.ok
      )

      if (!is.na(dataSize)) {
        if (colType %in% c("bigint", "int")) {
          params$upper <- 2**as.integer(dataSize)
        } else if (coltype %in% c("character", "varchar") && dataSize != "max") {
          params$max.chars <- dataSize
        }
      }
      do.call(private$.colTypeValidators[[coltype]], params)
    }
  ),
  public = list(
    exportDir = NULL,

    #' Init
    #' @description
    #' Create a class for exporting results from a study in a standard, consistend manner
    #' @param tableSpecification        Table specification data.frame
    #' @param exportDir                 Directory files are being exported to
    #' @param minCellCount              Minimum cell count - reccomended that you set with
    #'                                  options("ohdsi.minCellCount" = count) in all R projects. Default is 5
    #' @param databaseId                database identifier - required when exporting according to many specs
    initialize = function(tableSpecification,
                          exportDir,
                          minCellCount = getOption("ohdsi.minCellCount", default = 5),
                          databaseId = NULL) {
      self$exportDir <- exportDir
      # Check table spec is valid
      assertSpecificationColumns(colnames(tableSpecification))
      private$tableSpecification <- tableSpecification
      private$tables <- unique(tableSpecification$tableName)

      # Check/create export_dir
      if (!dir.exists(self$exportDir)) {
        dir.create(self$exportDir, recursive = TRUE)
      }

      checkmate::assertTRUE(is.null(databaseId) || length(databaseId) == 1)
      checkmate::assertIntegerish(minCellCount)
      private$minCellCount <- minCellCount
      private$databaseId <- databaseId

      #
      if (is.null(self$databaseId)) {
        dbIdCount <- private$tableSpecification %>%
          dplyr::filter(.data$columnName == "databaseId") %>%
          dplyr::count()

        if (dbIdCount > 0) {
          stop("database Id must be set for exports with this data model")
        }
      }
      # Remove primary key caches to prevent potentially weird behaviour
      private$removePrimaryKeyCache()
    },

    #' get table spec
    #' @description
    #' Get specification of table
    #' @param exportTableName table name
    getTableSpec = function(exportTableName) {
      private$tableSpecification %>%
        dplyr::filter(tableName == exportTableName)
    },

    #' Get min col values
    #' @description
    #' Columns to convert to minimum for a given table name
    #' @param rows  data.frame of rows
    #' @param exportTableName stering table name - must be defined in spec
    getMinColValues = function(rows, exportTableName) {
      spec <- self$getTableSpec(exportTableName)
      if ("minCellCount" %in% colnames(spec)) {
        filterCols <- spec %>%
          dplyr::filter(tolower(.data$minCellCount) == "yes") %>%
          dplyr::select("columnName") %>%
          dplyr::pull()

        if (length(filterCols)) {
          rows <- rows %>% dplyr::mutate(dplyr::across(
            all_of(filterCols),
            ~ ifelse(. > 0 & . < private$minCellCount, -private$minCellCount, .)
          ))
        }
      }
      return(rows)
    },
    #' Check row types
    #' @description
    #' Check types of rows before exporting
    #' @param rows data.frame of rows to export
    #' @param exportTableName table name
    checkRowTypes = function(rows, exportTableName) {
      spec <- self$getTableSpec(exportTableName)
      valid <- lapply(spec$columnName, function(colname) {
        nullOk <- FALSE
        if ("optional" %in% colnames(spec)) {
          nullOk <- spec %>%
            dplyr::filter(.data$columnName == colname) %>%
            dplyr::select("optional") %>%
            dplyr::pull() %>%
            tolower() == "yes"
        }

        coltype <- spec %>%
          dplyr::filter(.data$columnName == colname) %>%
          dplyr::pull("dataType")

        pattern <- "\\((\\d+|max)\\)"

        dataSize <- NA
        if (grepl(pattern, coltype)) {
          matches <- gregexpr(pattern, coltype)
          coltype <- gsub(dataSize, "", coltype)
          dataSize <- gsub("[()]", "", regmatches(text, matches)[1])
        }

        private$validateColType(coltype, rows[[colname]], dataSize, null.ok = nullOk)
      }) %>% unlist()
      return(all(valid))
    },

    #' List tables
    #' @description list all tables in schema
    listTables = function() {
      tableNames <- private$tableSpecification %>%
        dplyr::select("tableName") %>%
        dplyr::pull() %>%
        unique()

      return(tableNames)
    },

    #' Check primary keys of exported data
    #' @description
    #' Checks to see if the rows conform to the valid primary keys
    #' If the same table has already been checked in the life of this object set
    #' "invalidateCache" to TRUE as the keys will be cached in a temporary file
    #' on disk.
    #'
    #' @param rows data.frame to export
    #' @param exportTableName Table name (must be in spec)
    #' @param invalidateCache logical - if starting a fresh export use this to delete cache of primary keys
    checkPrimaryKeys = function(rows, exportTableName, invalidateCache = FALSE) {
      primaryKeyCols <- self$getTableSpec(exportTableName) %>%
        dplyr::filter(tolower(.data$primaryKey) == "yes") %>%
        dplyr::pull("columnName")

      pkdt <- rows %>% dplyr::select(dplyr::all_of(primaryKeyCols))
      # Primary keys cannot be null
      if (any(sapply(pkdt, function(x) any(is.null(x), is.na(x))))) {
        logMessage <- "null/na primary keys found"
        ParallelLogger::logError(logMessage)
        return(FALSE)
      }

      if (any(duplicated(pkdt))) {
        logMessage <- "duplicate primary keys found"
        ParallelLogger::logError(logMessage)
        return(FALSE)
      }

      isValid <- private$checkPkeyCache(pkdt, exportTableName, invalidateCache)

      if (isValid) {
        # Add to primary keys cache file
        private$addToPkeyCache(pkdt, exportTableName)
      }
      return(isValid)
    },

    #' Export data frame
    #' @description
    #' This method is intended for use where exporting a data.frame and not a query from a rdbms table
    #' For example, if you perform a transformation in R this method will check primary keys, min cell counts and
    #' data types before writing the file to according to the table spec
    #'
    #' @param rows              Rows to export
    #' @param exportTableName   Table name
    #' @param append    logical - if true will append the result to a file, otherwise the file will be overwritten
    exportDataFrame = function(rows, exportTableName, append = FALSE) {
      checkmate::assertDataFrame(rows)
      if (!exportTableName %in% private$tables) {
        stop("Table not found in specifications")
      }

      validRows <- self$checkRowTypes(rows, exportTableName)
      if (!all(isTRUE(validRows))) {
        stop(paste(validRows[!isTRUE(validRows)], collapse = "\n"))
      }

      # Convert < minCellCount to -minCellCount
      rows <- self$getMinColValues(rows, exportTableName)
      validPkeys <- self$checkPrimaryKeys(rows, exportTableName, invalidateCache = !append)

      if (!validPkeys) {
        stop("Cannot write data - primary keys already written to cache")
      }

      exportColumns <- self$getTableSpec(exportTableName) %>%
        dplyr::pull("columnName")
      # Subset to required columns only
      rows <- rows %>% dplyr::select(all_of(exportColumns))

      # Add database id, if present in spec
      outputFile <- file.path(self$exportDir, paste0(exportTableName, ".csv"))
      colnames(rows) <- tolower(colnames(rows))
      readr::write_csv(rows, file = outputFile, append = append)

      return(TRUE)
    },

    #' Export Data table with sql query
    #' @description
    #'
    #' Writes files in batch to stop overflowing system memory
    #' Checks primary keys on write
    #' Checks minimum cell count
    #'
    #' @param connection DatabaseConnector connection instance
    #' @param sql OHDSI sql string to export tables
    #' @param exportTableName Name of table to export (in snake_case format)
    #' @param transformFunction (optional) transformation of the data set callback.
    #'        must take two paramters - rows and pos
    #'
    #'        Following this transformation callback, results will be verified against data model,
    #'        Primary keys will be checked and minCellValue rules will be enforced
    #' @param transformFunctionArgs arguments to be passed to the transformation function
    #' @param append Logical add results to existing file, if FALSE (default) creates a new file and removes primary
    #'               key validation cache
    #' @param ...  extra parameters passed to sql
    exportQuery = function(connection,
                           sql,
                           exportTableName,
                           transformFunction = NULL,
                           transformFunctionArgs = list(),
                           append = FALSE,
                           ...) {
      checkmate::assertString(exportTableName)
      checkmate::assertString(sql)
      checkmate::assert(DatabaseConnector::dbIsValid(connection))

      exportData <- function(rows, pos, self, append, transformFunction, transformFunctionArgs) {
        if (!is.null(transformFunction)) {
          transformFunctionArgs$rows <- rows
          transformFunctionArgs$pos <- pos
          rows <- do.call(transformFunction, transformFunctionArgs)
        }

        self$exportDataFrame(rows, exportTableName, append = append || pos != 1)
        # Once the buffer is filled we no longer store the values to stop memory being killed
        return(NULL)
      }

      DatabaseConnector::renderTranslateQueryApplyBatched(connection,
        sql,
        fun = exportData,
        args = list(
          self = self,
          append = append,
          transformFunction = transformFunction,
          transformFunctionArgs = transformFunctionArgs
        ),
        snakeCaseToCamelCase = FALSE,
        ...
      )
      invisible()
    },

    #' get manifest list
    #' @description
    #' Create a meta data set for each collection of result files with sha256 has for all files
    #'
    #' @param packageName    if an R analysis package, specify the name
    #' @param packageVersion if an analysis package, specify the version
    #' @param migrationsPath path to sql migrations (use top level folder (e.g. sql/sql_server/migrations)
    #' @param migrationRegexp   (optional) regular expression to search for sql files. It is not reccomended to change the default.
    getManifestList = function(packageName = NULL,
                               packageVersion = NULL,
                               migrationsPath = NULL,
                               migrationRegexp = .defaultMigrationRegexp) {
      # For each file, create a checksum of the file and the filename
      exportedFiles <- file.path(self$exportDir, list.files(self$exportDir, "*.(csv|json)"))

      checksums <- data.frame(
        file = exportedFiles,
        checksum = lapply(exportedFiles, digest::digest, algo = "sha256") |> unlist()
      ) %>% dplyr::filter(.data$file != "mainfest.json")

      # Get any migrations from package that would be expected for data insert to work
      expectedMigrations <- private$getMigrationFiles(migrationsPath, packageName, migrationRegexp)
      resultFiles <- file.path(self$exportDir, paste0(self$listTables(), ".csv"))

      manifest <- list(
        packageName = packageName,
        packageVersion = packageVersion,
        resultFiles = resultFiles,
        checksums = checksums,
        expectedMigrations = expectedMigrations,
        rmmVersion = utils::packageVersion("ResultModelManager")
      )

      class(manifest) <- "RmmExportManifest"
      return(manifest)
    },

    #' Write manifest
    #' @description
    #' Write manifest json
    #' @param ...  @seealso getManifestList
    writeManifest = function(...) {
      outputFile <- file.path(self$exportDir, "manifest.json")
      manifest <- self$getManifestList(...)
      json <- jsonlite::toJSON(manifest, pretty = TRUE, force = TRUE, null = "null", auto_unbox = TRUE)
      write(json, outputFile)
      invisible()
    }
  )
)

#' Create Result Export Manager
#' @export
#' @description
#' For a give table specification file, create an export manager instance for creating results data sets that conform
#' to the data model.
#'
#' This checks that, at export time, internal validity is assured for the data (e.g. primary keys are valid, data types
#' are compatible
#'
#' In addition this utility will create a manifest object that can be used to maintain the validity of data.
#'
#' If an instance of a DataMigrationManager is present and available a packageVersion reference (where applicable)
#' and migration set will be referenced. Allowing data to be imported into a database schema at a specific version.
#'
#' @param tableSpecification        Table specification data.frame
#' @param exportDir                 Directory files are being exported to
#' @param minCellCount              Minimum cell count - reccomended that you set with
#'                                  options("ohdsi.minCellCount" = count) in all R projects. Default is 5
#' @param databaseId                database identifier - required when exporting according to many specs
createResultExportManager <- function(tableSpecification,
                                      exportDir,
                                      minCellCount = getOption("ohdsi.minCellCount", default = 5),
                                      databaseId = NULL) {
  ResultExportManager$new(
    tableSpecification = tableSpecification,
    exportDir = exportDir,
    minCellCount = minCellCount,
    databaseId = databaseId
  )
}
