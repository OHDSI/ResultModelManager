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

isRmdCheck <- function() {
  return(Sys.getenv("_R_CHECK_PACKAGE_NAME_", "") != "")
}

isUnitTest <- function() {
  return(tolower(Sys.getenv("TESTTHAT", "")) == "true")
}

.defaultMigrationRegexp <- "(Migration_([0-9]+))-(.+).sql"
# Taken from database connector - may be better if the package exposed this list formally
.availableDbms <- c(
  "oracle",
  "hive",
  "postgresql",
  "redshift",
  "sql server",
  "pdw",
  "netezza",
  "impala",
  "bigquery",
  "sqlite",
  "sqlite extended",
  "spark",
  "hive",
  "duckdb",
  "snowflake",
  "synapse"
)

#' DataMigrationManager (DMM)
#' @description
#' R6 class for management of database migration
#'
#' @field migrationPath                 Path migrations exist in
#' @field databaseSchema                Path migrations exist in
#' @field packageName                   packageName, can be null
#' @field tablePrefix                   tablePrefix, can be empty character vector
#' @field packageTablePrefix                   packageTablePrefix, can be empty character vector
#'
#' @importFrom ParallelLogger logError logInfo
#'
#' @export  DataMigrationManager
DataMigrationManager <- R6::R6Class(
  classname = "DataMigrationManager",
  public = list(
    migrationPath = NULL,
    tablePrefix = "",
    packageTablePrefix = "",
    databaseSchema = NULL,
    packageName = NULL,
    #'
    #' @param connectionDetails         DatabaseConnector connection details object
    #' @param databaseSchema            Database Schema to execute on
    #' @param tablePrefix               Optional table prefix for all tables (e.g. plp, cm, cd etc)
    #' @param packageName               If in package mode, the name of the R package
    #' @param packageTablePrefix        A table prefix when used in conjunction with other package results schema,
    #'                                  e.g. "cd_", "sccs_", "plp_", "cm_"
    #' @param migrationPath             Path to location of migration sql files. If in package mode, this should just
    #'                                  be a folder (e.g. "migrations") that lives in the location "sql/sql_server" (and)
    #'                                  other database platforms.
    #'                                  If in folder model, the folder must include "sql_server" in the relative path,
    #'                                  (e.g if  migrationPath = 'migrations' then the folder 'migrations/sql_server' should exists)
    #' @param migrationRegexp            (Optional) regular expression pattern default is `(Migration_([0-9]+))-(.+).sql`
    initialize = function(connectionDetails,
                          databaseSchema,
                          tablePrefix = "",
                          packageTablePrefix = "",
                          migrationPath,
                          packageName = NULL,
                          migrationRegexp = .defaultMigrationRegexp) {
      checkmate::checkString(databaseSchema)
      checkmate::checkString(tablePrefix)
      checkmate::checkString(packageTablePrefix)
      checkmate::checkString(migrationPath)
      checkmate::checkClass(connectionDetails, "connectionDetails")

      # Set required variables
      self$tablePrefix <- tablePrefix
      self$packageTablePrefix <- packageTablePrefix
      self$databaseSchema <- databaseSchema
      private$migrationRegexp <- migrationRegexp
      self$packageName <- packageName
      private$connectionDetails <- connectionDetails
      private$connectionHandler <- ConnectionHandler$new(connectionDetails, loadConnection = FALSE)
      self$migrationPath <- migrationPath

      if (self$isPackage()) {
        private$logInfo("Migrator using SQL files in ", packageName)
      } else {
        private$logInfo("Migrator using SQL files in directory structure")
      }
    },

    #' Migration table exists
    #' @description
    #' Check if migration table is present in schema
    #' @return boolean
    migrationTableExists = function() {
      tables <- DatabaseConnector::getTableNames(private$connectionHandler$getConnection(), self$databaseSchema)
      return(tolower(paste0(self$tablePrefix, "migration")) %in% tables)
    },

    #' Get path of migrations
    #' @description
    #' Get path to sql migration files
    #' @param dbms               Optionally specify the dbms that the migration fits under
    getMigrationsPath = function(dbms = "sql server") {
      checkmate::assertChoice(dbms, .availableDbms)
      dbms <- gsub(" ", "_", dbms)
      if (!self$isPackage()) {
        path <- file.path(self$migrationPath, dbms)
      } else {
        path <- system.file(file.path("sql", dbms, self$migrationPath), package = self$packageName)
      }
      return(path)
    },

    #' Get status of result model
    #' @description
    #' Get status of all migrations (executed or not)
    #' @returns data frame all migrations, including file name, order and execution status
    getStatus = function() {
      allMigrations <- list.files(self$getMigrationsPath(), pattern = "*.sql")
      if (length(allMigrations) == 0) {
        return(data.frame())
      }

      migrationsExecuted <- private$getCompletedMigrations()
      if (nrow(migrationsExecuted) > 0) {
        migrationsExecuted$executed <- TRUE
        migrationsToExecute <- setdiff(allMigrations, migrationsExecuted$migrationFile)
        migrations <- private$getMigrationOrder(migrationsToExecute)

        if (nrow(migrations) > 0) {
          migrations$executed <- FALSE
        }
        migrations <- rbind(migrationsExecuted, migrations)
        migrations[order(migrations$migrationOrder), ]
      } else {
        migrations <- private$getMigrationOrder(allMigrations)
        migrations$executed <- FALSE
      }

      return(migrations)
    },

    #' Get connection handler
    #' @description
    #' Return connection handler instance
    #' @seealso[ConnectionHandler] for information on returned class
    #' @return  ConnectionHandler instance
    getConnectionHandler = function() {
      return(private$connectionHandler)
    },

    #' Check migrations in folder
    #' @description
    #' Check if file names are valid for migrations
    check = function() {
      baseFiles <- list.files(self$getMigrationsPath(), pattern = "*.sql")

      allNameValidity <- c()
      allDirValid <- c()
      # Check if files exist for all db platforms
      for (dbms in .availableDbms) {
        # Check to see if files follow pattern
        sqlFiles <- list.files(self$getMigrationsPath(dbms = dbms), pattern = "*.sql")
        fileNameValidity <- grepl(private$migrationRegexp, sqlFiles)

        if (any(fileNameValidity == 0)) {
          private$logError(paste("File name not valid", sqlFiles[fileNameValidity == 0], collapse = "\n"))
        }

        allNameValidity <- c(allNameValidity, fileNameValidity)

        if (dbms != "sql server") {
          for (file in sqlFiles) {
            if (!file %in% baseFiles) {
              allDirValid <- c(allDirValid, FALSE)
              private$logError(paste(
                "File ", file.path(dbms, file),
                "not found in sql server dir. Even if migration is platform specific ",
                "it should be mached in sql_server with dummy sql"
              ))
            } else {
              allDirValid <- c(allDirValid, TRUE)
            }
          }
        }
      }

      return(all(allNameValidity > 0))
    },

    #' Execute Migrations
    #' @description
    #' Execute any unexecuted migrations
    #' @param stopMigrationVersion                      (Optional) Migrate to a specific migration number
    executeMigrations = function(stopMigrationVersion = NULL) {
      if (!self$check()) {
        stop("Check failed, aborting migrations until issues are resolved")
      }

      # if migrations table doesn't exist, create it
      if (!self$migrationTableExists()) {
        private$createMigrationsTable()
      }
      # load list of migrations
      migrations <- self$getStatus()
      # execute migrations that haven't been executed yet
      migrations <- migrations[!migrations$executed, ]
      if (nrow(migrations) > 0) {
        if (is.null(stopMigrationVersion)) {
          stopMigrationVersion <- max(migrations$migrationOrder)
        }

        for (i in 1:nrow(migrations)) {
          migration <- migrations[i, ]
          if (isTRUE(migration$migrationOrder <= stopMigrationVersion)) {
            private$executeMigration(migration)
          }
        }
      }
    },

    #' isPackage
    #' @description
    #' is a package folder structure or not
    isPackage = function() {
      return(!is.null(self$packageName))
    },

    #' finalize
    #' @description close database connection
    finalize = function() {
      private$connectionHandler$finalize()
    }
  ),
  private = list(
    migrationRegexp = NULL,
    connectionHandler = NULL,
    connectionDetails = NULL,
    executeMigration = function(migration) {
      private$logInfo("Executing migration: ", migration$migrationFile)
      # Load, render, translate and execute sql
      if (self$isPackage()) {
        sql <- SqlRender::loadRenderTranslateSql(file.path(self$migrationPath, migration$migrationFile),
          dbms = private$connectionDetails$dbms,
          database_schema = self$databaseSchema,
          table_prefix = self$tablePrefix,
          packageName = self$packageName
        )

        dialect <- attr(sql, "sqlDialect", TRUE)

        if (is.null(dialect)) {
          # Unknown as to why SqlRender doesn't always set this
          attr(sql, "sqlDialect") <- private$connectionDetails$dbms
        }

        private$connectionHandler$executeSql(sql)
      } else {
        # Check to see if a file for database platform exists
        if (file.exists(file.path(self$migrationPath, private$connectionDetails$dbms, migration$migrationFile))) {
          sql <- SqlRender::readSql(file.path(self$migrationPath, private$connectionDetails$dbms, migration$migrationFile))
        } else {
          # Default to using sql_server as path
          sql <- SqlRender::readSql(file.path(self$migrationPath, "sql_server", migration$migrationFile))
        }
        private$connectionHandler$executeSql(sql,
          database_schema = self$databaseSchema,
          table_prefix = self$tablePrefix
        )
      }
      private$logInfo("Saving migration: ", migration$migrationFile)
      # Save migration in set of migrations
      iSql <- "
      INSERT INTO @database_schema.@table_prefix@migration (migration_file, migration_order)
        VALUES ('@migration_file', @order);
      "
      private$connectionHandler$executeSql(iSql,
        database_schema = self$databaseSchema,
        migration_file = migration$migrationFile,
        table_prefix = self$tablePrefix,
        migration = paste0(self$packageTablePrefix, "migration"),
        order = migration$migrationOrder
      )
      private$logInfo("Migration complete ", migration$migrationFile)
    },
    createMigrationsTable = function() {
      private$logInfo("Creating migrations table")
      sql <- "
      --HINT DISTRIBUTE ON RANDOM
      CREATE TABLE @database_schema.@table_prefix@migration (
          migration_file VARCHAR PRIMARY KEY,
          migration_order INT NOT NULL unique
      );"

      private$connectionHandler$executeSql(sql,
        database_schema = self$databaseSchema,
        table_prefix = self$tablePrefix,
        migration = paste0(self$packageTablePrefix, "migration")
      )
      private$logInfo("Migrations table created")
    },
    getCompletedMigrations = function() {
      if (!self$migrationTableExists()) {
        return(data.frame())
      }

      sql <- "
      SELECT migration_file, migration_order FROM @database_schema.@table_prefix@migration ORDER BY migration_order;"
      migrationsExecuted <- private$connectionHandler$queryDb(sql,
        database_schema = self$databaseSchema,
        migration = paste0(self$packageTablePrefix, "migration"),
        table_prefix = self$tablePrefix
      )

      return(migrationsExecuted)
    },

    # Convert string patterns to int
    getMigrationOrder = function(migrations) {
      execution <- data.frame(
        migrationFile = migrations,
        migrationOrder = as.integer(gsub(private$migrationRegexp, "\\2", migrations))
      )
      return(execution[order(execution$migrationOrder), ])
    },

    # Wrapper calls to stop ParallelLogger outputting annoying messages
    logError = function(...) {
      if (isUnitTest() | isRmdCheck()) {
        writeLines(text = .makeMessage(...))
      } else {
        ParallelLogger::logError(...)
      }
    },
    logInfo = function(...) {
      if (isUnitTest() | isRmdCheck()) {
        writeLines(text = .makeMessage(...))
      } else {
        ParallelLogger::logInfo(...)
      }
    }
  ),
)
