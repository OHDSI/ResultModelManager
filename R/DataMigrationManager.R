# Copyright 2022 Observational Health Data Sciences and Informatics
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

.defaultMigrationRegexp <- "(Migration_([0-9]+))-(.+).sql"
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
  "hive"
)

#' DataMigrationManager (DMM)
#' @description
#' R6 class for management of database migration
#'
#' @field migrationPath                 Path migrations exist in
#' @field databaseSchema                Path migrations exist in
#' @field packageName                   packageName, can be null
#' @field tablePrefix                   packageName, can be null
#'
#' @export
DataMigrationManager <- R6::R6Class(
  "DataMigrationManager",
  public = list(
    migrationPath = NULL,
    tablePrefix = "",
    databaseSchema = NULL,
    packageName = NULL,
    #'
    #' @param connectionDetails         DatabaseConnector connection details object
    #' @param databaseSchema            Database Schema to execute on
    #' @param tablePrefix               Optional table prefix for all tables (e.g. plp, cm, cd etc)
    #' @param packageName               If in package mode, the name of the R package
    #' @param migrationPath             Path to location of migration sql files. If in package mode, this should just
    #'                                  be a folder (e.g. "migrations") that lives in the location "sql/sql_server" (and)
    #'                                  other database platforms.
    #'                                  If in folder model, the folder must include "sql_server" in the relative path,
    #'                                  (e.g if  migrationPath = 'migrations' then the folder 'migrations/sql_server' should exists)
    #' @param migrationRegexp            (Optional) regular expression pattern default is `(Migration_([0-9]+))-(.+).sql`
    initialize = function(connectionDetails,
                          databaseSchema,
                          tablePrefix = "",
                          migrationPath,
                          packageName = NULL,
                          migrationRegexp = .defaultMigrationRegexp) {
      checkmate::checkString(databaseSchema)
      checkmate::checkString(tablePrefix)
      checkmate::checkString(migrationPath)
      checkmate::checkClass(connectionDetails, "connectionDetails")

      # Set required variables
      self$tablePrefix <- tablePrefix
      self$databaseSchema <- databaseSchema
      private$migrationRegexp <- migrationRegexp
      self$packageName <- packageName
      private$connectionDetails <- connectionDetails
      private$connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
      self$migrationPath <- migrationPath

      if (self$isPackage()) {
        ParallelLogger::logInfo("Migrator using SQL files in ", packageName)
      } else {
        ParallelLogger::logInfo("Migrator using SQL files in directory structure")
      }
    },

    #' Migration table exists
    #' @description
    #' Check if migration table is present in schema
    #' @return boolean
    migrationTableExists = function() {
      tables <- DatabaseConnector::getTableNames(private$connection, self$databaseSchema)
      return(toupper(paste0(self$tablePrefix, "migration")) %in% tables)
    },

    #' Get path of migrations
    #' @description
    #' Get path to sql migration files
    #' @param dbms               Optionally specify the dbms that the migration fits under
    getMigrationsPath = function(dbms = "sql server") {
      checkmate::assertChoice(dbms, .availableDbms)
      if (!self$isPackage()) {
        path <- file.path(self$migrationPath, gsub(" ", "_", dbms))
      } else {
        path <- system.file(file.path("sql", gsub(" ", "_", dbms), self$migrationPath), package = self$packageName)
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
          ParallelLogger::logError(paste("File name not valid", sqlFiles[fileNameValidity == 0], collapse = "\n"))
        }

        allNameValidity <- c(allNameValidity, fileNameValidity)

        if (dbms != "sql server") {
          for (file in sqlFiles) {
            if (!file %in% baseFiles) {
              allDirValid <- c(allDirValid, FALSE)
              ParallelLogger::logError(paste(
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
    executeMigrations = function() {
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
      for (i in 1:nrow(migrations)) {
        private$executeMigration(migrations[i, ])
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
      DatabaseConnector::disconnect(private$connection)
    }
  ),
  private = list(
    migrationRegexp = NULL,
    connection = NULL,
    connectionDetails = NULL,
    executeMigration = function(migration) {
      ParallelLogger::logInfo("Executing migration: ", migration$migrationFile)
      # Load, render, translate and execute sql
      if (self$isPackage()) {
        sql <- SqlRender::loadRenderTranslateSql(file.path(self$migrationPath, migration$migrationFile),
          dbms = private$connection@dbms,
          database_schema = self$databaseSchema,
          table_prefix = self$tablePrefix,
          packageName = self$packageName
        )
        DatabaseConnector::executeSql(connection = private$connection, sql = sql)
      } else {
        # Check to see if a file for database platform exists
        if (file.exists(file.path(self$migrationPath, private$connection@dbms, migration$migrationFile))) {
          sql <- SqlRender::readSql(file.path(self$migrationPath, private$connection@dbms, migration$migrationFile))
          sql <- SqlRender::render(sql,
            database_schema = self$databaseSchema,
            table_prefix = self$tablePrefix
          )
          DatabaseConnector::executeSql(connection = private$connection, sql = sql)
        } else {
          # Default to using sql_server as path
          sql <- SqlRender::readSql(file.path(self$migrationPath, "sql_server", migration$migrationFile))
          DatabaseConnector::renderTranslateExecuteSql(
            connection = private$connection,
            sql = sql,
            database_schema = self$databaseSchema,
            table_prefix = self$tablePrefix
          )
        }
      }
      ParallelLogger::logInfo("Saving migration: ", migration$filePath)
      # Save migration in set of migrations
      iSql <- "
      {DEFAULT @migration = migration}
      INSERT INTO @database_schema.@table_prefix@migration (migration_file, migration_order)
        VALUES ('@migration_file', @order);
      "
      DatabaseConnector::renderTranslateExecuteSql(private$connection,
        sql = iSql, database_schema = self$databaseSchema,
        migration_file = migration$migrationFile,
        table_prefix = self$table_prefix,
        order = migration$migrationOrder
      )
      ParallelLogger::logInfo("Migration complete ", migration$filePath)
    },
    createMigrationsTable = function() {
      ParallelLogger::logInfo("Creating migrations table")
      sql <- "
      {DEFAULT @migration = migration}
      --HINT DISTRIBUTE ON RANDOM
      CREATE TABLE @database_schema.@table_prefix@migration (
          migration_file VARCHAR PRIMARY KEY, --string value represents file name
          migration_order INT NOT NULL unique
      );"

      DatabaseConnector::renderTranslateExecuteSql(private$connection,
        sql = sql,
        database_schema = self$databaseSchema,
        table_prefix = self$tablePrefix
      )
      ParallelLogger::logInfo("Migrations table created")
    },
    getCompletedMigrations = function() {
      if (!self$migrationTableExists()) {
        return(data.frame())
      }

      sql <- "
      {DEFAULT @migration = migration}
      SELECT migration_file, migration_order FROM @database_schema.@table_prefix@migration ORDER BY migration_order;"
      migrationsExecuted <- DatabaseConnector::renderTranslateQuerySql(private$connection,
        sql = sql,
        database_schema = self$databaseSchema,
        table_prefix = self$tablePrefix,
        snakeCaseToCamelCase = TRUE
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
    }
  ),
)
