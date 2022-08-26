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
.availableDbms <- c("oracle",
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
                    "hive")

#' DataMigrationManager (DMM)
#' @description
#' R6 class for management of database migration
#'
#' @field migrationPath                 Path migrations exist in
#' @field resulultsDatabaseSchema       Path migrations exist in
#' @field connection                    Database connection
#' @field packageName                   packageName, can be null
#'
#' @export
DataMigrationManager <- R6::R6Class(
  "DataMigrationManager",
  public = list(
    migrationPath = NULL,
    tablePrefix = "",
    resultsDatabaseSchema = NULL,
    connection = NULL,
    packageName = NULL,
    #' initalize
    initialize = function(connectionDetails,
                          resultsDatabaseSchema,
                          tablePrefix = "",
                          migrationPath,
                          packageName = NULL,
                          migrationRegexp = .defaultMigrationRegexp) {
      checkmate::checkString(resultsDatabaseSchema)
      checkmate::checkString(tablePrefix)
      checkmate::checkString(migrationPath)
      # Set required variables
      self$tablePrefix <- tablePrefix
      self$resultsDatabaseSchema <- resultsDatabaseSchema
      private$migrationRegexp <- migrationRegexp
      self$packageName <- packageName
      self$connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
      self$migrationPath <- migrationPath

      if (self$isPackage()) {
        ParallelLogger::logInfo("Migrator using SQL files in ", packageName)
      } else {
        ParallelLogger::logInfo("Migrator using SQL files in directory structure")
      }
    },

    migrationTableExists = function() {
      tables <- DatabaseConnector::getTableNames(self$connection, self$resultsDatabaseSchema)
      return(toupper(paste0(self$tablePrefix, "migration")) %in% tables)
    },

    #'
    #'
    getMigrationsPath = function() {
      if (is.null(self$packageName)) {
        path <- list.files(file.path(self$migrationPath, "sql_server"), pattern = "*.sql")
      } else {
        path <- system.file(file.path("sql", "sql_server", self$migrationPath), package = self$packageName)
      }
      return(path)
    },

    #' Get status of result model
    #'
    #' @returns data frame all migrations, including file name, order and execution status
    getStatus = function() {
      allMigrations <- list.files(self$getMigrationsPath(), pattern = "*.sql")

      if (self$migrationTableExists()) {
        migrationsExecuted <- private$getCompletedMigrations()
        if (nrow(migrationsExecuted) > 0) {
          migrationsExecuted$executed <- TRUE
          migrationsToExecute <- setdiff(allMigrations, migrationsExecuted$migrationFile)
          browser()
          migrations <- private$getMigrationOrder(migrationsToExecute)
          migrations$executed <- FALSE
          migrations <- rbind(migrationsExecuted, migrations)
          migrations[order(migrationOrder), ]
        } else {
          migrationsToExecute <- allMigrations
          migrations <- private$getMigrationOrder(migrationsToExecute)
          migrations$executed <- FALSE
        }
      } else {
        migrationsToExecute <- allMigrations
        migrations <- private$getMigrationOrder(migrationsToExecute)
        migrations$executed <- FALSE
      }

      return(migrations)
    },

    #' Check migrations in folder
    #'
    #'
    check = function() {
      # Check to see if files follow pattern
      sqlFiles <- list.files(self$getMigrationsPath(), pattern = "*.sql")
      # TODO: check if files for different db platforms are in valid directories
      fileNameValidity <- grepl(private$migrationRegexp, sqlFiles)

      if (any(fileNameValidity == 0)) {
        ParallelLogger::logError(paste("File name not valid", sqlFiles[fileNameValidity == 0], collapse = "\n"))
      }
     return(all(fileNameValidity > 0))
    },

    #' Execute Migrations
    #'
    #'
    executeMigrations = function() {
      # if (interactive()) {
      #   resp <- askYesNo("Migrations can cause permanent alteration and result in data loss. Backups are reccomended. Proceed?")
      #   if (is.na(resp) | isFALSE(resp)) {
      #     return()
      #   }
      # }
      # if migrations table doesn't exist, create it
      if (!self$migrationTableExists()) {
        private$createMigrationsTable()
      }
      # load list of migrations
      migrations <- self$getStatus()
      # execute migrations that haven't been executed yet
      migrations <- migrations[!migrations$executed,]
      for (i in 1:nrow(migrations)) {
        private$executeMigration(migrations[i,])
      }
    },

    isPackage = function() {
      return(!is.null(self$packageName))
    },

    finalize = function() {
      DatabaseConnector::disconnect(self$connection)
    }
  ),
  private = list(
    migrationRegexp = NULL,
    executeMigration = function(migration) {
      ParallelLogger::logInfo("Executing migration: ", migration$migrationFile)
      # Load, render, translate and execute sql
      if (self$isPackage()) {
        sql <- SqlRender::loadRenderTranslateSql(file.path(self$migrationPath, migration$migrationFile),
                                                 dbms = self$connection@dbms,
                                                 database_schema = self$resultsDatabaseSchema,
                                                 table_prefix = self$tablePrefix,
                                                 packageName = self$packageName)
        DatabaseConnector::executeSql(connection = self$connection, sql = sql)
      } else {
        # Check to see if a file for database platform exists
        if (file.exists(file.path(self$migrationPath, self$connection$dbms, migration$filePath))) {
          sql <- SqlRender::readSql(file.path(self$migrationPath, self$connection$dbms, migration$filePath))
          sql <- SqlRender::render(sql,
                                   database_schema = self$resultsDatabaseSchema,
                                   table_prefix = self$tablePrefix)
          DatabaseConnector::executeSql(connection = self$connection, sql = sql)
        } else {
          # Default to using sql_server as path
          sql <- SqlRender::readSql(file.path(self$migrationPath, "sql_server", migration$filePath))
          DatabaseConnector::renderTranslateExecuteSql(connection = self$connection,
                                                       sql = sql,
                                                       database_schema = self$resultsDatabaseSchema,
                                                       table_prefix = self$tablePrefix)
        }
      }
      ParallelLogger::logInfo("Saving migration: ", migration$filePath)
      # Save migration in set of migrations
      iSql <- "
      {DEFAULT @migration = migration}
      INSERT INTO @results_schema.@table_prefix@migration (migration_file, migration_order)
        VALUES ('@migration_file', @order);
      "
      DatabaseConnector::renderTranslateExecuteSql(self$connection,
                                                   sql = iSql, results_schema = self$resultsDatabaseSchema,
                                                   migration_file = migration$migrationFile,
                                                   table_prefix = self$table_prefix,
                                                   order = migration$migrationOrder)
      ParallelLogger::logInfo("Migration complete ", migration$filePath)
    },

    createMigrationsTable = function() {
      ParallelLogger::logInfo("Creating migrations table")
      sql <- "
      {DEFAULT @migration = migration}
      --HINT DISTRIBUTE ON RANDOM
      CREATE TABLE @results_schema.@table_prefix@migration (
          migration_file VARCHAR PRIMARY KEY, --string value represents file name
          migration_order INT NOT NULL unique
      );"

      DatabaseConnector::renderTranslateExecuteSql(self$connection,
                                                   sql = sql,
                                                   results_schema = self$resultsDatabaseSchema,
                                                   table_prefix = self$tablePrefix)
      ParallelLogger::logInfo("Migrations table created")
    },

    getCompletedMigrations = function() {
      sql <- "
      {DEFAULT @migration = migration}
      SELECT migration_file FROM @result_schema.@table_prefix@migration ORDER BY migration_order;"
      migrationsExecuted <- DatabaseConnector::renderTranslateQuerySql(self$connection,
                                                                       sql = sql,
                                                                       result_schema = self$resultsDatabaseSchema,
                                                                       table_prefix = self$tablePrefix,
                                                                       snakeCaseToCamelCase = TRUE)

      return(migrationsExecuted)
    },

    # Convert string patterns to int
    getMigrationOrder = function(migrations) {
      execution <- data.frame(
        migrationFile = migrations,
        migrationOrder = as.integer(gsub(private$migrationRegexp, "\\2", migrations))
      )
      return(execution[order(execution$migrationOrder),])
    }
  ),
)
