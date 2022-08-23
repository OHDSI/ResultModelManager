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

#' DataMigrationManager (DMM)
#' @description
#' R6 class for management of database migration
#'
#' @field migrationPath         Path migrations exist in
#' @field resulultsDatabaseSchema       Path migrations exist in
#' @field connection       Database connection
#'
#' @export
DataMigrationManager <- R6::R6Class(
  "DataMigrationManager",
  private = list(
    executeMigration = function(filePath) {
      # Load, render, translate and execute sql

      # Save migration in set of migrations

      # Error handling - stop execution, restore transaction
    }
  ),
  public = list(
    migrationPath = NULL,
    resulultsDatabaseSchema = NULL,
    connection = NULL,
    #' initalize
    #'
    #'
    initalize = function(connectionDetails,
                         resultsDatabaseSchema,
                         tablePrefix,
                         migrationsPath,
                         migrationRegexp = .defaultMigrationRegexp) {
      # Set required variables
    },

    #' Get status of result model
    #'
    #'
    getStatus = function() {
      # return data frame all migrations, including file name, order and
    },

    #' Check migrations in folder
    #'
    #'
    check = function() {
      # Check to see if files follow pattern
    },

    #' Execute Migrations
    #'
    #'
    executeMigrations = function() {
      # load list of migrations
      # Load list of executed migrations
      # if migrations table doesn't exist, create it
      # execute migrations that haven't been executed yet
    }
  )
)
