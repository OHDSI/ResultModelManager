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

.writeColumnDefinition <- function(column) {
  column <- as.list(column)
  str <- paste("\t", column$columnName, toupper(column$dataType))

  if (tolower(column$primaryKey) == "yes") {
    str <- paste(str, "NOT NULL")
  }

  str
}

#' Schema generator
#' @description
#' Take a csv schema definition and create a basic sql script with it.
#' returns string containing the sql for the table
#' @param csvFilepath                   Path to schema file. Csv file must have the columns:
#'                                      "table_name", "column_name", "data_type", "primary_key"
#' @param schemaDefinition              A schemaDefintiion data.frame` with the columns:
#'                                         tableName, columnName, dataType, isRequired, primaryKey
#' @param sqlOutputPath                 File to write sql to.
#' @param overwrite                     Boolean - overwrite existing file?
#' @export
#'
#' @importFrom readr read_csv
generateSqlSchema <- function(csvFilepath = NULL,
                              schemaDefinition = NULL,
                              sqlOutputPath = NULL,
                              overwrite = FALSE) {
  if (all(is.null(c(csvFilepath, schemaDefinition)))) {
    stop("Must spcify a csv file or schema definition")
  } else if (is.null(schemaDefinition)) {
    if (!is.null(sqlOutputPath) && (file.exists(sqlOutputPath) & !overwrite)) {
      stop("Output file ", sqlOutputPath, "already exists. Set overwrite = TRUE to continue")
    }

    checkmate::assertFileExists(csvFilepath)
    schemaDefinition <- readr::read_csv(csvFilepath, show_col_types = FALSE)
    names(schemaDefinition) <- SqlRender::snakeCaseToCamelCase(names(schemaDefinition))
  }
  assertSpecificationColumns(colnames(schemaDefinition))

  tableSqlStr <- "
CREATE TABLE @database_schema.@table_prefix@table_name (
  @table_columns
);
"
  fullScript <- ""
  defs <- "{DEFAULT @table_prefix = ''}\n"

  for (table in unique(schemaDefinition$tableName)) {
    tableColumns <- schemaDefinition[schemaDefinition$tableName == table, ]
    columnDefinitions <- apply(tableColumns, 1, .writeColumnDefinition)

    primaryKeyFields <- tableColumns[tolower(tableColumns$primaryKey) == "yes", ]
    if (nrow(primaryKeyFields)) {
      pkeyField <- paste0("\tPRIMARY KEY(", paste(primaryKeyFields$columnName, collapse = ","), ")")
      columnDefinitions <- c(columnDefinitions, pkeyField)
    }

    columnDefinitions <- paste(columnDefinitions, collapse = ",\n")
    tableString <- SqlRender::render(tableSqlStr,
      table_name = paste0("@", table),
      table_columns = columnDefinitions
    )

    tableDefStr <- paste0("{DEFAULT @", table, " = ", table, "}\n")
    defs <- paste0(defs, tableDefStr)

    fullScript <- paste(fullScript, tableString)
  }

  # Get columns for each table
  lines <- paste(defs, fullScript)
  if (!is.null(sqlOutputPath)) {
    writeLines(lines, sqlOutputPath)
  }

  lines
}
