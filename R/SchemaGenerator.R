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

.writeFieldDefinition <- function(field) {
  field <- as.list(field)
  str <- paste("\t", field$columnName, toupper(field$dataType))

  if (field$primaryKey == "yes") {
    str <- paste(str, "NOT NULL")
  }

  str
}

#' Schema generator
#' @description
#' Take a csv schema definition and create a basic sql script with it.
#'
#' @param csvFilepath                   Path to schema file. Csv file must have the columns:
#'                                      "table_name", "colum_name", "data_type", "is_required", "primary_key"
#'                                      Note -
#' @param sqlOutputPath                 File to write sql to.
#' @param overwrite                     Boolean - overwrite existing file?
#' @export
#'
#'
#' @return
#'  string containing the sql for the table
generateSqlSchema <- function(csvFilpath,
                              sqlOutputPath = NULL,
                              overwrite = FALSE) {

  if (!is.null(sqlOutputPath) && (file.exists(sqlOutputPath) & !overwrite))
    stop("Output file ", sqlOutputPath, "already exists. Set overwrite = TRUE to continue")

  checkmate::assertFileExists(csvFilpath)
  schemaDefinition <- readr::read_csv(csvFilpath, show_col_types = FALSE)
  colnames(schemaDefinition) <- SqlRender::snakeCaseToCamelCase(colnames(schemaDefinition))
  requiredFields <- c("tableName", "columnName", "dataType", "isRequired", "primaryKey")
  checkmate::assertNames(colnames(schemaDefinition), must.include = requiredFields)

  tableSqlStr <- "
CREATE TABLE @database_schema.@table_prefix@table_name (
  @table_fields
);
"
  fullScript <- ""
  defs <- "{DEFAULT @table_prefix = ''}\n"

  for(table in unique(schemaDefinition$tableName)) {
    tableFields <- schemaDefinition %>% dplyr::filter(tableName == table)
    fieldDefinitions <- apply(tableFields, 1, .writeFieldDefinition)

    primaryKeyFields <- tableFields %>% dplyr::filter(primaryKey == "yes")
    if (nrow(primaryKeyFields)) {
      pkeyField <- paste0("\tPRIMARY KEY(", paste(primaryKeyFields$columnName, collapse = ","), ")")
      fieldDefinitions <- c(fieldDefinitions, pkeyField)
    }

    fieldDefinitions <- paste(fieldDefinitions, collapse = ",\n")
    tableString <- SqlRender::render(tableSqlStr,
                                     table_name = paste0("@", table),
                                     table_fields = fieldDefinitions)

    tableDefStr <- paste0("{DEFAULT @", table, " = ", table, "}\n")
    defs <- paste0(defs, tableDefStr)

    fullScript <- paste(fullScript, tableString)
  }

  # Get fields for each table
  lines <- paste(defs, fullScript)
  if (!is.null(sqlOutputPath))
    writeLines(lines, sqlOutputPath)

  lines
}
