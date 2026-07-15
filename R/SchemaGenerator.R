# Copyright 2025 Observational Health Data Sciences and Informatics
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

.getPlatformConfig <- function(platforms, platform, namespace, table) {
  if (is.null(platforms) || is.null(platform)) {
    return(NULL)
  }
  platforms[[platform]]$namespace[[namespace]]$tables[[table]]
}

.generatePartitionBy <- function(partitionBy, tableName, platform) {
  if (is.null(partitionBy)) {
    return("")
  }

  supportedPlatforms <- c("postgresql")
  if (!platform %in% supportedPlatforms) {
    warning(sprintf(
      "Partitioning is not supported for platform '%s'. Supported platforms: %s. Skipping partitioning for table '%s'.",
      platform, paste(supportedPlatforms, collapse = ", "), tableName
    ))
    return("")
  }

  paste0(" PARTITION BY ", partitionBy)
}

.generateIndexes <- function(platformConfig, platform) {
  if (is.null(platformConfig) || is.null(platformConfig$indexes)) {
    return("")
  }

  supportedPlatforms <- c("postgresql", "sql_server")
  if (!platform %in% supportedPlatforms) {
    warning(sprintf(
      "Index DDL generation is not supported for platform '%s'. Supported platforms: %s.",
      platform, paste(supportedPlatforms, collapse = ", ")
    ))
    return("")
  }

  indexStatements <- character()
  for (idx in platformConfig$indexes) {
    columns <- paste(idx$columns, collapse = ", ")
    uniqueStr <- if (isTRUE(idx$unique)) "UNIQUE " else ""
    indexName <- gsub("[^a-zA-Z0-9_]", "_", paste(c("idx", idx$columns), collapse = "_"))
    indexStatements <- c(indexStatements,
      sprintf("\nCREATE %sINDEX %s ON @database_schema.@table_prefix@table_name (%s);",
        uniqueStr, indexName, columns))
  }

  paste(indexStatements, collapse = "")
}

#' Schema generator
#' @export
#' @description
#' Take a csv or yaml schema definition and create a basic sql script with it.
#' For YAML files with platform-specific configuration, use the `platform` parameter
#' to include features like partitioning and indexes.
#' returns string containing the sql for the table
#' @param csvFilepath                   Path to schema file (csv or yaml). Csv file must have the columns:
#'                                      "table_name", "column_name", "data_type", "primary_key".
#'                                      Yaml file must follow the namespaced YAML schema format.
#' @param schemaDefinition              A schemaDefinition data.frame with the columns:
#'                                         tableName, columnName, dataType, isRequired, primaryKey.
#'                                         May optionally include a 'namespace' column.
#' @param sqlOutputPath                 File to write sql to.
#' @param overwrite                     Boolean - overwrite existing file?
#' @param platform                      Target database platform for platform-specific DDL
#'                                      (e.g. "postgresql", "sql_server", "sqlite", "duckdb").
#'                                      Only applies when loading from a YAML file.
generateSqlSchema <- function(csvFilepath = NULL,
                              schemaDefinition = NULL,
                              sqlOutputPath = NULL,
                              overwrite = FALSE,
                              platform = NULL) {
  if (all(is.null(c(csvFilepath, schemaDefinition)))) {
    stop("Must spcify a csv or yaml file or schema definition")
  }

  platformConfig <- NULL

  if (is.null(schemaDefinition)) {
    if (!is.null(sqlOutputPath) && (file.exists(sqlOutputPath) & !overwrite)) {
      stop("Output file ", sqlOutputPath, "already exists. Set overwrite = TRUE to continue")
    }

    checkmate::assertFileExists(csvFilepath)

    if (grepl("\\.ya?ml$", csvFilepath, ignore.case = TRUE)) {
      yamlResult <- loadResultsDataModelFromYaml(csvFilepath)
      schemaDefinition <- yamlResult$specification
      if (!is.null(platform)) {
        platformConfig <- yamlResult$platforms
      }
    } else {
      warning(
        "CSV-based schema definitions are deprecated. ",
        "Use the namespaced YAML format instead. ",
        "See vignette('UploadFunctionality') and the csvToYaml() function to migrate.",
        call. = FALSE
      )
      schemaDefinition <- readr::read_csv(csvFilepath, show_col_types = FALSE)
      names(schemaDefinition) <- SqlRender::snakeCaseToCamelCase(names(schemaDefinition))
    }
  }
  assertSpecificationColumns(colnames(schemaDefinition))

  hasNamespace <- "namespace" %in% colnames(schemaDefinition)
  hasNamespacePrefix <- "namespacePrefix" %in% colnames(schemaDefinition)

  tableSqlStr <- "
CREATE TABLE @database_schema.@table_prefix@table_name (
  @table_columns
)@partition_by;
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

    varName <- table
    if (hasNamespace) {
      ns <- tableColumns$namespace[1]
      if (!is.na(ns)) {
        if (hasNamespacePrefix) {
          nsPrefix <- tableColumns$namespacePrefix[1]
          if (!is.na(nsPrefix)) {
            varName <- paste0(nsPrefix, table)
          } else {
            varName <- paste0(ns, "_", table)
          }
        } else {
          varName <- paste0(ns, "_", table)
        }
      }
    }

    partitionBySql <- ""
    if (!is.null(platformConfig) && hasNamespace) {
      ns <- tableColumns$namespace[1]
      if (!is.na(ns)) {
        tblPlatformConfig <- .getPlatformConfig(platformConfig, platform, ns, table)
        partitionBySql <- .generatePartitionBy(tblPlatformConfig$partition_by, varName, platform)
      }
    }

    tableString <- SqlRender::render(tableSqlStr,
      table_name = paste0("@", varName),
      table_columns = columnDefinitions,
      partition_by = partitionBySql
    )

    tableDefStr <- paste0("{DEFAULT @", varName, " = ", varName, "}\n")
    defs <- paste0(defs, tableDefStr)

    indexSql <- ""
    if (!is.null(platformConfig) && hasNamespace) {
      ns <- tableColumns$namespace[1]
      if (!is.na(ns)) {
        tblPlatformConfig <- .getPlatformConfig(platformConfig, platform, ns, table)
        indexSql <- .generateIndexes(tblPlatformConfig, platform)
        if (nchar(indexSql) > 0) {
          indexSql <- SqlRender::render(indexSql,
            table_name = paste0("@", varName)
          )
        }
      }
    }

    fullScript <- paste(fullScript, tableString, indexSql)
  }

  lines <- paste(defs, fullScript)
  if (!is.null(sqlOutputPath)) {
    writeLines(lines, sqlOutputPath)
  }

  lines
}
