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

.allowedGrantPermissions <- "SELECT|INSERT|DELETE|UPDATE"

#' Grant Table Permissions
#' @description
#' Grant a given permission for all tables on a given tableSpecification
#'
#' Very useful if you're hosting studies on data.ohdsi.org or other postgresql instances
#'
#' NOTE: only tested on postgresql, users' of other platforms may have Sql translation issues
#'
#' @param tableSpecification   data.frame conforming to table spec (must contain tableName field)
#' @param permissions      permissions to generate must be one of SELECT, INSERT, DELETE or UPDATE
#' @param databaseSchema   database schema to run this on
#' @param user   database user to grant permissions to
#' @param connection   DatabaseConnector connection instance
#' @inheritParams createQueryNamespace
#' @export
grantTablePermissions <- function(connectionDetails = NULL,
                                  connection = NULL,
                                  tableSpecification,
                                  databaseSchema,
                                  tablePrefix = "",
                                  permissions = "SELECT",
                                  user) {
  checkmate::assertString(toupper(permissions), pattern = .allowedGrantPermissions)
  checkmate::assertString(user)
  checkmate::assertString(databaseSchema)
  checkmate::assertDataFrame(tableSpecification)
  checkmate::assertTRUE("tableName" %in% colnames(tableSpecification))
  checkmate::assertClass(connectionDetails, "ConnectionDetails", null.ok = !is.null(connection))

  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  } else {
    stopifnot(DatabaseConnector::dbIsValid(connection))
  }

  if (DatabaseConnector::dbms(connection) == "sqlite") {
    stop("sqlite does not support permission handling")
  } else if (DatabaseConnector::dbms(connection) != "postgresql") {
    warning("Untested grant function for this database platform")
  }

  sql <- c()
  for (table in tableSpecification$tableName %>% unique()) {
    sql <- c(
      sql,
      SqlRender::render("GRANT @permissions ON @database_schema.@table_prefix@table TO @user;",
        table = table,
        table_prefix = tablePrefix
      )
    )
  }

  if (length(sql) >= 1) {
    sql <- paste(sql, collapse = "\n")
    DatabaseConnector::renderTranslateExecuteSql(connection,
      sql,
      user = user,
      database_schema = databaseSchema,
      permissions = permissions
    )
  }
}
