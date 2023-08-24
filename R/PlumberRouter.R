# Copyright 2023 Observational Health Data Sciences and Informatics
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

.getOpenApiSpecType <- function(x) {
  if (x %in% c("varchar", "character", "date")) {
    return("string")
  }

  if (x %in% c("numeric", "float", "int", "bigint")) {
    return("number")
  }

  x
}

.defaultPostHandler <- function(req, qns, tableName, tablePrefix, columnSpecs, ...) {
  sql <- SqlRender::render("SELECT * FROM @schema.@table_prefix@table_name",
                           table_name = tableName,
                           table_prefix = tablePrefix,
                           warnOnMissingParameters = FALSE)

  queryParams <- "WHERE 1 = 1"
  params <- list()
  for (param in columnSpecs$columnName) {
    camelParam <- SqlRender::snakeCaseToCamelCase(param)
    reqVal <- req$body[[camelParam]]

    if (is.null(reqVal)) {
      dataType <- columnSpecs %>%
        dplyr::filter(.data$columnName == param) %>%
        dplyr::select("dataType") %>%
        dplyr::pull()
      # Wrap query in comments
      if (dataType %in% c("character", "varchar", "date")) {
        reqVal <- paste0("'", reqVal, "'")
      }

      if (length(reqVal) == 1) {
        queryParam <- SqlRender::render("AND @param = @req_val", param = param, req_val = reqVal)
      } else {
        queryParam <- SqlRender::render("AND @param IN (@req_val)", param = param, req_val = reqVal)
      }
      queryParams <- c(queryParams, queryParam)
    }
  }

  params$sql <- paste(c(sql, queryParams), collapse = "\n")
  do.call(qns$queryDb, params)
}

#' Create a router for a plumber API
#' @description
#' create a plumber router for each table within a given table space.
#'
#' This is intended to be a basic web route to your data model and tables are automatically exposed.
#' However, you will likely want to expand this with additional results that join across tables or query the data
#' in specific ways. This is possible by expanding the route provided here with the
#' [Plumber API](https://www.rplumber.io/articles/programmatic-usage.html).
#'
#' If using multiple routers within a singler served plumber application is highly advised to use the same
#' ConnectionHandler instance across all routes.
#'
#' For example, mounting an additional file can be achieved with plumber::pr_mount
#'
#' Note that, by default, a new environment is created for the router rather than using the global nnv.
#'
#' use plumberEnv = .GlobalEnv to override this behaviour (not reccomended)
#' @returns plumber::pr router (invisibly)
#'
#' @export
#' @param ...       @seealso createQueryNamespace
#' @param apiTitle  Name to give API
#' @param apiVersion  API version number (defaults to 0.0.1)
#' @inheritParams createQueryNamespace
#' @param plumberEnv    (Optional) environment for access to plumber environment
createPlumberRouter <- function(tableSpecification,
                                plumberEnv = new.env(),
                                tablePrefix = "",
                                apiTitle = "OHDSI result set",
                                apiVersion = "0.0.1",
                                ...) {
  checkmate::assertEnvironment(plumberEnv)
  assertSpecificationColumns(colnames(tableSpecification))

  qns <- createQueryNamespace(tableSpecification = tableSpecification, tablePrefix = tablePrefix, ...)
  plumberEnv$qns <- qns
  plumberEnv$tableSpecification <- tableSpecification
  plumberEnv$tablePrefix <- tablePrefix

  router <- plumber::pr(envir = plumberEnv) %>%
    plumber::pr_get("/table_spec",
                    function() tableSpecification,
                    serializer = plumber::serializer_csv())

  lapply(unique(tableSpecification$tableName), function(tableName) {
    tablePath <- paste0("/", tableName)
    columnSpecs <- tableSpecification %>% dplyr::filter(.data$tableName == columnName)

    handler <- function(req, ...) {
      .defaultPostHandler(req, qns, tableName, tablePrefix, columnSpecs, ...)
    }

    callingParams <- list(
      pr = router,
      handler = handler,
      path = tablePath,
      serializer = plumber::serializer_json()
    )

    # Set handler call
    router <- do.call(plumber::pr_post, callingParams)
    return(router)
  })

  router <- plumber::pr_set_api_spec(router, function(spec) {
    spec$info$title <- apiTitle
    spec$info$version <- apiVersion

    for (tableName in unique(tableSpecification$tableName)) {
      tablePath <- paste0("/", tableName)
      columnSpecs <- tableSpecification %>% dplyr::filter(.data$tableName == tableName)
      requestSchema <- list()
      for (i in seq_len(nrow(columnSpecs))) {
        colDef <- columnSpecs[i, ]
        requestSchema[[colDef$columnName]] <- list(
          type = .getOpenApiSpecType(colDef$dataType),
          description = colDef$columnName
        )
      }
      # Setting the parameters exposed in the api
      spec$paths[[tablePath]]$post$requestBody$content[["application/json"]]$schema$properties <- requestSchema
    }
    return(spec)
  })

  invisible(router)
}
