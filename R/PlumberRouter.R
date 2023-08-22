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


#' Create a router for a plumber API
#' @description
#' create a plumber router for each table within a given table space
#'
#' @returns plumber::pr router (invisibly)
#'
#' @export
#' @param ...       @seealso createQueryNamespace
#' @inheritParams createQueryNamespace
#' @param plumberEnv    (Optional) environment for access to plumber environment
createPlumberRouter <- function(tableSpecification, plumberEnv = new.env(), ...) {
  checkmate::assertEnvironment(plumberEnv)
  plumberEnv$qns <- createQueryNamespace(tableSpecification = tableSpecification, ...)
  router <- plumber::pr()

  lapply(unique(tableSpecification$tableName), function(tableName) {
    plumber::pr_get(router, paste0("/", tableName), function (req, res) {
      plumberEnv$qns$queryDb()
    })
  })

  invisible(router)
}