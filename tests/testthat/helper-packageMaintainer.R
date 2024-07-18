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

# Format and check code

runPackageMaintenance <- function() {
  remotes::install_github("OHDSI/OhdsiRTools")
  packageName <- "ResultModelManager"
  devtools::document()
  OhdsiRTools::checkUsagePackage(packageName)
  OhdsiRTools::updateCopyrightYearFolder()
  styler::style_pkg()
  devtools::spell_check()

  # Create manual and vignettes:
  manualFile <- paste0("extras/", packageName, ".pdf")
  unlink(manualFile)
  system(paste0("R CMD Rd2pdf ./ --output=", manualFile))

  dir.create(path = "./inst/doc/", showWarnings = FALSE)


  vignetteFiles <- list.files("../../vignettes", pattern = "*.Rmd", full.names = TRUE)

  lapply(vignetteFiles, function(filename) {
    outputFile <- file.path("", "inst", "doc", paste0(gsub(".Rmd", ".pdf", basename(filename))))
    rmarkdown::render(filename,
      output_file = outputFile,
      rmarkdown::pdf_document(
        latex_engine = "pdflatex",
        toc = TRUE,
        number_sections = TRUE
      )
    )
  })

  pkgdown::build_site()
  OhdsiRTools::fixHadesLogo()
}
