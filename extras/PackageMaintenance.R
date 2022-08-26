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

# Format and check code
OhdsiRTools::checkUsagePackage("ResultModelManager")
OhdsiRTools::updateCopyrightYearFolder()
styler::style_pkg()
devtools::spell_check()

# Create manual and vignettes:
unlink("extras/CohortDiagnostics.pdf")
system("R CMD Rd2pdf ./ --output=extras/CohortDiagnostics.pdf")

dir.create(path = "./inst/doc/", showWarnings = FALSE)


rmarkdown::render("vignettes/creating-migrations.Rmd",
                  output_file = "../inst/doc/.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

rmarkdown::render("vignettes/design-specifications.Rmd",
                  output_file = "../inst/doc/.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

pkgdown::build_site()
OhdsiRTools::fixHadesLogo()