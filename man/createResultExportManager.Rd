% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ResultExportManager.R
\name{createResultExportManager}
\alias{createResultExportManager}
\title{Create Result Export Manager}
\usage{
createResultExportManager(
  tableSpecification,
  exportDir,
  minCellCount = getOption("ohdsi.minCellCount", default = 5),
  databaseId = NULL
)
}
\arguments{
\item{tableSpecification}{Table specification data.frame}

\item{exportDir}{Directory files are being exported to}

\item{minCellCount}{Minimum cell count - reccomended that you set with
options("ohdsi.minCellCount" = count) in all R projects. Default is 5}

\item{databaseId}{database identifier - required when exporting according to many specs}
}
\description{
For a give table specification file, create an export manager instance for creating results data sets that conform
to the data model.

This checks that, at export time, internal validity is assured for the data (e.g. primary keys are valid, data types
are compatible

In addition this utility will create a manifest object that can be used to maintain the validity of data.

If an instance of a DataMigrationManager is present and available a packageVersion reference (where applicable)
and migration set will be referenced. Allowing data to be imported into a database schema at a specific version.
}
