% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/DataModel.R
\name{uploadResults}
\alias{uploadResults}
\title{Upload results to the database server.}
\usage{
uploadResults(
  connection = NULL,
  connectionDetails = NULL,
  schema,
  resultsFolder,
  tablePrefix = "",
  forceOverWriteOfSpecifications = FALSE,
  purgeSiteDataBeforeUploading = TRUE,
  databaseIdentifierFile = "cdm_source_info.csv",
  runCheckAndFixCommands = FALSE,
  warnOnMissingTable = TRUE,
  purgeDataModel = FALSE,
  specifications
)
}
\arguments{
\item{connection}{An object of type \code{connection} as created using the
\code{\link[DatabaseConnector]{connect}} function in the
DatabaseConnector package. Can be left NULL if \code{connectionDetails}
is provided, in which case a new connection will be opened at the start
of the function, and closed when the function finishes.}

\item{connectionDetails}{An object of type \code{connectionDetails} as created using the
\code{\link[DatabaseConnector]{createConnectionDetails}} function in the
DatabaseConnector package.}

\item{schema}{The schema on the postgres server where the tables have been created.}

\item{resultsFolder}{The path to the folder containing the results to upload.
See \code{unzipResults} for more information.}

\item{tablePrefix}{String to prefix table names with - default is empty string}

\item{forceOverWriteOfSpecifications}{If TRUE, specifications of the phenotypes, cohort definitions, and analysis
will be overwritten if they already exist on the database. Only use this if these specifications
have changed since the last upload.}

\item{purgeSiteDataBeforeUploading}{If TRUE, before inserting data for a specific databaseId all the data for
that site will be dropped. This assumes the results folder contains the full data for that
data site.}

\item{databaseIdentifierFile}{File contained that references databaseId field (used when purgeSiteDataBeforeUploading == TRUE). You may
specify a relative path for the cdmSourceFile and the function will assume it resides in the resultsFolder.
Alternatively, you can provide a path outside of the resultsFolder for this file.}

\item{runCheckAndFixCommands}{If TRUE, the upload code will attempt to fix column names, data types and
duplicate rows. This parameter is kept for legacy reasons - it is strongly recommended
that you correct errors in your results where those results are assembled instead of
relying on this option to try and fix it during upload.}

\item{warnOnMissingTable}{Boolean, print a warning if a table file is missing.}

\item{purgeDataModel}{This function will purge all data from the tables in the specification prior to upload.
Use with care. If interactive this will require further input.}

\item{specifications}{A tibble data frame object with specifications.}
}
\description{
Requires the results data model tables have been created using following the specifications, generateSqlSchema function.

Results files should be in the snake_case format for table headers and not camelCase

Set the POSTGRES_PATH environmental variable to the path to the folder containing the psql executable to enable
bulk upload (recommended).
}
