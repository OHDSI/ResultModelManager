% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/DataModel.R
\name{deleteAllRowsForDatabaseId}
\alias{deleteAllRowsForDatabaseId}
\title{Delete all rows for database id}
\usage{
deleteAllRowsForDatabaseId(
  connection,
  schema,
  tableName,
  databaseId,
  idIsInt = TRUE
)
}
\arguments{
\item{connection}{DatabaseConnector connection instance}

\item{schema}{The schema on the postgres server where the results table exists}

\item{tableName}{Database table name}

\item{databaseId}{Results source database identifier}

\item{idIsInt}{Identified is a numeric type? If not character is used}
}
\description{
Delete all rows for database id
}
\details{
Only PostgreSQL servers are supported.
}
