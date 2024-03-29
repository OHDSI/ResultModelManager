% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/DataMigrationManager.R
\name{DataMigrationManager}
\alias{DataMigrationManager}
\title{DataMigrationManager (DMM)}
\value{
data frame all migrations, including file name, order and execution status
Get connection handler
}
\description{
R6 class for management of database migration
}
\seealso{
\link{ConnectionHandler} for information on returned class
}
\section{Public fields}{
\if{html}{\out{<div class="r6-fields">}}
\describe{
\item{\code{migrationPath}}{Path migrations exist in}

\item{\code{databaseSchema}}{Path migrations exist in}

\item{\code{packageName}}{packageName, can be null}

\item{\code{tablePrefix}}{tablePrefix, can be empty character vector}

\item{\code{packageTablePrefix}}{packageTablePrefix, can be empty character vector}
}
\if{html}{\out{</div>}}
}
\section{Methods}{
\subsection{Public methods}{
\itemize{
\item \href{#method-DataMigrationManager-new}{\code{DataMigrationManager$new()}}
\item \href{#method-DataMigrationManager-migrationTableExists}{\code{DataMigrationManager$migrationTableExists()}}
\item \href{#method-DataMigrationManager-getMigrationsPath}{\code{DataMigrationManager$getMigrationsPath()}}
\item \href{#method-DataMigrationManager-getStatus}{\code{DataMigrationManager$getStatus()}}
\item \href{#method-DataMigrationManager-getConnectionHandler}{\code{DataMigrationManager$getConnectionHandler()}}
\item \href{#method-DataMigrationManager-check}{\code{DataMigrationManager$check()}}
\item \href{#method-DataMigrationManager-executeMigrations}{\code{DataMigrationManager$executeMigrations()}}
\item \href{#method-DataMigrationManager-isPackage}{\code{DataMigrationManager$isPackage()}}
\item \href{#method-DataMigrationManager-finalize}{\code{DataMigrationManager$finalize()}}
\item \href{#method-DataMigrationManager-clone}{\code{DataMigrationManager$clone()}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-new"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-new}{}}}
\subsection{Method \code{new()}}{
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$new(
  connectionDetails,
  databaseSchema,
  tablePrefix = "",
  packageTablePrefix = "",
  migrationPath,
  packageName = NULL,
  migrationRegexp = .defaultMigrationRegexp
)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{connectionDetails}}{DatabaseConnector connection details object}

\item{\code{databaseSchema}}{Database Schema to execute on}

\item{\code{tablePrefix}}{Optional table prefix for all tables (e.g. plp, cm, cd etc)}

\item{\code{packageTablePrefix}}{A table prefix when used in conjunction with other package results schema,
e.g. "cd_", "sccs_", "plp_", "cm_"}

\item{\code{migrationPath}}{Path to location of migration sql files. If in package mode, this should just
be a folder (e.g. "migrations") that lives in the location "sql/sql_server" (and)
other database platforms.
If in folder model, the folder must include "sql_server" in the relative path,
(e.g if  migrationPath = 'migrations' then the folder 'migrations/sql_server' should exists)}

\item{\code{packageName}}{If in package mode, the name of the R package}

\item{\code{migrationRegexp}}{(Optional) regular expression pattern default is \verb{(Migration_([0-9]+))-(.+).sql}
Migration table exists}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-migrationTableExists"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-migrationTableExists}{}}}
\subsection{Method \code{migrationTableExists()}}{
Check if migration table is present in schema
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$migrationTableExists()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
boolean
Get path of migrations
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-getMigrationsPath"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-getMigrationsPath}{}}}
\subsection{Method \code{getMigrationsPath()}}{
Get path to sql migration files
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$getMigrationsPath(dbms = "sql server")}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{dbms}}{Optionally specify the dbms that the migration fits under
Get status of result model}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-getStatus"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-getStatus}{}}}
\subsection{Method \code{getStatus()}}{
Get status of all migrations (executed or not)
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$getStatus()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-getConnectionHandler"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-getConnectionHandler}{}}}
\subsection{Method \code{getConnectionHandler()}}{
Return connection handler instance
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$getConnectionHandler()}\if{html}{\out{</div>}}
}

\subsection{Returns}{
ConnectionHandler instance
Check migrations in folder
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-check"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-check}{}}}
\subsection{Method \code{check()}}{
Check if file names are valid for migrations
Execute Migrations
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$check()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-executeMigrations"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-executeMigrations}{}}}
\subsection{Method \code{executeMigrations()}}{
Execute any unexecuted migrations
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$executeMigrations(stopMigrationVersion = NULL)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{stopMigrationVersion}}{(Optional) Migrate to a specific migration number
isPackage}
}
\if{html}{\out{</div>}}
}
}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-isPackage"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-isPackage}{}}}
\subsection{Method \code{isPackage()}}{
is a package folder structure or not
finalize
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$isPackage()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-finalize"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-finalize}{}}}
\subsection{Method \code{finalize()}}{
close database connection
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$finalize()}\if{html}{\out{</div>}}
}

}
\if{html}{\out{<hr>}}
\if{html}{\out{<a id="method-DataMigrationManager-clone"></a>}}
\if{latex}{\out{\hypertarget{method-DataMigrationManager-clone}{}}}
\subsection{Method \code{clone()}}{
The objects of this class are cloneable with this method.
\subsection{Usage}{
\if{html}{\out{<div class="r">}}\preformatted{DataMigrationManager$clone(deep = FALSE)}\if{html}{\out{</div>}}
}

\subsection{Arguments}{
\if{html}{\out{<div class="arguments">}}
\describe{
\item{\code{deep}}{Whether to make a deep clone.}
}
\if{html}{\out{</div>}}
}
}
}
