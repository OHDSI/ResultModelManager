ResultModelManager
==================
[![Build Status](https://github.com/OHDSI/ResultModelManager/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/ResultModelManager/actions?query=workflow%3AR-CMD-check)
[![codecov.io](https://codecov.io/github/OHDSI/ResultModelManager/coverage.svg?branch=main)](https://app.codecov.io/github/OHDSI/ResultModelManager?branch=main)
[![CRAN_Status_Badge](https://www.r-pkg.org/badges/version/ResultModelManager)](https://cran.r-project.org/package=ResultModelManager)
[![CRAN_Status_Badge](https://cranlogs.r-pkg.org/badges/ResultModelManager)](https://cran.r-project.org/package=ResultModelManager)


ResultModelManager (RMM) [HADES](https://ohdsi.github.io/Hades/).

Introduction
============
RMM is a database data model management utilities for R packages in the [Observational Health Data Sciences and Informatics program](https://ohdsi.org). RMM provides utility functions to
allow package maintainers to migrate existing SQL database models, export and import results in consistent patterns.


System Requirements
===================

Requires R. Some of the packages used by ResultModelManager require Java.

Installation
=============

1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including Java.

2. In R, use the following commands to download and install ResultModelManager:

```r
install.packages("ResultModelManager")
```

or, to install the development version:

```r
remotes::install_github("ResultModelManager", ref = 'develop')
```

Usage
=====

See articles:
- [Creating migrations](https://ohdsi.github.io/ResultModelManager/articles/CreatingMigrations.html)
- [Example Project](https://ohdsi.github.io/ResultModelManager/articles/ExampleProject.html)
- [Upload functionality](https://ohdsi.github.io/ResultModelManager/articles/UploadFunctionality.html)
- [Connection handler](https://ohdsi.github.io/ResultModelManager/articles/UsingConnectionHandlers.html)
- [Using query namespaces](https://ohdsi.github.io/ResultModelManager/articles/UsingQueryNamespaces.html)


Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/ResultModelManager/issues">GitHub issue tracker</a> for all bugs/issues/enhancements

Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
ResultModelManager is licensed under Apache License 2.0

Development
===========
ResultModelManager is being developed in R Studio.

Development status
==================

Initial release - use with care
