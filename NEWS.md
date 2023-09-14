# ResultModelManager 0.5.1

Bug fixes:

1. Fixed issue with uploads results from removal of `emptyIsNa` property which is no longer used/required

# ResultModelManager 0.5.0
Changes:
1. Added utility function `grantTablePermissions` to make it easier to grant select, delete, insert and update
permissions for users on results database setups

2. Added `ResultExportManager` class and utility to support standardized validation routines for exported data

3. Allow packages to have an internal migration table prefix separate from the user defined one e.g `my_study_sccs_migration`

4. `emptyIsNa` field is no longer required in specifications

5. Purge all data supported in upload functionality (requiring user input)

Bug fixes:

1.  `generateSqlSchema` No longer requires primary key field to be lower case in results spec files, is now case-insensitive

2. connectionHandlers now check sql string attribute to see if query needs translating, avoiding potential for errors
caused by double translation

# ResultModelManager 0.4.0
Changes:

1. removed spuriously added `createResultsDataModel` function

2. added `QueryNamespace`  R6 class that allows users to define a table specification and then complete queries without
always having to specify table names

3. Added `createQueryNamespace` helper function to allow a variety of convenient ways to create query namespaces

4. Added `loadResultsDataModelSpecifications` that loads results specs from a csv file and checks that the columns are
correct

5. Added some vignettes on usage of package features

# ResultModelManager 0.3.0
Changes:

1. Added support for validating results model schema data.frames

2. Added function to generate sql for creating schemas from specification data frames

3. Added unzip folder function

4. Added robust upload function for directories of csv files conforming to model specifications

5. Added vignette to describe creating results schemas and uploading data sets


# ResultModelManager 0.2.0
Changes:

1. Fixed support for newer versions of DatabaseConnector that have altered interface - this will mean this package
now depends on DatabaseConnector version 6.0.0 and above. However, `queryDb` compatibility means that no code
that uses these classes will need to be altered.

2. Added support for `ConnectionHandler` to load tables with `dbplyr/dplyr` `tbl` interface (i.e. lazy loading).
This means that `dplyr` style queries should be supported natively in Pooled and non-pooled connections.

# ResultModelManager 0.1.1

Changes:
1. Added snakeCaseToCamelCase parameter to public in connectionHandlers so it can be defined once if required

2. Added schema generator function that creates sql from csv files with table defs

# ResultModelManager 0.1.0

Initial version