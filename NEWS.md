# ResultModelManager 0.3.1
Changes:

1. removed spuriously added `createResultsDataModel` function

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