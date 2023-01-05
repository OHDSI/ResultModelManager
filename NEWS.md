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