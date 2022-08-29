-- This example serves to show how multi-db platform changes can be Implemented
-- For example, altering a table on sqlite requires copying the table.
-- See equivalent file in inst/sql/sqlite/migrations
{DEFAULT @foo = foo}
ALTER TABLE @database_schema.@table_prefix@foo ALTER COLUMN foo FLOAT;
