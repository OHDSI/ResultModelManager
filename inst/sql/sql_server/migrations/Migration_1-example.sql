/*
The follow is intended to be an example sql migration only
*/
{DEFAULT @foo = foo}

CREATE TABLE @database_schema.@table_prefix@foo (
    id bigint,
    foo INT
);
