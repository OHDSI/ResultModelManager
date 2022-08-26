{DEFAULT @foo = foo}

ALTER TABLE @database_schema.@table_prefix@foo RENAME TO _foo_old;

CREATE TABLE @database_schema.@table_prefix@foo (
    id bigint,
    foo float
);

INSERT INTO @database_schema.@table_prefix@foo (id, foo)
SELECT * FROM _foo_old;
