--The follow is intended to be an example sql migration only
{DEFAULT @foo = foo}

CREATE TABLE @database_schema.package_version(
    version varchar
);

INSERT INTO @database_schema.package_version (version) VALUES ('0.0.1');

CREATE TABLE @database_schema.@table_prefix@foo (
    id bigint,
    foo INT
);
