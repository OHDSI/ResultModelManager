from typing import Dict, Any
import io

def upload_table(connection,
                 table: str,
                 filepath: str,
                 schema: str,
                 disable_constraints: bool = False) -> Dict[str, Any]:
    """
    utility function for pushing a csv file into a postgres db. Intended to be used in R but should work with and CSV
    delimited file that uses empty as NULL.
    :param connection: psycopg2 connection instance
    :param table: string table name
    :param filepath: path to csvfile
    :param schema: database schema of table
    :param disable_constraints: optionally switch of constraints prior to insert and switch back on after
    :return:
    """
    with connection.cursor() as curr:
        if disable_constraints:
            try:
                curr.execute(f"ALTER TABLE {schema}.{table} DISABLE TRIGGER ALL;")
                connection.commit()
            except Exception as e:
                return dict(status=-1, message=f"{e}")

        # Taken from database connector
        copy_string = f"COPY {schema}.{table} FROM STDIN NULL AS '' DELIMITER ',' CSV HEADER;"
        try:
            with open(filepath, "rb") as uf:
                # Copy file from STDIN - should only use buffer size memory
                curr.copy_expert(copy_string, uf)
            if disable_constraints:
                curr.execute(f"ALTER TABLE {schema}.{table} ENABLE TRIGGER ALL;")
            connection.commit()
            status = dict(status=1, message="upload success")
        except Exception as e:
            status = dict(status=-1, message=f"{e}")
            connection.rollback()

    return status


def upload_buffer_to_db(connection, csv_content, schema: str, table: str):
    # Create a StringIO buffer from the CSV content
    copy_string = f"COPY {schema}.{table} FROM STDIN NULL AS '' DELIMITER ',' CSV HEADER;"
    # Upload the CSV data to the database table
    with connection.cursor() as curr:
        # Use the COPY command
        with io.StringIO(csv_content) as buffer:
            curr.copy_expert(copy_string, buffer)

    # Commit the transaction
    connection.commit()
    status = dict(status=1, message="upload success")
    return status
