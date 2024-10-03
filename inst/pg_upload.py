from typing import Dict, Any


def upload_table(connection, table: str,
                 filepath: str,
                 schema: str,
                 disable_constraints: bool = False) -> Dict[str, Any]:
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
