"""
PostgresORM
"""
import logging
import os
import warnings

import psycopg2

from .utils.config import load_yaml


def _get_logger() -> logging.Logger:
    logging.basicConfig(
        level=os.environ.get("LOGLEVEL", "INFO"),
        format="[%(filename)s:%(lineno)s - %(funcName)s()] %(asctime)-15s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(__name__)


class PostgresORM:
    """
    This is a simple demonstration using psycopg2
    to produce a set of simple Postgresql operations
    that can be used in the AWS environment using
    Python.
    """

    def __init__(self, secrets: dict, config_filepath: str, **kwargs):
        self.logger = _get_logger()
        self.config = load_yaml(filepath=config_filepath)
        self._secrets = secrets
        self._connection = self._get_connection()
        self.kwargs = kwargs

    def _get_connection(self) -> psycopg2.connect:
        """
        Gets the psycopg2 client connection object.
        :return: psycopg2.connect
        """
        return psycopg2.connect(
            database=self._secrets.get("database"),
            user=self._secrets.get("user"),
            password=self._secrets.get("password"),
            host=self._secrets.get("host"),
            port=self._secrets.get("port"),
        )

    def _create_table(self) -> str:
        """
        Generates CREATE TABLE DDL from payload.
        """
        for table, constraints in self.config.get("create_table", {}).items():
            schema_name = table.split(".")[0]
            table_name = table.split(".")[1]
            constraints_length, count = len(constraints), 1
            sql_statement_constituents = [
                f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ("
            ]
            for column_name, column_constraint in constraints.items():
                if constraints_length == count:
                    sql_statement_constituents.append(
                        f"{column_name} {column_constraint});"
                    )
                else:
                    sql_statement_constituents.append(
                        f"{column_name} {column_constraint},"
                    )
                count += 1
            yield "".join(sql_statement_constituents)

    def _drop_table(self) -> str:
        """
        Generates DROP TABLE from payload.
        """
        for table_name in self.config.get("drop_table", []):
            yield f"DROP TABLE {table_name};"

    def _insert_data(self) -> str:
        """
        Generates CREATE TABLE DDL from payload.
        """
        for table, constraints in self.config.get("insert_data", {}).items():
            schema_name = table.split(".")[0]
            table_name = table.split(".")[1]
            columns = constraints.get("columns")
            data = constraints.get("data")

            # check if table exists first
            if self._check_table(schema=schema_name, table=table_name):
                for row in data:
                    sql_statement_constituents = [
                        f"INSERT INTO {schema_name}.{table_name} ({', '.join(columns)}) VALUES ("
                    ]
                    row_length, count = len(row), 1
                    for value in row:
                        if isinstance(value, str):
                            value = f"'{value}'"
                        if row_length == count:
                            sql_statement_constituents.append(f"{value});")
                        else:
                            sql_statement_constituents.append(f"{value},")
                        count += 1
                    yield "".join(sql_statement_constituents)
            else:
                warnings.warn(f"The table {schema_name}.{table_name} does not exist.")

    def _check_table(self, schema: str, table: str) -> bool:
        """
        Check if table exists.
        """
        cursor = self._connection.cursor()
        cursor.execute(
            f"""
            SELECT EXISTS (
               SELECT FROM information_schema.tables 
               WHERE  table_schema = '{schema}'
               AND    table_name   = '{table}'
               );
            """
        )
        return cursor.fetchall()[0][0]

    def run(self):
        try:
            cursor = self._connection.cursor()
            _sql_funcs = [self._create_table, self._insert_data, self._drop_table]
            for _func in _sql_funcs:
                for query in _func():
                    self.logger.info(query)
                    cursor.execute(query)
            self._connection.commit()
            cursor.close()
        except (Exception, psycopg2.DatabaseError) as err:
            raise err
        finally:
            self.logger.info("Execution complete!")
            self._connection.close()
