from collections.abc import Sequence

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver import Client
from sqlalchemy import engine

from connectors_to_databases.BaseOperator import BaseOperator
from connectors_to_databases.TypeHinting import SQLQuery


class ClickHouse(BaseOperator):
    """Connector to ClickHouse database."""

    def __init__(
            self,
            host: str = "localhost",
            port: int = 8123,
            login: str = "default",
            password: str = "default",  # noqa: S107
    ):
        """
        Init class.

        :param host: Host/IP database; default 'localhost'.
        :param port: port database; default '8123'.
        :param login: login to database; default 'default'.
        :param password: password to database; default 'default'.
        """
        super().__init__(host, port, login, password)
        self._host = host
        self._login = login
        self._password = password
        self._port = port

    def _authorization_database(self) -> clickhouse_connect.driver.httpclient.Client:
        """Creating connector engine to database ClickHouse."""  # noqa D401

        return clickhouse_connect.get_client(
            host=self._host,
            port=self._port,
            username=self._login,
            password=self._password,
        )

    def execute_script(
            self,
            manual_sql_script: SQLQuery = None,
    ) -> None:
        """
        Execute manual scripts (INSERT, TRUNCATE, DROP, CREATE, etc.). Other than SELECT.

        :param manual_sql_script: query with manual script; default `''`.
        :return: None.
        """

        ch = self._authorization_database()

        ch.query(query=manual_sql_script)

    def execute_to_df(
            self,
            sql_query: SQLQuery = None,
    ) -> pd.DataFrame | Exception:
        """
        Getting data from database with SQL-query.

        :param sql_query; default `''`.
        :return: DataFrame with data from database.
        """  # noqa D415

        ch = self._authorization_database()

        return ch.query_df(
            query=sql_query,
        )

    def insert_df(
            self,
            df: pd.DataFrame = None,
            table_name: str = None,
            table_schema: str = None,
            dtype: Sequence[str] | None = None,
            **kwargs,
    ) -> None:
        """
        Inserting data from dataframe to database.

        Does not create a table if not exists.

        :param df: dataframe with data; default None.
        :param table_name: name of table; default None.
        :param table_schema: name of schema; default None.
        :param dtype: ClickHouse column type names; default None.
        :return: None,
        """  # noqa: D401

        ch = self._authorization_database()

        ch.insert_df(
            df=df,
            table=table_name,
            database=table_schema,
            column_type_names=dtype,
        )

    def get_uri(self) -> Client:
        """
        Get connector for manual manipulation with connect to database.

        :return engine.base.Engine:
        """

        return self._authorization_database()
