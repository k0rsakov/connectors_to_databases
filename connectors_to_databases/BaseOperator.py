from urllib.parse import quote
from typing import Union

from sqlalchemy import create_engine, engine

import pandas as pd

from .TypeHinting import SQLQuery


class BaseOperator:
    """
    BaseOperator for databases
    """
    def __init__(self,
                 host: str = 'localhost',
                 port: int = None,
                 database: str = None,
                 login: str = None,
                 password: str = None,
                 ):
        """
        :param host: Host/IP database; default 'localhost'.
        :param database: name database; default 'None'.
        :param port: port database; default 'None'.
        :param login: login to database; default 'None'.
        :param password: password to database; default 'None'.
        """
        self._host = host
        self._database = database
        self._login = login
        self._password = password
        self._port = port

    def _authorization_database(self) -> engine.base.Engine:
        """
        Creating connector engine to database PostgreSQL.
        """

        engine_str = f'base://' \
                     f'{self._login}:{quote(self._password)}@{self._host}:{self._port}/' \
                     f'{self._database}'

        return create_engine(engine_str)

    def insert_df(
            self,
            df: pd.DataFrame = None,
            table_name: str = None,
            table_schema: str = None,
            chunksize: Union[int, None] = 10024,
            index: bool = False,
            if_exists: str = 'append',
    ) -> Union[None, Exception]:
        """
        Inserting data from dataframe to database.

        :param df: dataframe with data; default None.
        :param table_name: name of table; default None.
        :param table_schema: name of schema; default None.
        :param chunksize: Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once; default `10024`.
        :param if_exists: {'fail', 'replace', 'append'}, default 'append'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.
        :param index: Write DataFrame index as a column. Uses `index_label` as the column
            name in the table.
        """

        df.to_sql(
            name=table_name,
            schema=table_schema,
            con=self._authorization_database(),
            chunksize=chunksize,
            index=index,
            if_exists=if_exists,
        )

    def execute_to_df(
            self,
            sql_query: str = SQLQuery,
    ) -> Union[pd.DataFrame, Exception]:
        """
        Getting data from database with SQL-query.

        :param sql_query; default `''`.
        :return: DataFrame with data from database.
        """

        return pd.read_sql(
            sql=sql_query,
            con=self._authorization_database(),
        )

    def execute_script(
            self,
            manual_sql_script: SQLQuery
    ) -> None:
        """
        Execute manual scripts (INSERT, TRUNCATE, DROP, CREATE, etc). Other than SELECT

        :param manual_sql_script: query with manual script; default `''`.
        :return: None.
        """
        self._authorization_database().execute(manual_sql_script)

    def get_uri(self) -> engine.base.Engine:
        """
        Get connector for manual manipulation with connect to database.

        :return engine.base.Engine:
        """

        return self._authorization_database()

    def check_connection_to_database(self) -> Union[bool, Exception]:
        """
        Method to check connection to database.

        :return: boolean True, if connection to database is successful, Exception otherwise.
        """
        df = self.execute_to_df('SELECT 1 AS ONE')

        return bool(isinstance(df, pd.DataFrame))
