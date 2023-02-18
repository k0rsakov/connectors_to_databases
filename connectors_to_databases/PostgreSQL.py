from .BaseOperator import BaseOperator

from urllib.parse import quote
from typing import Union

from sqlalchemy import create_engine, engine

import pandas as pd

class PostgreSQL(BaseOperator):
    """
    Connector to PostgreSQL database
    """
    def __init__(
            self,
            host: str = 'localhost',
            port: int = 5432,
            database: str = 'postgres',
            login: str = 'postgres',
            password: str = 'postgres'
    ):
        """
        :param host: Host/IP database; default 'localhost'.
        :param database: name database; default 'localhost'.
        :param port: port database; default 5432.
        :param login: login to database; default 'postgres'.
        :param password: password to database; default 'postgres'.
        """
        super().__init__(host, port, database, login, password)
        self._host = host
        self._database = database
        self._login = login
        self._password = password
        self._port = port

    def _authorization_pg(self) -> engine.base.Engine:
        """
        Creating connector engine to database PostgreSQL.
        """

        engine_str = f'postgresql://' \
                     f'{self._login}:{quote(self._password)}@{self._host}:{self._port}/' \
                     f'{self._database}'

        return create_engine(engine_str)

    def insert_df(self,
                      df: pd.DataFrame = None,
                      pg_table_name: str = None,
                      pg_table_schema: str = 'public',
                      chunksize: Union[int, None] = 10024,
                      index: bool = False,
                      if_exists:str = 'append',
                      ) -> Union[None, Exception]:
        """
        Inserting data from dataframe to database

        :param df: dataframe with data; default None.
        :param pg_table_name: name of table; default None.
        :param pg_table_schema: name of schema; default 'public'.
        :param chunksize: Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once.
        :param if_exists: {'fail', 'replace', 'append'}, default 'append'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.
        :param index:bool: Write DataFrame index as a column. Uses `index_label` as the column
            name in the table.
        """

        df.to_sql(
            name=pg_table_name,
            schema=pg_table_schema,
            con=self._authorization_pg(),
            chunksize=chunksize,
            index=index,
            if_exists=if_exists,
        )
