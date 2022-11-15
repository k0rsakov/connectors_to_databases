from typing import Union

from sqlalchemy import create_engine, engine

import pandas as pd

from TypeHinting import SQLQuery

class PostgreSQL:
    """
    Connector to PostgreSQL database
    """
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 5432,
                 database: str = 'postgres',
                 login: str = 'postgres',
                 password: str = 'postgres',
                 ):
        """
        :param host:str: Host/IP database; default 'localhost'.
        :param database:str: name database; default 'localhost'.
        :param port:int: port database; default 5432.
        :param login:str: login to database; default 'postgres'.
        :param password:str: password to database; default 'postgres'.
        """
        self.__host__ = host
        self.__database__ = database
        self.__login__ = login
        self.__password__ = password
        self.__port__ = port

    def __authorization_pg__(self) -> engine.base.Engine:
        """
        Creating connector engine to database PostgreSQL.
        """

        engine_str = f'postgresql://' \
                     f'{self.__login__}:{self.__password__}@{self.__host__}:{self.__port__}/{self.__database__}'
        engine_connector = create_engine(engine_str)

        return engine_connector

    def into_pg_table(self,
                      df: pd.DataFrame = None,
                      pg_table_name: str = None,
                      pg_table_schema: str = 'public',
                      chunksize: Union[int, None] = 10024,
                      index: bool = False,
                      if_exists:str = 'append',
                      ) -> Union[None, Exception]:
        """
        Inserting data from dataframe to database

        :param df:pd.DataFrame: dataframe with data; default None.
        :param pg_table_name:str: name of table; default None.
        :param pg_table_schema: name of schema; default 'public'.
        :param chunksize:int: Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once.
        :param if_exists:str: {'fail', 'replace', 'append'}, default 'append'
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
            con=self.__authorization_pg__(),
            chunksize=chunksize,
            index=index,
            if_exists=if_exists
        )

    def execute_to_df(
            self,
            sql_query: str = SQLQuery,
    ) -> Union[pd.DataFrame, Exception]:
        """
        Getting data from database with SQL-query.

        :param sql_query:str: SQL-query; default ''.
        :return:pd.DataFrame: dataframe with data from database
        """

        return pd.read_sql(sql_query, self.__authorization_pg__())

    def execute_script(self,
                       manual_sql_script: SQLQuery):
        """
        Execute manual scripts (INSERT, TRUNCATE, DROP, CREATE, etc). Other than SELECT

        :param manual_sql_script:SQLQuery: `str` query with manual script
        :return:
        """
        self.__authorization_pg__().execute(manual_sql_script)

    def get_uri(self) -> engine.base.Engine:
        """
        Get connector for manual manipulation with connect to database

        :return engine.base.Engine:
        """

        return self.__authorization_pg__()
