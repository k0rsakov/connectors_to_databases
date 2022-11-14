from typing import Union

from sqlalchemy import create_engine
import pandas as pd

class PostgreSQL:
    """
    Connector to PostgreSQL database
    """
    def __init__(self,
                 host: str = 'localhost',
                 database: str = 'postgres',
                 login: str = 'postgres',
                 password: str = 'postgres',
                 port: int = 5432
                 ):
        """
        :param host:str: Host/IP database; default 'localhost'.
        :param database:str: name database; default 'localhost'.
        :param port:int: port database; default 5432.
        :param login:str: login to database; default 'postgres'.
        :param password:str: password to database; default 'postgres'.
        """
        self.host = host
        self.db = database
        self.login = login
        self.password = password
        self.port = port

    def authorization_pg(self):
        """
        Creating connector engine to database postgresql.
        """

        engine_str = f'postgresql://{self.login}:{self.password}@{self.host}:{self.port}/{self.db}'
        engine = create_engine(engine_str)

        return engine

    def into_pg_table(self,
                      df: pd.DataFrame = None,
                      pg_table_name: str = None,
                      pg_table_schema: str = 'public',
                      ) -> Union[int, None, Exception]:
        """
        Inserting data from dataframe to database

        :param df:pd.DataFrame: dataframe with data; default None
        :param pg_table_name:str: name of table; default None.
        :param pg_table_schema: name of schema; default 'public'.
        :return:
        """

        try:
            connector = self.authorization_pg()
            df.to_sql(
                name=pg_table_name,
                schema=pg_table_schema,
                con=connector,
                chunksize=10024,
                index=False,
                if_exists='append'
            )
        except Exception as ex:
            print(f"Can't insert df in {pg_table_name}")
            raise ex

    def execute_to_df(
            self,
            sql_query: str = '',
    ) -> Union[pd.DataFrame, Exception]:
        """
        Getting data from database with SQL-query.

        :param sql_query:str: SQL-query; default ''.
        :return:pd.DataFrame: dataframe with data from database
        """

        try:
            return pd.read_sql(sql_query, self.authorization_pg())
        except Exception as ex:
            print(f"Can't execute df from PostgreSQL")
            raise ex