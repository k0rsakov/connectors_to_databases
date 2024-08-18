from abc import ABC, abstractmethod
from urllib.parse import quote

import pandas as pd
from sqlalchemy import create_engine, engine

from .TypeHinting import SQLQuery


class BaseOperator(ABC):
    """BaseOperator for databases."""

    def __init__(
            self,
            host: str = "localhost",
            port: int = None,
            database: str = None,
            login: str = None,
            password: str = None,
    ):
        """
        Init class.

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

    @abstractmethod
    def _authorization_database(self) -> engine.base.Engine:
        """
        Creating connector engine to some database.

        :return: engine for database.
        """ # noqa D415

        engine_str = f"base://" \
                     f"{self._login}:{quote(self._password)}@{self._host}:{self._port}/" \
                     f"{self._database}"

        return create_engine(engine_str)

    def insert_df(
            self,
            df: pd.DataFrame = None,
            table_name: str = None,
            table_schema: str = None,
            chunksize: int | None = 10024,
            index: bool = False,
            if_exists: str = "append",
            dtype: None | dict = None,
    ) -> None:
        """
        Inserting data from dataframe to database.
         
        :param df: dataframe with data; default None.
        :param table_name: name of table; default None.
        :param table_schema: name of schema; default None.
        :param chunksize: Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once; default `10024`.
        :param index: Write DataFrame index as a column. Uses `index_label` as the column
            name in the table.
        :param if_exists: {'fail', 'replace', 'append'}, default 'append'
            How to behave if the table already exists.

            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.
        :param dtype: Specifying the datatype for columns. If a dictionary is used, the
            keys should be the column names and the values should be the
            SQLAlchemy types or strings for the sqlite3 legacy mode. If a
            scalar is provided, it will be applied to all columns.
            
        Example:
        -------
            Create df
            >>> from connectors_to_databases import PostgreSQL
            >>> import sqlalchemy
            >>> from sqlalchemy.dialects.postgresql import UUID
            
            >>> pg = PostgreSQL()
            >>> dict_ = {'id': '41e5091e-6e97-4670-a4c9-7d6d4cc7c2af', 'date': '2020-01-01', 'amount': 100}
            >>> df_foo = pd.DataFrame([dict_]) # noqa: PD901
            >>> pg.insert_df(
            ...    df=df_foo, 
            ...    table_name='tmp_fct_sales', 
            ...    table_schema='public',
            ...    dtype={
            ...        'id': UUID,
            ...        'date': sqlalchemy.Date
            ...    }
            ... )

        """ # noqa D415

        df.to_sql(
            name=table_name,
            schema=table_schema,
            con=self._authorization_database(),
            chunksize=chunksize,
            index=index,
            if_exists=if_exists,
            dtype=dtype,
        )

    def execute_to_df(
            self,
            sql_query: SQLQuery = None,
    ) -> pd.DataFrame | Exception:
        """
        Getting data from database with SQL-query.

        :param sql_query; default `''`.
        :return: DataFrame with data from database.
        """ # noqa D415

        return pd.read_sql(
            sql=sql_query,
            con=self._authorization_database(),
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
        self._authorization_database().execute(manual_sql_script)

    def get_uri(self) -> engine.base.Engine:
        """
        Get connector for manual manipulation with connect to database.

        :return engine.base.Engine:
        """

        return self._authorization_database()

    def check_connection_to_database(self) -> bool | Exception:
        """
        Method to check connection to database.

        :return: boolean True, if connection to database is successful, Exception otherwise.
        """ # noqa D415
        df = self.execute_to_df("SELECT 1 AS ONE")  # noqa: PD901

        return bool(isinstance(df, pd.DataFrame))
