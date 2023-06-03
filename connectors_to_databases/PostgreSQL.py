# from .BaseOperator import BaseOperator
from connectors_to_databases.BaseOperator import BaseOperator
from urllib.parse import quote
from typing import Union, Iterable

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

    def _authorization_database(self) -> engine.base.Engine:
        """
        Creating connector engine to database PostgreSQL.
        """

        engine_str = f'postgresql://' \
                     f'{self._login}:{quote(self._password)}@{self._host}:{self._port}/' \
                     f'{self._database}'

        return create_engine(engine_str)

    # def insert_df(
    #         self,
    #         df: pd.DataFrame = None,
    #         pg_table_name: str = None,
    #         pg_table_schema: str = 'public',
    #         chunksize: Union[int, None] = 10024,
    #         index: bool = False,
    #         if_exists:str = 'append',
    # ) -> Union[None, Exception]:
    #     """
    #     Inserting data from dataframe to database
    #
    #     :param df: dataframe with data; default None.
    #     :param pg_table_name: name of table; default None.
    #     :param pg_table_schema: name of schema; default 'public'.
    #     :param chunksize: Specify the number of rows in each batch to be written at a time.
    #         By default, all rows will be written at once.
    #     :param if_exists: {'fail', 'replace', 'append'}, default 'append'
    #         How to behave if the table already exists.
    #
    #         * fail: Raise a ValueError.
    #         * replace: Drop the table before inserting new values.
    #         * append: Insert new values to the existing table.
    #     :param index:bool: Write DataFrame index as a column. Uses `index_label` as the column
    #         name in the table.
    #     """
    #
    #     df.to_sql(
    #         name=pg_table_name,
    #         schema=pg_table_schema,
    #         con=self._authorization_database(),
    #         chunksize=chunksize,
    #         index=index,
    #         if_exists=if_exists,
    #     )

    @staticmethod
    def list_columns_in_str_with_double_quotes(list_columns: list = None) -> str:
        """
        **Function: list_columns_in_str_with_double_quotes**

        This static function takes a list of columns as the `list_columns` parameter and returns a string where each
        column value is enclosed in double quotes.

        **Parameters:**
        - `list_columns` (list, optional): The list of columns to be enclosed in double quotes.
        If not specified, defaults to `None`.

        **Return:**
        - `str`: A string containing column values enclosed in double quotes and separated by commas.

        **Example Usage:**

        ```python
        columns = ['column1', 'column2', 'column3']
        result = MyClass.list_columns_in_str_with_double_quotes(columns)
        print(result)
        ```

        **Output:**

        ```
        "column1", "column2", "column3"
        ```

        In this example, we pass the `columns` list of columns to the `list_columns_in_str_with_double_quotes`
        function and store the result in the `result` variable. Then, we print the value of `result`, which will
        contain the strings from the `columns` list enclosed in double quotes and separated by commas.


        @param list_columns: The list of columns to be enclosed in double quotes. If not specified, defaults
            to `None`.; default 'None'
        @return: A string containing column values enclosed in double quotes and separated by commas.
        """

        return ', '.join([f"\"{value}\"" for value in list_columns])

    @classmethod
    def generate_on_conflict_sql_query(
            cls,
            source_table_schema_name: str = 'public',
            source_table_name: str = None,
            target_table_schema_name: str = 'public',
            target_table_name: str = None,
            list_columns: Iterable[str] = None,
            pk: Union[str, list] = 'id',
            replace: bool = False,
    ) -> str:
        """
        **Function: generate_on_conflict_sql_query**

        This class method generates an SQL query for performing data insertion with conflict handling using a specified primary key.

        **Parameters:**
        - `source_table_schema_name` (str, optional): The schema name of the source table. Defaults to `'public'`.
        - `source_table_name` (str): The name of the source table from which data will be inserted.
        - `target_table_schema_name` (str, optional): The schema name of the target table where data will be inserted.
            Defaults to `'public'`.
        - `target_table_name` (str): The name of the target table where data will be inserted.
        - `list_columns` (Iterable[str], optional): An iterable object containing the names of columns to be inserted.
            If not specified, all columns from the source table will be inserted.
        - `pk` (Union[str, list]): The primary key for checking insertion conflicts. It can be a string representing
            a single column name or a list of column names.
        - `replace` (bool): A flag indicating whether to replace existing data in case of conflicts. Defaults to
            `False`, which means conflicts will be ignored and nothing will be done.

        **Return:**
        - `str`: The generated SQL query for data insertion with conflict handling.

        **Example Usage:**

        ```python
        source_table_name = 'source_table'
        target_table_name = 'target_table'
        columns = ['column1', 'column2', 'column3']
        sql_query = MyClass.generate_on_conflict_sql_query(
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            list_columns=columns,
            pk='id',
            replace=True
        )
        print(sql_query)
        ```

        **Output:**

        ```
        INSERT INTO public.target_table
        (
            "column1", "column2", "column3"
        )
        SELECT
            "column1", "column2", "column3"
        FROM
            public.source_table
        ON CONFLICT ("id") DO UPDATE SET
            "column1" = EXCLUDED."column1",
            "column2" = EXCLUDED."column2",
            "column3" = EXCLUDED."column3"
        ```

        In this example, we pass the source table name (`source_table_name`), target table name (`target_table_name`),
        column list (`columns`), primary key (`pk`), and the `replace` flag to the `generate_on_conflict_sql_query`
        function. Then, we store the result in the `sql_query` variable and print it. The output will contain the
        generated SQL query for data insertion with conflict handling.

        :param source_table_schema_name: The schema name of the source table; default `'public'`.
        :param source_table_name: The name of the source table from which data will be inserted; default `None`.
        :param target_table_schema_name: The schema name of the target table where data will be
            inserted; default `'public'`.
        :param target_table_name: The name of the target table where data will be inserted; default `None`.
        :param list_columns: An iterable object containing the names of columns to be inserted; default `None`.
        :param pk: The primary key for checking insertion conflicts. It can be a string representing a single
        column name or a list of column names; default `'id'`.
        :param replace: A flag indicating whether to replace existing data in case of conflicts.
        Defaults to `False`, which means conflicts will be ignored and nothing will be done; default `False`.
        :return: The generated SQL query for data insertion with conflict handling.
        """

        if isinstance(pk, list):
            pk = cls.list_columns_in_str_with_double_quotes(list_columns=pk)
        else:
            pk = f'"{pk}"'

        if replace:
            replace = f'''DO UPDATE SET {', '.join([f'"{i}" = EXCLUDED."{i}"' for i in list_columns])}'''
        else:
            replace = f'DO NOTHING'


        sql = f'''
        INSERT INTO {target_table_schema_name}.{target_table_name} 
        (
            {cls.list_columns_in_str_with_double_quotes(list_columns=list_columns)}
        )
        SELECT 
            {cls.list_columns_in_str_with_double_quotes(list_columns=list_columns)} 
        FROM 
            {source_table_schema_name}.{source_table_name}
        ON CONFLICT ({pk}) {replace}
        '''

        return sql
