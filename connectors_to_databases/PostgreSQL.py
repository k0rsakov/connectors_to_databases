# from .BaseOperator import BaseOperator
from connectors_to_databases.BaseOperator import BaseOperator
from urllib.parse import quote
from typing import Union, Iterable

from sqlalchemy import create_engine, engine

import pandas as pd

#TODO: add generate on conflict
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

    def insert_df(
            self,
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
            con=self._authorization_database(),
            chunksize=chunksize,
            index=index,
            if_exists=if_exists,
        )

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
            source_table_schema_name,
            source_table_name,
            target_table_schema_name,
            target_table_name,
            list_columns: Iterable,
            pk: Union[str, list],
            replace: bool = False,
    ):
        """"""

        if isinstance(pk, list):
            pk = cls.list_columns_in_str_with_double_quotes(list_columns=pk)
        else:
            pk = f'"{pk}"'
        #TODO: check if replace is False or True

        sql = f'''
        INSERT INTO {target_table_schema_name}.{target_table_name} 
        (
            {cls.list_columns_in_str_with_double_quotes(list_columns=list_columns)}
        )
        SELECT 
            {cls.list_columns_in_str_with_double_quotes(list_columns=list_columns)} 
        FROM 
            {source_table_schema_name}.{source_table_name}
        ON CONFLICT ({pk})
        '''

        print(sql)