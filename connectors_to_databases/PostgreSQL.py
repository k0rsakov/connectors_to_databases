from typing import Iterable

from urllib.parse import quote
import pandas as pd
from sqlalchemy import create_engine, engine

from connectors_to_databases.BaseOperator import BaseOperator

from helper_functions.function_list import list_values_in_str_with_double_quotes, list_values_in_str_with_single_quotes

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

    @classmethod
    def generate_on_conflict_sql_query(
            cls,
            source_table_schema_name: str = 'public',
            source_table_name: str = None, # noqa: RUF013
            target_table_schema_name: str = 'public',
            target_table_name: str = None, # noqa: RUF013
            list_columns: Iterable[str] = None, # noqa: RUF013
            pk: str | list[str, ...] = 'id',
            replace: bool = False,
    ) -> str:
        """
        **Function: generate_on_conflict_sql_query**

        This class method generates an SQL query for performing data insertion with conflict handling using a
        specified primary key.

        **Parameters:**
        - `source_table_schema_name` (str, optional): The schema name of the source table. Defaults to `'public'`.
        - `source_table_name` (str): The name of the source table from which data will be inserted.
        - `target_table_schema_name` (str, optional): The schema name of the target table where data will be inserted.
            Defaults to `'public'`.
        - `target_table_name` (str): The name of the target table where data will be inserted.
        - `list_columns` (Iterable[str], optional): An iterable object containing the names of columns to be inserted.
            If not specified, all columns from the source table will be inserted.
        - `pk` (str | list[str, ...]): The primary key for checking insertion conflicts. It can be a string representing
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

        pk = list_values_in_str_with_double_quotes(list_columns=pk) if isinstance(pk, list) else f'"{pk}"'

        if replace:
            replace = f'''DO UPDATE SET {', '.join([f'"{i}" = EXCLUDED."{i}"' for i in list_columns])}'''
        else:
            replace = 'DO NOTHING'


        sql = f'''
        INSERT INTO {target_table_schema_name}.{target_table_name} 
        (
            {list_values_in_str_with_double_quotes(list_columns=list_columns)}
        )
        SELECT 
            {list_values_in_str_with_double_quotes(list_columns=list_columns)} 
        FROM 
            {source_table_schema_name}.{source_table_name}
        ON CONFLICT ({pk}) {replace}
        '''

        return sql # noqa: RET504

    def get_database_description(
            self,
            table_name: str | list[str, ...] = None,
            table_schema: str | list[str, ...] = None,
    ) -> pd.DataFrame:
        """
        **Function: get_database_description**

        This method retrieves descriptive information about tables, columns, and their descriptions within a
        PostgreSQL database schema.
        
        **Parameters:**
        - `table_name` (str | list[str, ...]): The name of the table or list of tables. If provided, the function 
        will filter results to include only the specified table(s). Default is `None`.
        - `table_schema` (str | list[str, ...]): The schema name of the table or list of tables. If provided, the 
        function will filter results to include only the specified schema(s). Default is `None`.
        
        **Return:**
        - `pd.DataFrame`: A pandas DataFrame containing descriptive information about the tables and columns.
        
        **Example Usage:**
        
        ```python
        # Instantiate an object of the class
        pg = PostgreSQL()
        
        # Retrieve description for a specific table
        table_description = pg.get_database_description(table_name="employees")
        
        # Retrieve description for tables in a schema
        schema_description = pg.get_database_description(table_schema="public")

        :param table_name: The table name.
        :param table_schema: The table schema.
        :return: pd.DataFrame with descriptive information about the tables and columns.
        """
        
        where_condition = ''

        if table_name:
            if table_name and not isinstance(table_name, list):
                where_condition += f'''AND all_columns.table_name='{table_name}'\n'''
            else:
                where_condition += f'''AND all_columns.table_name IN ({list_values_in_str_with_single_quotes(
                    list_columns=table_name
                )})\n'''
                
        if table_schema:
            if table_schema and not isinstance(table_schema, list):
                where_condition += f'''AND all_columns.table_schema='{table_schema}'\n'''
            else:
                where_condition += f'''AND all_columns.table_schema IN ({list_values_in_str_with_single_quotes(
                    list_columns=table_schema
                )})\n'''
            
        sql_query = f'''
        SELECT
            all_columns.table_schema,
            schema_info.schema_description,
            all_columns.table_name,
            table_info.table_description AS table_description,
            all_columns.column_name,
            all_columns.data_type,
            columns_info.description AS column_description
        FROM
            information_schema.columns AS all_columns
        LEFT JOIN (
            SELECT
                *
            FROM
                pg_catalog.pg_statio_all_tables AS st
            LEFT JOIN pg_catalog.pg_description pgd 
                ON pgd.objoid = st.relid
            LEFT JOIN information_schema.columns AS c
                ON pgd.objsubid = c.ordinal_position
                AND c.table_schema = st.schemaname
                AND c.table_name = st.relname 
            ) AS columns_info
                ON all_columns.table_schema = columns_info.schemaname
                AND all_columns.table_name = columns_info.relname
                AND columns_info.column_name = all_columns.column_name
        LEFT JOIN (
            SELECT
                *,
                pg_catalog.obj_description(pgc.oid, 'pg_class') AS table_description
            FROM
                information_schema.tables AS t
            INNER JOIN pg_catalog.pg_class AS pgc 
                ON t.table_name = pgc.relname
            WHERE
                t.table_type = 'BASE TABLE'
                AND pg_catalog.obj_description(pgc.oid, 'pg_class') IS NOT NULL
            ) AS table_info
                ON	
                    table_info.table_name = all_columns.table_name
                    AND table_info.table_schema = all_columns.table_schema
        INNER JOIN (
            SELECT
                nspname,
                obj_description(oid) AS schema_description
            FROM
                pg_catalog.pg_namespace 
            ) AS schema_info 
                ON schema_info.nspname = all_columns.table_schema
        WHERE
            all_columns.table_schema != 'pg_catalog'
            {where_condition}
	    '''

        return self.execute_to_df(sql_query=sql_query)
        