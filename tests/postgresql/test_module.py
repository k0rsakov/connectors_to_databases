"""
Модульные тесты проверяют функциональность отдельного модуля кода,
изолированного от его зависимостей
"""
import pandas as pd

from connectors_to_databases.postgre_base import PostgreSQL


def test_execute_script():
    """"""
    pg = PostgreSQL(
        port=1
    )

    pg.execute_script('CREATE TABLE test(id bigserial PRIMARY KEY, value int8)')

    df = pg.execute_to_df(
        '''
        SELECT
            *
        FROM
            information_schema."tables" AS t
        WHERE
            table_schema = 'public'
            AND table_name = 'test'
        '''
    )

    assert len(df) == 1

def test_insert_pg_table():
    """"""
    pg = PostgreSQL(
        port=1
    )

    d = {'value': [i for i in range(10000)]}
    df = pd.DataFrame(d)
    pg.into_pg_table(
        df=df,
        chunksize=None,
        pg_table_name='test'
    )

    df = pg.execute_to_df('SELECT * FROM test')

    assert len(df) == 10000


def test_execute_df():
    """"""

    pg = PostgreSQL(
        port=1
    )

    df = pg.execute_to_df('SELECT count(value) FROM test')

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)

