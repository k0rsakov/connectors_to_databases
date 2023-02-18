import pandas as pd

from connectors_to_databases.ClickHouse import ClickHouse


def test_execute_script():
    """"""
    ch = ClickHouse(
        login='click',
        password='click',
    )

    ch.execute_script(
        '''
        CREATE TABLE test 
        (
            value Int64
        ) 
        ENGINE = MergeTree 
        ORDER BY value
        '''
    )

    df = ch.execute_to_df(
        '''
        SELECT
            *
        FROM
            information_schema.`tables` t
        WHERE
            table_schema = 'default'
            AND table_name = 'test'
        '''
    )

    assert len(df) == 1

def test_insert_pg_table():
    """"""
    ch = ClickHouse(
        login='click',
        password='click',
    )

    d = {'value': [i for i in range(10000)]}
    df = pd.DataFrame(d)
    ch.insert_df(
        df=df,
        chunksize=None,
        pg_table_name='test'
    )

    df = ch.execute_to_df('SELECT * FROM test')

    assert len(df) == 10000


def test_execute_df():
    """"""

    ch = ClickHouse(
        login='click',
        password='click',
    )

    df = ch.execute_to_df('SELECT count(value) FROM test')

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)

def test_get_uri():
    """"""

    ch = ClickHouse(
        login='click',
        password='click',
    )

    ch.get_uri().execute('DROP TABLE IF EXISTS test')
