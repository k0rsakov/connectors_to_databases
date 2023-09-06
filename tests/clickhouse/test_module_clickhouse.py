import pandas as pd

from connectors_to_databases.ClickHouse import ClickHouse


def test_execute_script():
    """
    Checking the script for execution.

    The test creates a table and checks if it has been created.
    """
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

    df = ch.execute_to_df( # noqa: PD901
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

def test_insert_ch_table():
    """
    Checking the method for inserting data.
    """
    ch = ClickHouse(
        login='click',
        password='click',
    )

    d = {'value': list(range(1000000))}
    df = pd.DataFrame(d) # noqa: PD901
    ch.insert_df(
        df=df,
        chunksize=None,
        table_name='test'
    )

    df = ch.execute_to_df('SELECT * FROM test') # noqa: PD901

    assert len(df) == 1000000


def test_execute_df():
    """
    Checking the method for extracting data into a dataframe.
    """

    ch = ClickHouse(
        login='click',
        password='click',
    )

    df = ch.execute_to_df('SELECT count(value) FROM test')  # noqa: PD901

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)

def test_get_uri():
    """
    Checking for getting a uri for use outside class methods.
    """

    ch = ClickHouse(
        login='click',
        password='click',
    )

    ch.get_uri().execute('DROP TABLE IF EXISTS test')
