import pandas as pd

from connectors_to_databases.PostgreSQL import PostgreSQL


def test_execute_script():
    """
    Checking the script for execution.

    The test creates a table and checks if it has been created.
    """
    pg = PostgreSQL(
        port=1,
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
    """
    Checking the method for inserting data.
    """
    pg = PostgreSQL(
        port=1
    )

    d = {'value': [i for i in range(10000)]}
    df = pd.DataFrame(d)
    pg.insert_df(
        df=df,
        chunksize=None,
        pg_table_name='test'
    )

    df = pg.execute_to_df('SELECT * FROM test')

    assert len(df) == 10000


def test_execute_df():
    """
    Checking the method for extracting data into a dataframe.
    """

    pg = PostgreSQL(
        port=1
    )

    df = pg.execute_to_df('SELECT count(value) FROM test')

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)

def test_get_uri():
    """
    Checking for getting a uri for use outside of class methods.
    """

    pg = PostgreSQL(
        port=1
    )

    pg.get_uri().execute('DROP TABLE IF EXISTS test')
