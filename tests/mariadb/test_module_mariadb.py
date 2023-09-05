import pandas as pd

from connectors_to_databases.MariaDB import MariaDB


def test_execute_script():
    """
    Checking the script for execution.

    The test creates a table and checks if it has been created.
    """
    m = MariaDB(
        host='127.0.0.1',
        port=2,
    )

    m.execute_script('CREATE TABLE test(id bigserial PRIMARY KEY, value int8)')

    df = m.execute_to_df(
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

def test_insert_m_table():
    """
    Checking the method for inserting data.
    """
    m = MariaDB(
        host='127.0.0.1',
        port=2,
    )

    d = {'value': list(range(10000))}
    df = pd.DataFrame(d)
    m.insert_df(
        df=df,
        chunksize=None,
        table_name='test'
    )

    df = m.execute_to_df('SELECT * FROM test')

    assert len(df) == 10000


def test_execute_df():
    """
    Checking the method for extracting data into a dataframe.
    """

    m = MariaDB(
        host='127.0.0.1',
        port=2,
    )

    df = m.execute_to_df('SELECT count(value) FROM test')

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)

def test_get_uri():
    """
    Checking for getting a uri for use outside class methods.
    """

    m = MariaDB(
        port=1
    )

    m.get_uri().execute('DROP TABLE IF EXISTS test')
