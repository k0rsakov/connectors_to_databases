import pandas as pd
from sqlalchemy import text

from connectors_to_databases.MySQL import MySQL


# TODO: add fixtures
def test_execute_script_drop():
    """
    Checking the script for execution.

    The test drop tables before tests.
    """
    m = MySQL(
        host="127.0.0.1",
        port=3,
    )

    m.execute_script("DROP TABLE IF EXISTS test")


def test_connection():
    """Checking connection to database."""

    m = MySQL(
        host="127.0.0.1",
        port=3,
    )

    check = m.check_connection_to_database()

    assert check is True


def test_execute_script():
    """
    Checking the script for execution.

    The test creates a table and checks if it has been created.
    """

    m = MySQL(
        host="127.0.0.1",
        port=3,
    )

    m.execute_script("CREATE TABLE IF NOT EXISTS test(id INT8 AUTO_INCREMENT PRIMARY KEY, value int8)")

    df = m.execute_to_df(
        """
        SHOW TABLES
        WHERE tables_in_sys = 'test'
        """,
    )

    assert len(df) == 1


def test_insert_m_table():
    """Checking the method for inserting data."""

    m = MySQL(
        host="127.0.0.1",
        port=3,
    )

    d = {"value": list(range(10000))}
    df = pd.DataFrame(d)
    m.insert_df(
        df=df,
        chunksize=None,
        table_name="test",
    )

    df = m.execute_to_df("SELECT * FROM test")

    assert len(df) == 10000


def test_execute_df():
    """Checking the method for extracting data into a dataframe."""

    m = MySQL(
        host="127.0.0.1",
        port=3,
    )

    df = m.execute_to_df("SELECT count(value) FROM test")

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)


def test_get_uri():
    """Checking for getting a uri for use outside class methods."""

    m = MySQL(
        host="127.0.0.1",
        port=3,
    )

    with m.get_uri().begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test"))

