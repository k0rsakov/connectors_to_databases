import pandas as pd
from sqlalchemy import text

from connectors_to_databases.MariaDB import MariaDB


# TODO: add fixtures
def test_execute_script_drop():
    """
    Checking the script for execution.

    The test drop tables before tests.
    """
    m = MariaDB(
        host="127.0.0.1",
        port=3,
    )

    m.execute_script("DROP TABLE IF EXISTS test")


def test_connection():
    """Checking connection to database."""
    m = MariaDB(
        host="127.0.0.1",
        port=2,
    )

    check = m.check_connection_to_database()

    assert check is True


def test_execute_script():
    """
    Checking the script for execution.

    The test creates a table and checks if it has been created.
    """

    m = MariaDB(
        host="127.0.0.1",
        port=2,
    )

    m.execute_script("CREATE TABLE IF NOT EXISTS test(id INT8 AUTO_INCREMENT PRIMARY KEY, value int8)")

    df = m.execute_to_df(
        """
        SHOW TABLES
        WHERE tables_in_sys = 'test'
        """,
    )

    assert len(df) == 1
    assert df.Tables_in_sys[0] == "test"


def test_insert_m_table():
    """Checking the method for inserting data."""

    m = MariaDB(
        host="127.0.0.1",
        port=2,
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

    m = MariaDB(
        host="127.0.0.1",
        port=2,
    )

    df = m.execute_to_df("SELECT count(value) FROM test")

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)


def test_get_uri():
    """Checking for getting a uri for use outside class methods."""

    m = MariaDB(
        host="127.0.0.1",
        port=2,
    )

    with m.get_uri().begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test"))
