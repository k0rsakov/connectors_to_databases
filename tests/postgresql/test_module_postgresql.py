import pandas as pd
from sqlalchemy import text

from connectors_to_databases.PostgreSQL import PostgreSQL


def test_execute_script_drop():
    """
    Checking the script for execution.

    The test drop tables before tests.
    """
    pg = PostgreSQL(
        port=1,
    )

    pg.execute_script("DROP TABLE IF EXISTS test")


def test_execute_script():
    """
    Checking the script for execution.

    The test creates a table and checks if it has been created.
    """
    pg = PostgreSQL(
        port=1,
    )

    pg.execute_script("CREATE TABLE test(id bigserial PRIMARY KEY, value int8)")

    df = pg.execute_to_df(
        """
        SELECT
            table_name
        FROM
            information_schema."tables" AS t
        WHERE
            table_schema = 'public'
            AND table_name = 'test'
        """,
    )

    assert len(df) == 1
    assert df.table_name[0] == "test"


def test_insert_pg_table():
    """Checking the method for inserting data."""

    pg = PostgreSQL(
        port=1,
    )

    d = {"value": list(range(10000))}
    df = pd.DataFrame(d)
    pg.insert_df(
        df=df,
        table_name="test",
    )

    df = pg.execute_to_df("SELECT * FROM test")

    assert len(df) == 10000


def test_execute_df():
    """Checking the method for extracting data into a dataframe."""

    pg = PostgreSQL(
        port=1,
    )

    df = pg.execute_to_df("SELECT count(value) AS value FROM test")

    assert df.value[0] == 10000


def test_get_uri():
    """Checking for getting uri for use outside class methods."""

    pg = PostgreSQL(
        port=1,
    )

    with pg.get_uri().begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS test"))
