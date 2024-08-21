import uuid

import pandas as pd

from connectors_to_databases.ClickHouse import ClickHouse


# TODO: add fixtures
def test_get_uri_and_drop_test_table():
    """Checking for getting uri for use outside class methods."""

    ch = ClickHouse(
        login="click",
        password="click",  # noqa: S106
    )

    ch.get_uri().query("DROP TABLE IF EXISTS test")


def test_get_uri_and_drop_test_uuid_table():
    """Checking for getting uri for use outside class methods."""

    ch = ClickHouse(
        login="click",
        password="click",  # noqa: S106
    )

    ch.get_uri().query("DROP TABLE IF EXISTS test_uuid")


def test_execute_script():
    """
    Checking the script for execution.

    The test creates a table and checks if it has been created.
    """
    ch = ClickHouse(
        login="click",
        password="click",  # noqa: S106
    )

    ch.execute_script(
        """
        CREATE TABLE IF NOT EXISTS test
        (
            value Int64
        )
        ENGINE = MergeTree
        ORDER BY value
        """,
    )

    df = ch.execute_to_df(
        """
        SELECT
            *
        FROM
            information_schema.`tables` t
        WHERE
            table_schema = 'default'
            AND table_name = 'test'
        """,
    )

    assert len(df) == 1
    assert df.table_name[0] == "test"


def test_insert_ch_table():
    """Checking the method for inserting data."""

    ch = ClickHouse(
        login="click",
        password="click",  # noqa: S106
    )

    d = {"value": list(range(1000000))}
    df = pd.DataFrame(d)
    ch.insert_df(
        df=df,
        table_name="test",
    )

    df = ch.execute_to_df("SELECT * FROM test")

    assert len(df) == 1000000


def test_execute_df():
    """Checking the method for extracting data into a dataframe."""

    ch = ClickHouse(
        login="click",
        password="click",  # noqa: S106
    )

    df = ch.execute_to_df("SELECT count(value) FROM test")

    assert len(df) == 1
    assert isinstance(df, pd.DataFrame)


def test_insert_ch_table_with_dtype():
    """Checking the method for inserting data with dtype."""

    ch = ClickHouse(
        login="click",
        password="click",  # noqa: S106
    )

    ch.execute_script(
        """
        CREATE TABLE IF NOT EXISTS test_uuid
        (
            value UUID
        )
        ENGINE = MergeTree
        ORDER BY
        value
        """,
    )

    d = {"value": [str(uuid.uuid4()) for i in range(1000000)]}
    df = pd.DataFrame(d)
    ch.insert_df(
        df=df,
        table_name="test_uuid",
        dtype=["UUID"],
    )

    df = ch.execute_to_df("SELECT * FROM test_uuid")

    assert len(df) == 1000000


def test_check_connection():
    """Check connection to database."""

    ch = ClickHouse(
        login="click",
        password="click",  # noqa: S106
    )

    assert ch.check_connection_to_database() is True
