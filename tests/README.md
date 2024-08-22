# Tests

Description for conducting tests.

Tests must be performed using pytest.

For the tests to work correctly, they must be run from the root folder of the 
`connectors_to_databases` project

The commands for running tests for each module are described below.

To install pytest in your environment

## All test

To run all tests, run the command:

```bash
python -m pytest tests/
```

## ClickHouse

To run ClickHouse tests, run the command:

```bash
python -m pytest tests/clickhouse/
```

## MariaDB

To run MariaDB tests, run the command:

```bash
python -m pytest tests/mariadb/
```

## MySQL

To run MySQL tests, run the command:

```bash
python -m pytest tests/mysql/
```

## PostgreSQL

To run PostgreSQL tests, run the command:

```bash
python -m pytest tests/postgresql/
```


