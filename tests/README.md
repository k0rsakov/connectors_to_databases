# Tests

Description for conducting tests.

Tests must be performed using pytest.

For the tests to work correctly, they must be run from the root folder of the 
`connectors_to_databases` project

The commands for running tests for each module are described below.

To install pytest in your environment, run the command:

```bash
pip install pytest==7.4.1
```

## ClickHouse

To run ClickHouse tests, run the command:

```bash
python -m pytest tests/clickhouse/test_module_clickhouse.py
```

## MariaDB

To run MariaDB tests, run the command:

```bash
python -m pytest tests/mariadb/test_module_mariadb.py
```

## MySQL

To run MySQL tests, run the command:

```bash
python -m pytest tests/mysql/test_module_mysql.py
```

## PostgreSQL

To run PostgreSQL tests, run the command:

```bash
python -m pytest tests/postgresql/test_module_postgresql.py
```
