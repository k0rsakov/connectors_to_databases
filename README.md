# Connector to databases

![PyPI](https://img.shields.io/pypi/v/connectors-to-databases?color=blueviolet) 
![Python 3.6, 3.7, 3.8, 3.9, 3.10, 3.11](https://img.shields.io/pypi/pyversions/clubhouse?color=blueviolet)
![License](https://img.shields.io/pypi/l/connectors-to-databases?color=blueviolet) 

**Connector to databases** – easy package for connect with database 
[PostgreSQL](https://github.com/postgres/postgres) and 
[ClickHouse](https://github.com/ClickHouse/ClickHouse)

## Installation

Install the current version with 
[PyPI](https://pypi.org/project/connectors-to-databases/):

```bash
pip install connectors-to-databases
```

Or from GitHub:

```bash
pip install https://github.com/k0rsakov/connectors_to_databases/archive/refs/heads/main.zip
```

## How to use class PostgreSQL

### Creating instance of class

You can create as many database connectors as you want.

```python
from connectors_to_databases import PostgreSQL

pg = PostgreSQL()

pg_other = PostgreSQL(
    host='0.0.0.0',
    port=0,
    database='main',
    login='admin',
    password='admin',
)
```

### Creating a table for examples

```python
pg.execute_script('CREATE TABLE simple_ (id int4)')
```

### Filling the table with data

```python
# simple pd.DataFrame
df = pd.DataFrame(data={'id':[1]})

pg.insert_df(
    df=df,
    pg_table_name='simple_'
)
```

### Getting data from a table

```python
pg.execute_to_df(
    '''select * from simple_'''
)
```

### Getting a connector to the database.

It can be used as you need.

```python
pg.get_uri()
```

What does the connector look like

```log
Engine(postgresql://postgres:***@localhost:5432/postgres)
```

### Delete our `simple_` table

```python
pg.execute_script('DROP TABLE simple_')
```

## How to use class ClickHouse

### Creating instance of class

You can create as many database connectors as you want.

```python
from connectors_to_databases import ClickHouse

ch = ClickHouse()

ch_other = ClickHouse(
    host='0.0.0.0',
    port=0,
    login='admin',
    password='admin',
)
```

### Creating a table for examples

```python
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
```

### Filling the table with data

```python
# simple pd.DataFrame
df = ch.DataFrame(data={'value':[1]})

ch.insert_df(
    df=df,
    pg_table_name='test'
)
```

### Getting data from a table

```python
ch.execute_to_df(
    '''select * from test'''
)
```

### Getting a connector to the database.

It can be used as you need.

```python
ch.get_uri()
```

What does the connector look like

```log
Engine(clickhouse://click:***@localhost:8123/default)
```

### Delete our `simple_` table

```python
ch.execute_script('DROP TABLE test')
```


## Contributing

Bug reports and/or pull requests are welcome

## License

The module is available as open source under the terms of the 
[Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0)
