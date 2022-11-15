# Connector to database

![PyPI](https://img.shields.io/pypi/v/connectors-to-databases?color=blueviolet) 
![Python 3.6, 3.7, 3.8, 3.9, 3.10, 3.11](https://img.shields.io/pypi/pyversions/clubhouse?color=blueviolet)
![License](https://img.shields.io/pypi/l/connectors-to-databases?color=blueviolet) 

**Connector to database** – easy package for connect with database PostgreSQL.

## Installation

Install the current version with [PyPI](https://pypi.org/project/connectors-to-databases/):

```bash
pip install connectors-to-databases
```

Or from GitHub:

```bash
pip install https://github.com/k0rsakov/connectors_to_databases/archive/refs/heads/main.zip
```

## Example

you can create as many database connectors as you want.

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

Creating a table for examples

```python
pg.execute_script('CREATE TABLE simple_ (id int)')
```

Filling the table with data

```python
# simple pd.DataFrame
df = pd.DataFrame(data={'id':[1]})

pg.into_pg_table(
    df=df,
    pg_table_name='simple_'
)
```

Getting data from a table

```python
pg.execute_to_df(
    '''select * from simple_'''
)
```

Getting a connector to the database.
It can be used as you need.

```python
pg.get_uri()
```

What does the connector look like

```log
Engine(postgresql://postgres:***@localhost:5432/postgres)
```

Delete our `simple_` table

```python
pg.execute_script('DROP TABLE simple_')
```


## Contributing

Bug reports and/or pull requests are welcome

## License

The module is available as open source under the terms of the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0)
