# How to use class PostgreSQL

## Creating instance of class

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

## Check connection to database

You can check connection to database.

```python
pg.check_connection()
```

## Creating a table for examples

You can create table and execute any PostgreSQL query.

```python
pg.execute_script('CREATE TABLE simple_ (id int4)')
```

## Filling the table with data

You can insert data from pandas dataframe in PostgreSQL table

```python
# simple pd.DataFrame
df = pd.DataFrame(data={'id':[1]})

pg.insert_df(
    df=df,
    pg_table_name='simple_'
)
```

## Getting data from a table

You can get data from PostgreSQL table in pandas dataframe.

```python
pg.execute_to_df(
    '''select * from simple_'''
)
```

## Getting a connector to the database.

It can be used as you need.

```python
pg.get_uri()
```

What does the connector look like

```log
Engine(postgresql://postgres:***@localhost:5432/postgres)
```

## Delete our `simple_` table

You can drop any PostgreSQL table

```python
pg.execute_script('DROP TABLE simple_')
```