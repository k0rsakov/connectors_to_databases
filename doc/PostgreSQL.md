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

### Check connection to database

```python
pg.check_connection()
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