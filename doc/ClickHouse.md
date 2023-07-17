

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
