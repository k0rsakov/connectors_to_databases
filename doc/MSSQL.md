# How to use class MSSQL

## Creating instance of class

You can create as many database connectors as you want.

```python
from connectors_to_databases import MSSQL

m = MSSQL()

m_other = MSSQL(
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
m.check_connection()
```

## Creating a table for examples

You can create table and execute any MSSQL query.

```python
m.execute_script('CREATE TABLE simple_ (id int4)')
```

## Filling the table with data

You can insert data from pandas dataframe in MSSQL table

```python
# simple pd.DataFrame
df = pd.DataFrame(data={'id':[1]})

m.insert_df(
    df=df,
    m_table_name='simple_'
)
```

## Getting data from a table

You can get data from MSSQL table in pandas dataframe.

```python
m.execute_to_df(
    '''select * from simple_'''
)
```

## Getting a connector to the database.

It can be used as you need.

```python
m.get_uri()
```

What does the connector look like

```log
Engine(Engine(MSSQL+pyMSSQL://root:***@127.0.0.1:3/sys))
```

## Delete our `simple_` table

You can drop any MSSQL table

```python
m.execute_script('DROP TABLE simple_')
```