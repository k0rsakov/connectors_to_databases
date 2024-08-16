# Connector to databases

![PyPI](https://img.shields.io/pypi/v/connectors-to-databases?color=blueviolet) 
![Python](https://img.shields.io/pypi/pyversions/connectors-to-databases?color=blueviolet)
![License](https://img.shields.io/pypi/l/connectors-to-databases?color=blueviolet)

![PG](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![PG](https://img.shields.io/badge/ClickHouse-white?style=for-the-badge&logo=clickhouse&logoColor=yellow)

**Connector to databases** – easy package for connect with database:

- [ClickHouse](https://github.com/ClickHouse/ClickHouse)
- [MariaDB](https://github.com/MariaDB/server)
- [MSSQL](https://www.microsoft.com/en/sql-server/sql-server-2019)
- [MySQL](https://github.com/mysql/mysql-server)
- [PostgreSQL](https://github.com/postgres/postgres)- 

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

## Usage

How to use connections:
- [ClickHouse.md](doc/ClickHouse.md)
- [MariaDB.md](doc/MariaDB.md)
- [MSSQL.md](doc/MSSQL.md)
- [MySQL.md](doc/MySQL.md)
- [PostgreSQL.md](doc/PostgreSQL.md)

## Contributing

Bug reports and/or pull requests are welcome

### Create `venv`

```bash
python3.11 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install -r requirements.txt
```


## License

The module is available as open source under the terms of the 
[Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0)
