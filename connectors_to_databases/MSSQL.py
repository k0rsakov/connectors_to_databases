from urllib.parse import quote

from sqlalchemy import create_engine, engine

from connectors_to_databases.BaseOperator import BaseOperator

class MSSQL(BaseOperator):
    def __init__(
            self,
            host: str = 'localhost',
            port: int = 1433,
            database: str = 'master',
            login: str = 'SA',
            password: str = 'SA'
    ):
        """
        :param host: Host/IP database; default 'localhost'.
        :param database: name database; default 'localhost'.
        :param port: port database; default 1433.
        :param login: login to database; default 'SA'.
        :param password: password to database; default 'SA'.
        """
        super().__init__(host, port, database, login, password)
        self._host = host
        self._database = database
        self._login = login
        self._password = password
        self._port = port

    def _authorization_database(self) -> engine.base.Engine:
        """
        Creating connector engine to database PostgreSQL.
        """

        engine_str = f'mssql+pymssql://' \
                     f'{self._login}:{quote(self._password)}@{self._host}:{self._port}/' \
                     f'{self._database}'

        return create_engine(engine_str)
