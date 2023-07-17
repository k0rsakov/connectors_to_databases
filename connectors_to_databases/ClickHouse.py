from urllib.parse import quote

from sqlalchemy import create_engine, engine

from .BaseOperator import BaseOperator

# TODO: Change sqlalchemy to clickhouse-driver

class ClickHouse(BaseOperator):
    """
    Connector to PostgreSQL database
    """
    def __init__(
            self,
            host: str = 'localhost',
            port: int = 8123,
            login: str = 'default',
            password: str = 'default'
    ):
        """
        :param host: Host/IP database; default 'localhost'.
        :param port: port database; default '8123'.
        :param login: login to database; default 'default'.
        :param password: password to database; default 'default'.
        """
        super().__init__(host, port, login, password)
        self._host = host
        self._login = login
        self._password = password
        self._port = port

    def _authorization_database(self) -> engine.base.Engine:
        """
        Creating connector engine to database ClickHouse.
        """

        engine_str = f'clickhouse://{self._login}:{quote(self._password)}@{self._host}:{self._port}/default'

        return create_engine(engine_str)
