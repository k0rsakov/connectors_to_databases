from urllib.parse import quote

from sqlalchemy import create_engine, engine

from connectors_to_databases.BaseOperator import BaseOperator

class MariaDB(BaseOperator):
    def __init__(
            self,
            host: str = '127.0.0.1',
            port: int = 3306,
            database: str = 'sys',
            login: str = 'root',
            password: str = 'root'
    ):
        """
        :param host: Host/IP database; default 'localhost'.
        :param database: name database; default 'sys'.
        :param port: port database; default 3306.
        :param login: login to database; default 'root'.
        :param password: password to database; default 'root'.
        """
        super().__init__(host, port, database, login, password)
        self._host = host
        self._database = database
        self._login = login
        self._password = password
        self._port = port

    def _authorization_database(self) -> engine.base.Engine:
        """
        Creating connector engine to database MariaDB.
        """

        # mariadb+mariadbconnector
        engine_str = f'mariadb+pymysql://' \
                     f'{self._login}:{quote(self._password)}@{self._host}:{self._port}/' \
                     f'{self._database}'

        return create_engine(engine_str)
