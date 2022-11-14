from sqlalchemy import create_engine
import pandas as pd

class PostgreSQL:
    """
    Класс для работы с БД PostgreSQL

    Принимаемые параметры:
        - `host` – хост базы данных в `str` формате, по умолчанию ='localhost'
        - `database` – название базы данных в `str` формате, по умолчанию 'postgres'
        - `login` – логин для авторизации в `str` формате, по умолчанию 'postgres'
        - `password` – пароль для авторизации в `str` формате, по умолчанию 'postgres'
        - `port` – порт базы данных в `int` формате, по умолчанию 5432
    """
    def __init__(self,
                 host: str = 'localhost',
                 database: str = 'postgres',
                 login: str = 'postgres',
                 password: str = 'postgres',
                 port: int = 5432
                 ):
        self.host = host
        self.db = database
        self.login = login
        self.password = password
        self.port = port

    def authorization_pg(self):
        """
        Создание коннектора к БД PostgreSQL
        """
        engine_str = f'postgresql://{self.login}:{self.password}@{self.host}:{self.port}/{self.db}'
        engine = create_engine(engine_str)

        return engine

    def into_pg_table(self,
                      pg_table_name: str,
                      df: pd.DataFrame,
                      schema: str = 'public'
                      ) -> bool:
        """
        Помещение данных в БД PostgreSQL

        Принимаемые атрибуты:
            - `pg_table_name` – название таблицы для помещения в `str` формате
            - `df` – pd.DataFrame для помещения в базу данных
            - `schema` – название схемы для помещения в `str` формате, по умолчанию 'public'

        Возвращает:
            - True при успехе
        """
        try:
            dataframe = df
            connector = self.authorization_pg()
            dataframe.to_sql(
                name=pg_table_name,
                schema=schema,
                con=connector,
                chunksize=10000,
                index=False,
                if_exists='append'
            )
            return True
        except Exception:
            print(f"Can't insert df in {pg_table_name}")

    def execute_to_df(
            self,
            query: str,) -> pd.DataFrame:
        """
        Метод для извлечения df из БД

        Принимаемые атрибуты:
            - `query` – SQL-запрос в `str` формате.

        Возвращает:
            - pd.DataFrame
        """
        try:
            return pd.read_sql(query, self.authorization_pg())
        except Exception:
            print(f"Can't execute df from PostgreSQL")