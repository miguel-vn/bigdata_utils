import os
import warnings

from sqlalchemy import create_engine


class PostgresAuth:

    def __init__(self, username: str, password: str, database: str, url: str):
        self.username = username
        self.password = password
        self.database = database
        self.url = url


class PostgreSQLParams:
    auth: PostgresAuth = None

    jdbc_params: dict = None
    engine = None

    def set_auth(self, auth: PostgresAuth):
        self.auth = auth
        return self

    def set_auth_from_env(self, env_name: str = None):
        postfix = ''
        if env_name:
            postfix = f'_{env_name.upper()}'

        self.auth = PostgresAuth(username=os.getenv('POSTGRES_USERNAME'),
                                 password=os.getenv('POSTGRES_PASSWORD'),
                                 database=os.getenv(f'POSTGRES_DATABASE{postfix}'),
                                 url=os.getenv(f'POSTGRES_URL{postfix}'))

        return self

    def set_jdbc_params(self):
        if self.auth:
            self.jdbc_params = {'url': f'jdbc:postgresql://{self.auth.url}/{self.auth.database}',
                                'properties': {'user': self.auth.username,
                                               'password': self.auth.password,
                                               'driver': 'org.postgresql.Driver'}}
        else:
            warnings.warn('Auth is not set for setting JDBC params')

        return self

    def set_engine(self):
        if self.auth:
            connect_str = f"postgresql://{self.auth.username}:{self.auth.password}@{self.auth.url}/{self.auth.database}"
            self.engine = create_engine(connect_str)
        else:
            warnings.warn('Auth is not set for setting postgres engine')

        return self

    def __repr__(self):
        if self.auth:
            return f'Postgres class for {self.auth.username} to {self.auth.database} on {self.auth.url}'

        return 'Postgres user class without auth'
