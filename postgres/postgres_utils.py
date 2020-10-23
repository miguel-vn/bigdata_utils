import os

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def get_jdbc_params() -> dict:
    """
    Method creates dict of parameters for JDBC-driver from environment variables for PostgreSQL.
    e.g. spark_data_frame.write.jdbc(table=table, **self.jdbc_params)
    """
    username = os.environ.get('POSTGRESQL_USERNAME')
    password = os.environ.get('POSTGRESQL_PASSWORD')
    db_name = os.environ.get('POSTGRESQL_DATABASE')
    url = os.environ.get('POSTGRESQL_URL')

    jdbc_params = {'url': f'jdbc:postgresql://{url}/{db_name}',
                   'properties': {'user': username,
                                  'password': password,
                                  'driver': 'org.postgresql.Driver'}}

    return jdbc_params


def get_postgres_engine() -> Engine:
    """
    The method creates configured SQLAlchemy engine for PostgreSQL.
    """
    username = os.environ.get('POSTGRESQL_USERNAME')
    password = os.environ.get('POSTGRESQL_PASSWORD')
    db_name = os.environ.get('POSTGRESQL_DATABASE')
    url = os.environ.get('POSTGRESQL_URL')

    engine = create_engine(f"postgresql://{username}:{password}@{url}/{db_name}")

    engine.connect().close()  # Auth check

    return engine
