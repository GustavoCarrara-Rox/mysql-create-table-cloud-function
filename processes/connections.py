# 1. Standard library imports (Python built-ins)
import logging
import os
from urllib.parse import quote_plus

# 2. Third-party imports
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

# 3. Local application/library specific imports
from processes.utils import get_secret

def connection_mysql() -> Session:

    MYSQL_DATABASE_USER = get_secret("mysql-ingestion-test-db-username")
    MYSQL_DATABASE_PASSWORD = quote_plus(get_secret("mysql-ingestion-test-db-password"))
    MYSQL_DATABASE_HOST = get_secret("mysql-ingestion-test-db-server-id")
    MYSQL_DATABASE_NAME = get_secret("mysql-ingestion-test-db-name")

    try:
        engine_mysql = create_engine(
            f'mysql+pymysql://{MYSQL_DATABASE_USER}:{MYSQL_DATABASE_PASSWORD}@{MYSQL_DATABASE_HOST}/{MYSQL_DATABASE_NAME}'
        )

        local_session = sessionmaker(bind=engine_mysql)
        session = local_session()
    
    except Exception as e:
        raise Exception(f"Failed to connect to mysql: {e}")
    logging.info(f"Successfully connected to MySQL instance")   
    return session


def connection_postgresql() -> Session:
    
    POSTGRE_DATABASE_USER = get_secret("controle_username")
    POSTGRE_DATABASE_PASSWORD = quote_plus(get_secret("controle_password"))
    POSTGRE_DATABASE_HOST = get_secret("controle_server_id")
    POSTGRE_DATABASE_NAME = get_secret("controle_db")

    try:
        engine_postgres = create_engine(
            f'postgresql+psycopg2://{POSTGRE_DATABASE_USER}:{POSTGRE_DATABASE_PASSWORD}@{POSTGRE_DATABASE_HOST}/{POSTGRE_DATABASE_NAME}'
        )

        local_session = sessionmaker(bind=engine_postgres)
        session = local_session()
    
    except Exception as e:
        raise Exception(f"Failed to connect to PostgreSQL: {e}")
    
    logging.info(f"Successfully connected to PostgreSQL instance")   
    return session
    