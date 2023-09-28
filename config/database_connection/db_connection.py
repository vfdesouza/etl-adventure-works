from os import getenv
import psycopg2

from dotenv import load_dotenv

load_dotenv()


class DataBaseConnection:
    """classe com as funções responsáveis pela interação com o banco de dados"""
    def __init__(self) -> None:
        pass

    def get_db_creds(self, database: str):
        """função responsável por retornar as credenciais de acesso do banco de dados"""
        db_creds = {
            "postgres_advw": {
                "host": getenv("HOST_ADVW"),
                "database": getenv("DATABASE_ADVW"),
                "user": getenv("USER_ADVW"),
                "password": getenv("PASSWORD_ADVW"),
                "port": "5432",
                "type": "postgres",
            }
        }

        return db_creds[database] if database in db_creds.keys() else None

    def get_db_connection(self, database: str):
        """função responsável por retornar a conexão com o banco de dados"""
        db = self.get_db_creds(database=database)
        conn = psycopg2.connect(
            host=db["host"],
            database=db["database"],
            port=db["port"],
            user=db["user"],
            password=db["password"],
        )

        return conn
