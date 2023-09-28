from os import getenv
import time
import requests as rq
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.types import Text
from dotenv import load_dotenv

from config.database_connection.db_connection import DataBaseConnection
from libs.logger import Logger

load_dotenv()

BASE_URL = getenv("BASE_URL")


class Module:
    """Class with functions"""

    def __init__(self) -> None:
        pass

    def get_restful_api(self, dim: str, pag: int, max_retries: int = 5):
        """função responsável por extrair os dados da API

        Args:
            dim (str): dimensão a ser extraída da API
            pag (int): se a paginação deve ser aplicada | 0 false, 1 true
            max_retries (int, optional): quantidade máxima de requisições. Defaults to 5.

        Returns:
            list: lista de jsons com os dados extraídos
        """
        list_response = []
        count_request = 0
        paginate = 500
        page_number = 1

        headers = {
            "accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
        }

        url = f"{BASE_URL}/{dim}"

        if pag == 0:
            while count_request <= max_retries:
                response = rq.get(url, headers=headers, timeout=10)
                count_request += 1
                if response.status_code == 200:
                    data = response.json()
                    list_response.append(data)
                    break
                elif count_request <= 5:
                    Logger.warning(
                        message=f"Request failed with status code: {response.status_code}"
                    )
                    Logger.warning(message="Retrying...")
                    time.sleep(5)
                else:
                    Logger.error(
                        message="Maximum number of retries reached. Application closed."
                    )
                    break
        elif pag == 1:
            while True:
                url = f"{BASE_URL}/{dim}?PageNumber={page_number}&PageSize={paginate}"
                response = rq.get(url, headers=headers, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    if not data:
                        break
                    list_response.append(data)
                    page_number += 1
                elif count_request <= 5:  # Fazer no máximo 5 tentativas extras
                    Logger.warning(
                        message=f"Request failed with status code: {response.status_code}"
                    )
                    Logger.warning(message="Retrying...")
                    time.sleep(5)  # Esperar 5 segundos antes de tentar novamente
                else:
                    Logger.error(
                        message="Maximum number of retries reached. Application closed."
                    )
                    break

                count_request += 1

                if page_number > 200:
                    Logger.warning(
                        message="Maximum number of pages reached. Application closed."
                    )
                    break

        return list_response

    def create_conn_psycopg2(self, database: str):
        """Conexão com o banco de dados usando o psycopg2"""
        return DataBaseConnection().get_db_connection(database)

    def create_conn_engine(self, db_name: str):
        """Conexão com o banco de dados usando o sqlalchemy.create_engine"""
        db = DataBaseConnection().get_db_creds(database=db_name)

        host = db["host"]
        database = "raw_zone"
        port = db["port"]
        user = db["user"]
        password = db["password"]

        connection_string = (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        )

        engine = create_engine(connection_string)
        conn = engine.connect()

        return conn

    def load_dataframe_raw_zone(
        self, db: str, dataframe: pd.DataFrame, table_name: str, write: str
    ) -> None:
        """função responsável por carregar os dados extraidos no banco de dados

        Args:
            db (str): banco de dados que os dados serão salbos
            dataframe (pd.DataFrame): dataframe com os dados extraidos
            table_name (str): nome da tabela a ser salva
            write (str): modo de insersão dos dados (append, replace etc)
        """
        Logger.info(
            message=f"Starting to send data from the dataframe to the table {table_name.lower()} in the raw zone.."
        )
        start_time = time.time()
        conn = self.create_conn_engine(db)
        dict_types = {col_name: Text() for col_name in dataframe.columns}
        dataframe.to_sql(
            name=table_name,
            con=conn,
            if_exists=write,
            index=False,
            dtype=dict_types,
        )

        duration = time.time() - start_time
        Logger.warning(message=f"Data sent successfully. Duration: {duration}")
