import pandas as pd

from dotenv import load_dotenv
from libs.module import Module
from libs.logger import Logger

load_dotenv()

func = Module()


def main(database: str, dims: list):
    """_summary_

    Args:
        database (str): banco de dados a ser usado para salvar os dados extraídos
        dims (list): lista com as dimensões a serem extraídas das API
    """
    for dim, pag_true_false in dims:
        try:
            list_json = func.get_restful_api(dim, pag=pag_true_false)
            dfs = []
            for pos_json in range(len(list_json)):
                df_ = pd.DataFrame(list_json[pos_json])
                dfs.append(df_)

            df_final = pd.concat(dfs, ignore_index=True).astype(str)

            dim_name = dim[0].lower() + dim[1:]

            func.load_dataframe_raw_zone(
                db=database,
                dataframe=df_final,
                table_name=f"{dim_name}_raw",
                write="replace",
            )

            Logger.warning(message=f"Table created: {dim}_raw")

        except Exception as excp:
            Logger.error(message=f"Error: {excp}")


if __name__ == "__main__":
    DATABASE = "postgres_advw"
    list_dim = [
        ["Persons", 1],
        ["BusinessEntityContacts", 0],
        ["ContactTypes", 0],
        ["Emails", 1],
        ["PhoneNumbers", 1],
        ["PhoneNumberTypes", 0],
    ]

    Logger.info(message="Extracting data from API AdventureWorks...")
    main(DATABASE, list_dim)
    Logger.sucess(message="Extraction complete!")

# Persons = persons
# BusinessEntityContacts - businessEntityContacts - NAO TEM
# ContactTypes - contactTypes - NAO TEM
# EmailAddresses - emails
# PersonPhones - phoneNumbers
# PhoneNumberTypes - phoneNumberTypes - NAO TEM
