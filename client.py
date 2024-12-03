from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import DataLakeDirectoryClient
from dotenv import load_dotenv
import os

load_dotenv()

CONN_STR_MOLINA = os.getenv("CONN_STR_MOLINA")
dl_service_client = DataLakeServiceClient.from_connection_string(CONN_STR_MOLINA)

def storage_exists(storage_name):
    return dl_service_client.get_file_system_client(storage_name).exists()


def directory_exists(storage_name, dir_name):
    if storage_exists(storage_name):
        return DataLakeDirectoryClient.from_connection_string(CONN_STR_MOLINA, storage_name, dir_name).exists()
    else:
        return False

