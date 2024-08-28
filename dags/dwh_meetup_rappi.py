"""
dwh_meetup_rappi
DAG auto-generated by Astro Cloud IDE.
"""

from airflow.decorators import dag
from astro import sql as aql
import pandas as pd
import pendulum

import os
import logging
from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import zipfile
import tempfile

@aql.dataframe(task_id="download_files")
def download_files_func():
    import os
    import logging
    import zipfile
    from kaggle.api.kaggle_api_extended import KaggleApi
    
    def download_and_extract_files(dataset, download_path='/tmp'):
        try:
            # Configuración de logging
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
            # Autenticación con la API de Kaggle
            api = KaggleApi()
            api.authenticate()
    
            # Listar todos los archivos del dataset
            files = api.dataset_list_files(dataset).files
            logging.info(f"Archivos encontrados en el dataset {dataset}: {[file.name for file in files]}")
    
            # Lista para almacenar los archivos descargados
            downloaded_files = []
    
            # Descargar y descomprimir cada archivo
            for file in files:
                file_name = file.name
                
                # Excluir el archivo members.csv
                if file_name == 'members.csv':
                    logging.info(f"El archivo {file_name} ha sido excluido de la descarga.")
                    continue
    
                logging.info(f"Descargando el archivo {file_name} desde el dataset {dataset}")
                api.dataset_download_file(dataset, file_name, path=download_path)
    
                # Verificar si el archivo descargado es un ZIP
                zip_file_path = os.path.join(download_path, f"{file_name}.zip")
                csv_file_path = os.path.join(download_path, file_name)
    
                if os.path.exists(zip_file_path):
                    logging.info(f"Descomprimiendo el archivo {zip_file_path}")
                    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                        zip_ref.extractall(download_path)
                    logging.info(f"El archivo {zip_file_path} se descomprimió correctamente.")
    
                    # Eliminar el archivo ZIP después de la descompresión
                    os.remove(zip_file_path)
                    logging.info(f"El archivo ZIP {zip_file_path} se eliminó después de descomprimirlo.")
    
                    # Agregar el archivo CSV a la lista de archivos descargados
                    downloaded_files.append(csv_file_path)
    
                # Si ya se descargó como CSV directamente
                elif os.path.exists(csv_file_path):
                    logging.info(f"El archivo {file_name} se descargó correctamente en {csv_file_path}")
                    downloaded_files.append(csv_file_path)
    
                else:
                    raise FileNotFoundError(f"El archivo {file_name} no se descargó correctamente.")
    
            logging.info("Todos los archivos del dataset se han descargado y descomprimido correctamente.")
            return downloaded_files
    
        except Exception as e:
            logging.error(f"Error al descargar y descomprimir los archivos del dataset {dataset}: {e}")
            raise
    
    # Ejemplo de uso
    downloaded_files = download_and_extract_files('megelon/meetup')
    print("Archivos descargados:", downloaded_files)
    return downloaded_files

@aql.dataframe(task_id="list_dir")
def list_dir_func():
    directory ='/tmp'
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    entries_info = []
    
    with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_dir():
                    entry_info = f'Directory: {entry.name}'
                elif entry.is_file():
                    entry_info = f'File: {entry.name}'
                
                entries_info.append(entry_info)
                logging.info(entry_info)
    
        # Join the list into a single string with newline characters
    return '\n'.join(entries_info)
    
    
    

default_args={
    "owner": "oscar andres morales perez,Open in Cloud IDE",
}

@dag(
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=pendulum.from_format("2024-08-27", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "oscar andres morales perez": "mailto:omoralesperez@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm06zmrq309x401n6211h3ngz/cloud-ide/cm0b3z9670sry01n5ylkjp5ca/cm0cd5wv80zqu01n57sdg733k",
    },
)
def dwh_meetup_rappi():
    download_files = download_files_func()

    list_dir = list_dir_func()

    list_dir << download_files

dag_obj = dwh_meetup_rappi()
