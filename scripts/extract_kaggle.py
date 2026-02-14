import os
import boto3
from kaggle.api.kaggle_api_extended import KaggleApi

# Configuración
DATASET = 'nombre-del-dataset/en-kaggle' # Ej: 'netflix-inc/netflix-movies'
FILE_NAME = 'data.csv'
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

def download_and_upload():
    # 1. Autenticación Kaggle (usa vars de entorno KAGGLE_USERNAME y KAGGLE_KEY)
    api = KaggleApi()
    api.authenticate()

    # 2. Descargar
    print(f"Descargando {DATASET}...")
    api.dataset_download_files(DATASET, path='./temp', unzip=True)

    # 3. Subir a S3
    s3 = boto3.client('s3')
    local_path = f"./temp/{FILE_NAME}"
    s3_key = f"raw/{FILE_NAME}"
    
    print(f"Subiendo a s3://{BUCKET_NAME}/{s3_key}...")
    s3.upload_file(local_path, BUCKET_NAME, s3_key)
    print("¡Carga completada!")

if __name__ == "__main__":
    download_and_upload()
