import os
import boto3
import kagglehub
import shutil

# Configurar cache en /tmp para Lambda
os.environ['XDG_CACHE_HOME'] = '/tmp'

S3_BUCKET = os.environ['S3_BUCKET_NAME']
s3_client = boto3.client('s3')

DATASETS = [
    "rahuldabholkar/world-of-stadiums",
    "imtkaggleteam/football-stadiums",
    "antimoni/football-stadiums"
]

def upload_directory_to_s3(local_path, s3_prefix):
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            # Calcular ruta relativa para mantener estructura si es necesario
            s3_key = os.path.join(s3_prefix, file)
            print(f"Subiendo {local_file} a s3://{S3_BUCKET}/{s3_key}")
            s3_client.upload_file(local_file, S3_BUCKET, s3_key)

def handler(event, context):
    try:
        for dataset in DATASETS:
            print(f"Descargando {dataset}...")
            # Descarga a /tmp/...
            path = kagglehub.dataset_download(dataset)
            
            # Definir prefijo en S3 (ej: raw/world-of-stadiums/2023-10-01/)
            dataset_name = dataset.split('/')[-1]
            s3_prefix = f"raw/{dataset_name}"
            
            upload_directory_to_s3(path, s3_prefix)
            
        return {"statusCode": 200, "body": "Ingesta completada"}
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e
