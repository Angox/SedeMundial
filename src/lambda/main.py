import os
import boto3
import kagglehub
import pandas as pd
import io

# Configurar cache
os.environ['XDG_CACHE_HOME'] = '/tmp'

S3_BUCKET = os.environ['S3_BUCKET_NAME']
s3_client = boto3.client('s3')

# --- FUNCIÓN 1: INGESTA (Ya la tenías) ---
DATASETS = [
    "rahuldabholkar/world-of-stadiums",
    "imtkaggleteam/football-stadiums",
    "antimoni/football-stadiums"
]

def upload_directory_to_s3(local_path, s3_prefix):
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            s3_key = os.path.join(s3_prefix, file)
            print(f"Subiendo {local_file} a s3://{S3_BUCKET}/{s3_key}")
            s3_client.upload_file(local_file, S3_BUCKET, s3_key)

def handler(event, context):
    try:
        for dataset in DATASETS:
            print(f"Descargando {dataset}...")
            path = kagglehub.dataset_download(dataset)
            dataset_name = dataset.split('/')[-1]
            # Guardamos en /raw
            s3_prefix = f"raw/{dataset_name}"
            upload_directory_to_s3(path, s3_prefix)
        return {"statusCode": 200, "body": "Ingesta completada"}
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e

# --- FUNCIÓN 2: LIMPIEZA (Reemplazo de Glue) ---
def cleaner_handler(event, context):
    try:
        # Detectar qué archivo se subió desde el evento de S3
        record = event['Records'][0]
        key = record['s3']['object']['key']
        
        print(f"Procesando archivo: {key}")
        
        # Solo procesar CSVs en la carpeta raw/
        if not key.endswith('.csv') or 'raw/' not in key:
            return {"statusCode": 200, "body": "No es un CSV o no está en raw/"}

        # 1. Leer CSV desde S3
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(obj['Body'])
        
        # 2. Limpieza BÁSICA (Ejemplo: quitar duplicados y nulos)
        print(f"Filas originales: {len(df)}")
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        print(f"Filas limpias: {len(df)}")
        
        # 3. Convertir a Parquet (Más rápido para Redshift)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        
        # 4. Guardar en carpeta /clean
        clean_key = key.replace("raw/", "clean/").replace(".csv", ".parquet")
        
        print(f"Guardando en s3://{S3_BUCKET}/{clean_key}")
        s3_client.put_object(Bucket=S3_BUCKET, Key=clean_key, Body=parquet_buffer.getvalue())
        
        return {"statusCode": 200, "body": f"Limpieza exitosa: {clean_key}"}

    except Exception as e:
        print(f"Error en limpieza: {str(e)}")
        raise e
