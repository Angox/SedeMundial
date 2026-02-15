import os
import boto3
import kagglehub
import pandas as pd
import io
import glob

# Configurar cache para Lambda (solo /tmp es escribible)
os.environ['XDG_CACHE_HOME'] = '/tmp'

S3_BUCKET = os.environ['S3_BUCKET_NAME']
s3_client = boto3.client('s3')

DATASETS = [
    "rahuldabholkar/world-of-stadiums",
    "imtkaggleteam/football-stadiums",
    "antimoni/football-stadiums"
]

def analyze_and_upload(local_path, s3_prefix):
    """Analiza archivos locales y los sube a S3 con logs detallados."""
    # Buscamos todos los archivos en la ruta descargada
    files = glob.glob(f"{local_path}/**", recursive=True)
    
    for local_file in files:
        if os.path.isfile(local_file):
            s3_key = os.path.join(s3_prefix, os.path.basename(local_file))
            
            print(f"--- 📄 Procesando archivo: {os.path.basename(local_file)} ---")
            
            # Si es un CSV, lo inspeccionamos antes de subir
            if local_file.endswith('.csv'):
                try:
                    df_temp = pd.read_csv(local_file)
                    print(f"📊 [INSPECCIÓN] Dimensiones: {df_temp.shape[0]} filas x {df_temp.shape[1]} columnas")
                    print(f"📋 [COLUMNAS]: {df_temp.columns.tolist()}")
                except Exception as e:
                    print(f"⚠️ No se pudo pre-visualizar el CSV: {e}")

            print(f"📤 Subiendo a s3://{S3_BUCKET}/{s3_key}...")
            s3_client.upload_file(local_file, S3_BUCKET, s3_key)

def handler(event, context):
    """Función 1: Ingesta desde Kaggle"""
    try:
        print(f"🚀 Iniciando descarga de {len(DATASETS)} datasets de Kaggle...")
        for dataset in DATASETS:
            print(f"\n⬇️ Descargando {dataset}...")
            path = kagglehub.dataset_download(dataset)
            
            dataset_name = dataset.split('/')[-1]
            s3_prefix = f"raw/{dataset_name}"
            
            # Analizamos y subimos
            analyze_and_upload(path, s3_prefix)
            
        return {"statusCode": 200, "body": "Ingesta y análisis completado exitosamente"}
    except Exception as e:
        print(f"❌ ERROR en Ingestor: {str(e)}")
        raise e

def cleaner_handler(event, context):
    """Función 2: Limpieza y Transformación (Triggered por S3)"""
    try:
        record = event['Records'][0]
        key = record['s3']['object']['key']
        
        if not key.endswith('.csv') or 'raw/' not in key:
            return {"statusCode": 200, "body": "Ignorado: No es un CSV en /raw"}

        print(f"🧹 Iniciando ETL para: {key}")
        
        # 1. Leer de S3
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        df = pd.read_csv(obj['Body'])
        
        # LOGS DE PROCESAMIENTO
        filas_orig, cols_orig = df.shape
        print(f"📥 Datos cargados. Dimensiones originales: {filas_orig} filas, {cols_orig} columnas")
        
        # 2. Limpieza
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        
        filas_final, _ = df.shape
        filas_eliminadas = filas_orig - filas_final
        
        print(f"✨ Limpieza completada:")
        print(f"   - Filas eliminadas (nulos/duplicados): {filas_eliminadas}")
        print(f"   - Dimensiones finales: {df.shape}")
        
        # 3. Conversión a Parquet
        print(f"📦 Convirtiendo a Parquet...")
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        
        # 4. Guardar
        clean_key = key.replace("raw/", "clean/").replace(".csv", ".parquet")
        s3_client.put_object(Bucket=S3_BUCKET, Key=clean_key, Body=parquet_buffer.getvalue())
        
        print(f"✅ Archivo procesado y guardado en: {clean_key}")
        return {"statusCode": 200, "body": f"Procesado: {clean_key}"}

    except Exception as e:
        print(f"❌ ERROR en Limpieza: {str(e)}")
        raise e
