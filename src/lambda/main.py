import os

# --- CONFIGURACIÓN CRÍTICA PARA LAMBDA ---
# Forzamos todo a /tmp
os.environ['HOME'] = '/tmp'
os.environ['KAGGLEHUB_CACHE'] = '/tmp'
os.environ['XDG_CACHE_HOME'] = '/tmp'
# -----------------------------------------

import boto3
import kagglehub
import pandas as pd
import glob
import shutil

S3_BUCKET = os.environ['S3_BUCKET_NAME']
s3_client = boto3.client('s3')

# Configuración precisa por dataset
# format: "kaggle_handle": {"s3_folder": "nombre_carpeta", "file_filter": "nombre_archivo_exacto_o_None"}
DATASETS_CONFIG = {
    "rahuldabholkar/world-of-stadiums": {
        "s3_folder": "rahuldabholkar_world-of-stadiums",
        "file_filter": "all_stadiums.csv" # Solo queremos este archivo
    },
    "imtkaggleteam/football-stadiums": {
        "s3_folder": "imtkaggleteam_football-stadiums",
        "file_filter": None # Queremos todo (solo trae uno)
    },
    "antimoni/football-stadiums": {
        "s3_folder": "antimoni_football-stadiums",
        "file_filter": None # Queremos todo
    }
}

def analyze_file(local_file):
    """Intenta leer el CSV para imprimir info, probando varias codificaciones."""
    if not local_file.endswith('.csv'):
        return

    # Lista de encodings para probar (utf-8 falla con caracteres raros de estadios europeos/latinos)
    encodings = ['utf-8', 'latin-1', 'cp1252', 'ISO-8859-1']
    
    for enc in encodings:
        try:
            # Leemos solo 3 filas para ser rápidos y no gastar memoria
            df_temp = pd.read_csv(local_file, encoding=enc, nrows=3)
            print(f"   📊 [INSPECCIÓN - {enc}] Cols: {len(df_temp.columns)} | Ej: {df_temp.columns.tolist()}")
            return # Éxito, salimos
        except UnicodeDecodeError:
            continue # Probamos el siguiente encoding
        except Exception as e:
            print(f"   ⚠️ No se pudo leer el CSV: {e}")
            return

    print("   ❌ Fallaron todos los intentos de lectura (encoding desconocido).")

def upload_directory_to_s3(local_path, s3_folder_name, specific_file=None):
    """Sube archivos recursivamente, respetando filtros y evitando colisiones."""
    files = glob.glob(f"{local_path}/**", recursive=True)
    
    for local_file in files:
        if os.path.isfile(local_file):
            filename = os.path.basename(local_file)
            
            # --- FILTRADO ---
            # Si hay filtro definido y el archivo no coincide, lo saltamos
            if specific_file and filename != specific_file:
                # print(f"   ⏭️ Saltando archivo no deseado: {filename}")
                continue
            
            # --- INSPECCIÓN ---
            print(f"--- 📄 Procesando: {filename} ---")
            analyze_file(local_file)
            
            # --- SUBIDA ---
            # Estructura: raw / nombre_dataset_unico / archivo.csv
            s3_key = f"raw/{s3_folder_name}/{filename}"
            print(f"   📤 Subiendo a: s3://{S3_BUCKET}/{s3_key}")
            s3_client.upload_file(local_file, S3_BUCKET, s3_key)

def handler(event, context):
    try:
        print(f"🚀 Iniciando Ingesta Controlada...")
        
        # Limpiar /tmp para asegurar espacio si se reusa la lambda
        if os.path.exists("/tmp/datasets"):
            shutil.rmtree("/tmp/datasets", ignore_errors=True)

        for dataset_handle, config in DATASETS_CONFIG.items():
            print(f"\n⬇️ Descargando: {dataset_handle}...")
            
            try:
                path = kagglehub.dataset_download(dataset_handle)
                
                upload_directory_to_s3(
                    local_path=path, 
                    s3_folder_name=config['s3_folder'], 
                    specific_file=config['file_filter']
                )
                
            except Exception as e:
                print(f"❌ Error descargando {dataset_handle}: {e}")
                # No lanzamos raise aquí para que intente descargar los otros si uno falla
                continue
            
        return {"statusCode": 200, "body": "Ingesta Selectiva Completada"}
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        raise e

# --- SE MANTIENE EL CLEANER IGUAL (pero no se ejecutará si no hay trigger) ---
def cleaner_handler(event, context):
    # (El código del cleaner se queda igual que antes, 
    #  esperando el trigger de S3 para funcionar)
    pass
