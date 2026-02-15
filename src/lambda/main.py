import os

# --- CONFIGURACIÓN CRÍTICA PARA LAMBDA ---
# Forzamos todo a /tmp para evitar errores de permisos de escritura
os.environ['HOME'] = '/tmp'
os.environ['KAGGLEHUB_CACHE'] = '/tmp'
os.environ['XDG_CACHE_HOME'] = '/tmp'
# -----------------------------------------

import boto3
import kagglehub
import pandas as pd
import io
import glob
import shutil
import time
import unicodedata
import concurrent.futures # IMPORTANTE: Para ejecución en paralelo

# Inicializamos clientes fuera de los handlers para reutilizar conexiones
s3_client = boto3.client('s3')
location_client = boto3.client('location')

S3_BUCKET = os.environ['S3_BUCKET_NAME']

# Configuración precisa por dataset
DATASETS_CONFIG = {
    "rahuldabholkar/world-of-stadiums": {
        "s3_folder": "rahuldabholkar_world-of-stadiums",
        "file_filter": "all_stadiums.csv"
    },
    "imtkaggleteam/football-stadiums": {
        "s3_folder": "imtkaggleteam_football-stadiums",
        "file_filter": None
    },
    "antimoni/football-stadiums": {
        "s3_folder": "antimoni_football-stadiums",
        "file_filter": None
    }
}

# ==========================================
# FUNCIONES DE INGESTA (EXTRACCIÓN)
# ==========================================

def analyze_file(local_file):
    """Intenta leer el CSV para imprimir info, probando varias codificaciones."""
    if not local_file.endswith('.csv'):
        return

    encodings = ['utf-8', 'latin-1', 'cp1252', 'ISO-8859-1']
    
    for enc in encodings:
        try:
            df_temp = pd.read_csv(local_file, encoding=enc, nrows=3)
            print(f"   📊 [INSPECCIÓN - {enc}] Cols: {len(df_temp.columns)} | Ej: {df_temp.columns.tolist()}")
            return
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"   ⚠️ No se pudo leer el CSV: {e}")
            return
    print("   ❌ Fallaron todos los intentos de lectura (encoding desconocido).")

def upload_directory_to_s3(local_path, s3_folder_name, specific_file=None):
    """Sube archivos recursivamente, respetando filtros."""
    files = glob.glob(f"{local_path}/**", recursive=True)
    
    for local_file in files:
        if os.path.isfile(local_file):
            filename = os.path.basename(local_file)
            
            if specific_file and filename != specific_file:
                continue
            
            print(f"--- 📄 Procesando: {filename} ---")
            analyze_file(local_file)
            
            s3_key = f"raw/{s3_folder_name}/{filename}"
            print(f"   📤 Subiendo a: s3://{S3_BUCKET}/{s3_key}")
            s3_client.upload_file(local_file, S3_BUCKET, s3_key)

def handler(event, context):
    """Función Lambda para INGESTA (Descarga de Kaggle -> S3)"""
    try:
        print(f"🚀 Iniciando Ingesta Controlada...")
        
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
                continue
            
        return {"statusCode": 200, "body": "Ingesta Selectiva Completada"}
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO INGESTA: {str(e)}")
        raise e

# ==========================================
# FUNCIONES DE PROCESAMIENTO (ETL & GEO)
# ==========================================

def read_csv_from_s3_robust(bucket, key):
    """Lectura robusta de CSV desde S3 probando encodings."""
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'windows-1250', 'ISO-8859-1']
    
    for encoding in encodings:
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj['Body'], encoding=encoding)
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"   ⚠️ Error leyendo {key} con {encoding}: {e}")
            continue

    print(f"   ⚠️ Advertencia: Forzando lectura de {key} con reemplazo de caracteres.")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'], encoding='utf-8', encoding_errors='replace')

def normalize_text(text):
    """Normaliza texto para comparaciones (minusculas, sin acentos)."""
    if pd.isna(text): return ""
    text = str(text).lower().strip()
    return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")

def get_coordinates_aws(stadium, city, country, index_name):
    """
    Obtiene coordenadas usando AWS Location Service.
    Es mucho más rápido y no requiere sleeps.
    """
    text = f"{stadium}, {city}, {country}"
    
    try:
        # Intento 1: Búsqueda precisa
        response = location_client.search_place_index_for_text(
            IndexName=index_name,
            Text=text,
            MaxResults=1
        )
        
        if response['Results']:
            point = response['Results'][0]['Place']['Geometry']['Point']
            # AWS devuelve [Longitud, Latitud], nosotros queremos Lat, Lon
            return point[1], point[0]
            
        # Intento 2: Solo Ciudad y País (Fallback)
        fallback_text = f"{city}, {country}"
        response = location_client.search_place_index_for_text(
            IndexName=index_name,
            Text=fallback_text,
            MaxResults=1
        )
        
        if response['Results']:
            point = response['Results'][0]['Place']['Geometry']['Point']
            return point[1], point[0]
            
    except Exception as e:
        print(f"   ⚠️ Error geolocalizando '{text}': {e}")
        pass
        
    return None, None

def cleaner_handler(event, context):
    """Función Lambda para LIMPIEZA y GEOLOCALIZACIÓN"""
    try:
        print("⚽ Iniciando Procesamiento (Lectura Robusta + AWS Location Service)...")
        
        # Recuperamos el nombre del índice desde variables de entorno (Terraform)
        place_index_name = os.environ.get('PLACE_INDEX')
        if not place_index_name:
            # Fallback por si acaso no se pasó la variable, aunque debería fallar
            place_index_name = "stadiums-place-index" 

        sources = [
            {'key': 'raw/rahuldabholkar_world-of-stadiums/all_stadiums.csv', 'type': 'rahul'},
            {'key': 'raw/imtkaggleteam_football-stadiums/Football Stadiums.csv', 'type': 'imtk'},
            {'key': 'raw/antimoni_football-stadiums/Football Stadiums.csv', 'type': 'antimoni'}
        ]

        dfs = []
        
        # 1. Carga
        for source in sources:
            try:
                print(f"📖 Leyendo: {source['key']}...")
                df = read_csv_from_s3_robust(S3_BUCKET, source['key'])
                
                # Estandarizar columnas
                if source['type'] == 'rahul':
                    if 'sport_played' in df.columns:
                        df = df[df['sport_played'].str.contains('Football|Soccer', case=False, na=False)]
                    df = df.rename(columns={'stadium_name': 'Stadium', 'location': 'City', 'country': 'Country', 'total_capacity': 'Capacity'})
                
                df = df[['Stadium', 'City', 'Country', 'Capacity']].copy()
                
                # Limpiar Capacidad
                df['Capacity'] = df['Capacity'].astype(str).str.replace(',', '').str.extract(r'(\d+)')[0]
                df['Capacity'] = pd.to_numeric(df['Capacity'], errors='coerce').fillna(0).astype(int)
                
                dfs.append(df)
            except Exception as e:
                print(f"❌ Error fatal leyendo {source['key']}: {e}")

        if not dfs:
            return {"statusCode": 500, "body": "No hay datos."}

        # 2. Fusión y Deduplicación
        full_df = pd.concat(dfs, ignore_index=True)
        print(f"📊 Total bruto: {len(full_df)}")
        
        full_df['norm_stadium'] = full_df['Stadium'].apply(normalize_text)
        full_df['norm_city'] = full_df['City'].apply(normalize_text)
        
        full_df = full_df.drop_duplicates(subset=['norm_stadium', 'norm_city'])
        print(f"📉 Tras eliminar duplicados: {len(full_df)}")
        
        # 3. FILTRO FIFA (>40k)
        candidates_df = full_df[full_df['Capacity'] >= 40000].copy()
        candidates_df = candidates_df.drop(columns=['norm_stadium', 'norm_city'])
        
        count_candidates = len(candidates_df)
        print(f"🏆 Candidatos finales (>40k): {count_candidates}")

        # 4. Geocodificación PARALELA con AWS Location Service
        print(f"🌍 Buscando coordenadas en paralelo usando '{place_index_name}'...")
        
        # Listas para almacenar resultados en orden
        lats = [None] * count_candidates
        lons = [None] * count_candidates
        
        # Convertimos DataFrame a lista de diccionarios para iterar
        rows = candidates_df.to_dict('records')
        
        # Usamos ThreadPoolExecutor para lanzar múltiples peticiones a la vez
        # max_workers=10 permite hacer 10 búsquedas simultáneas
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            # Enviamos todas las tareas
            future_to_index = {
                executor.submit(
                    get_coordinates_aws, 
                    row['Stadium'], 
                    row['City'], 
                    row['Country'], 
                    place_index_name
                ): i for i, row in enumerate(rows)
            }
            
            completed_count = 0
            # Recogemos los resultados a medida que llegan
            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    lat, lon = future.result()
                    lats[index] = lat
                    lons[index] = lon
                except Exception as exc:
                    print(f"   ⚠️ Excepción en worker {index}: {exc}")
                
                completed_count += 1
                if completed_count % 20 == 0:
                    print(f"   ... procesados {completed_count}/{count_candidates}")

        candidates_df['Latitude'] = lats
        candidates_df['Longitude'] = lons

        # 5. Guardar (Parquet y CSV)
        final_df = candidates_df.dropna(subset=['Latitude'])
        
        clean_key = "clean/world_cup_candidates.parquet"
        parquet_buffer = io.BytesIO()
        final_df.to_parquet(parquet_buffer, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=clean_key, Body=parquet_buffer.getvalue())
        
        print(f"✅ ANÁLISIS COMPLETADO. Parquet guardado en: s3://{S3_BUCKET}/{clean_key}")
        print(f"   Estadios finales geolocalizados: {len(final_df)}")

        return {"statusCode": 200, "body": f"Proceso OK. {len(final_df)} candidatos geolocalizados."}
        
    except Exception as e:
        print(f"❌ Error Fatal: {str(e)}")
        raise e
