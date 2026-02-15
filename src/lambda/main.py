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
import io
import glob
import shutil
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

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

# --- LÓGICA DE PROCESAMIENTO Y ANÁLISIS FIFA ---

def get_coordinates(stadium, city, country, geolocator):
    """Intenta obtener coordenadas con reintentos para evitar timeouts."""
    query = f"{stadium}, {city}, {country}"
    try:
        # Timeout alto y sleep para respetar límites de la API gratuita (1 req/seg)
        location = geolocator.geocode(query, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            # Intento secundario: solo ciudad y país si falla el estadio exacto
            location = geolocator.geocode(f"{city}, {country}", timeout=10)
            if location:
                return location.latitude, location.longitude
            return None, None
    except (GeocoderTimedOut, Exception) as e:
        print(f"   ⚠️ Error geocodificando {query}: {e}")
        return None, None

def cleaner_handler(event, context):
    try:
        print("⚽ Iniciando Análisis de Candidatos a Copa del Mundo...")

        # 1. Definir los archivos que esperamos encontrar en S3
        files_to_process = [
            {'key': 'raw/rahuldabholkar_world-of-stadiums/all_stadiums.csv', 'type': 'rahul'},
            {'key': 'raw/imtkaggleteam_football-stadiums/Football Stadiums.csv', 'type': 'imtk'},
            {'key': 'raw/antimoni_football-stadiums/Football Stadiums.csv', 'type': 'antimoni'}
        ]

        dfs = []
        
        # 2. Leer y Normalizar cada dataset
        for item in files_to_process:
            try:
                print(f"📖 Leyendo: {item['key']}...")
                obj = s3_client.get_object(Bucket=S3_BUCKET, Key=item['key'])
                
                # Leemos con 'latin-1' si falla 'utf-8' (común en datasets antiguos)
                try:
                    df = pd.read_csv(obj['Body'], encoding='utf-8')
                except:
                    obj = s3_client.get_object(Bucket=S3_BUCKET, Key=item['key']) # Re-abrir stream
                    df = pd.read_csv(obj['Body'], encoding='latin-1')

                # Normalización de Columnas
                if item['type'] == 'rahul':
                    # Filtrar solo fútbol
                    if 'sport_played' in df.columns:
                        df = df[df['sport_played'].str.contains('Football|Soccer', case=False, na=False)]
                    
                    df = df.rename(columns={
                        'stadium_name': 'Stadium', 'location': 'City', 
                        'country': 'Country', 'total_capacity': 'Capacity'
                    })
                    df = df[['Stadium', 'City', 'Country', 'Capacity']]

                elif item['type'] in ['imtk', 'antimoni']:
                    # Estos ya vienen con nombres parecidos
                    df = df[['Stadium', 'City', 'Country', 'Capacity']]

                # Limpieza de Capacidad (quitar comas y convertir a número)
                df['Capacity'] = df['Capacity'].astype(str).str.replace(',', '').str.extract('(\d+)')[0]
                df['Capacity'] = pd.to_numeric(df['Capacity'], errors='coerce').fillna(0).astype(int)

                dfs.append(df)
            
            except Exception as e:
                print(f"⚠️ No se pudo procesar {item['key']}: {e}")
                # Continuamos con los que sí pudimos leer

        if not dfs:
            return {"statusCode": 500, "body": "No se pudieron cargar datasets."}

        # 3. Unificar (Merge)
        full_df = pd.concat(dfs, ignore_index=True)
        print(f"📊 Total estadios crudos: {len(full_df)}")

        # Eliminar duplicados exactos (mismo nombre y ciudad)
        full_df.drop_duplicates(subset=['Stadium', 'City'], keep='first', inplace=True)
        
        # 4. FILTRO FIFA: Capacidad >= 40,000
        # Requisito oficial: 40k (fase grupos), 60k (semis), 80k (final)
        FIFA_MIN_CAPACITY = 40000
        
        candidates_df = full_df[full_df['Capacity'] >= FIFA_MIN_CAPACITY].copy()
        print(f"🏆 Estadios candidatos (>40k): {len(candidates_df)} (de {len(full_df)} originales)")

        # 5. Geocodificación (Solo a los candidatos para ahorrar tiempo)
        print("🌍 Buscando coordenadas (esto puede tardar unos minutos)...")
        geolocator = Nominatim(user_agent="my_world_cup_analyser_v1")
        
        # Iteramos y aplicamos geocoding con pausa pequeña
        lats = []
        lons = []
        
        for index, row in candidates_df.iterrows():
            lat, lon = get_coordinates(row['Stadium'], row['City'], row['Country'], geolocator)
            lats.append(lat)
            lons.append(lon)
            # Pausa de seguridad para no saturar la API
            time.sleep(1.1) 
            
            if index % 10 == 0:
                print(f"   ... procesados {index + 1} estadios")

        candidates_df['Latitude'] = lats
        candidates_df['Longitude'] = lons
        
        # Filtrar los que no encontramos coordenadas (opcional)
        candidates_df.dropna(subset=['Latitude', 'Longitude'], inplace=True)

        # 6. Guardar Resultado Final
        clean_key = "clean/world_cup_candidates.parquet"
        
        # Guardamos en Parquet (mejor rendimiento)
        parquet_buffer = io.BytesIO()
        candidates_df.to_parquet(parquet_buffer, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=clean_key, Body=parquet_buffer.getvalue())

        # Guardamos también un CSV para que puedas verlo fácil
        csv_key = "clean/world_cup_candidates.csv"
        csv_buffer = io.StringIO()
        candidates_df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=csv_key, Body=csv_buffer.getvalue().encode('utf-8'))

        print(f"✅ ANÁLISIS COMPLETADO. Archivo guardado en: {clean_key}")
        
        return {
            "statusCode": 200, 
            "body": f"Procesamiento exitoso. {len(candidates_df)} candidatos encontrados."
        }

    except Exception as e:
        print(f"❌ Error CRÍTICO en Cleaner: {str(e)}")
        raise e
