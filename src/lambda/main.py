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
import unicodedata
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

def read_csv_from_s3_robust(bucket, key):
    """Intenta leer un CSV de S3 probando múltiples codificaciones."""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'ISO-8859-1']
    
    for encoding in encodings:
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj['Body'], encoding=encoding)
        except UnicodeDecodeError:
            continue # Prueba el siguiente
        except Exception as e:
            print(f"   ⚠️ Error leyendo {key} con {encoding}: {e}")
            raise e
    raise ValueError(f"❌ No se pudo leer {key} con ninguna codificación conocida.")

def normalize_text(text):
    """Normaliza texto para comparaciones (minusculas, sin acentos)."""
    if pd.isna(text): return ""
    text = str(text).lower().strip()
    # Quitar acentos (ej: 'Málaga' -> 'malaga')
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    return text

def get_coordinates(stadium, city, country, geolocator):
    """Obtiene coordenadas con manejo de errores."""
    query = f"{stadium}, {city}, {country}"
    try:
        # Timeout generoso para la API
        location = geolocator.geocode(query, timeout=10)
        if location:
            return location.latitude, location.longitude
        # Reintento solo con ciudad
        location = geolocator.geocode(f"{city}, {country}", timeout=10)
        if location:
            return location.latitude, location.longitude
    except:
        pass
    return None, None

def cleaner_handler(event, context):
    try:
        print("⚽ Iniciando Procesamiento Unificado (Modo Seguro)...")
        
        # 1. Definir fuentes
        sources = [
            {'key': 'raw/rahuldabholkar_world-of-stadiums/all_stadiums.csv', 'type': 'rahul'},
            {'key': 'raw/imtkaggleteam_football-stadiums/Football Stadiums.csv', 'type': 'imtk'},
            {'key': 'raw/antimoni_football-stadiums/Football Stadiums.csv', 'type': 'antimoni'}
        ]

        dfs = []
        
        # 2. Carga y Estandarización
        for source in sources:
            try:
                print(f"📖 Leyendo: {source['key']}...")
                df = read_csv_from_s3_robust(S3_BUCKET, source['key'])
                
                # Estandarizar columnas a: Stadium, City, Country, Capacity
                if source['type'] == 'rahul':
                    if 'sport_played' in df.columns:
                        df = df[df['sport_played'].str.contains('Football|Soccer', case=False, na=False)]
                    df = df.rename(columns={'stadium_name': 'Stadium', 'location': 'City', 'country': 'Country', 'total_capacity': 'Capacity'})
                
                # Seleccionar solo columnas clave
                df = df[['Stadium', 'City', 'Country', 'Capacity']].copy()
                
                # Limpiar Capacidad (quitar comas)
                df['Capacity'] = df['Capacity'].astype(str).str.replace(',', '').str.extract(r'(\d+)')[0]
                df['Capacity'] = pd.to_numeric(df['Capacity'], errors='coerce').fillna(0).astype(int)
                
                dfs.append(df)
            except Exception as e:
                print(f"⚠️ Saltando {source['key']} por error: {e}")

        if not dfs:
            return {"statusCode": 500, "body": "No hay datos para procesar."}

        # 3. Fusión y Deduplicación
        full_df = pd.concat(dfs, ignore_index=True)
        print(f"📊 Total bruto: {len(full_df)}")
        
        # Crear columnas temporales normalizadas para identificar duplicados reales
        full_df['norm_stadium'] = full_df['Stadium'].apply(normalize_text)
        full_df['norm_city'] = full_df['City'].apply(normalize_text)
        
        # Eliminar duplicados basados en nombre y ciudad normalizados
        full_df = full_df.drop_duplicates(subset=['norm_stadium', 'norm_city'])
        print(f"📉 Tras eliminar duplicados: {len(full_df)}")
        
        # 4. Filtro FIFA (>40k)
        candidates_df = full_df[full_df['Capacity'] >= 40000].copy()
        # Limpiar columnas temp
        candidates_df = candidates_df.drop(columns=['norm_stadium', 'norm_city'])
        print(f"🏆 Candidatos finales (>40k): {len(candidates_df)}")

        # 5. Geocodificación con "Salvavidas" (Time-Aware)
        geolocator = Nominatim(user_agent="wc_analyser_v2")
        lats, lons = [], []
        
        print("🌍 Buscando coordenadas...")
        processed_count = 0
        
        for index, row in candidates_df.iterrows():
            # VERIFICACIÓN DE TIEMPO RESTANTE
            # Si quedan menos de 20 segundos (20000 ms), paramos para guardar
            remaining_ms = context.get_remaining_time_in_millis()
            if remaining_ms < 20000:
                print(f"⚠️ TIEMPO AGOTADO ({remaining_ms}ms restantes). Guardando progreso parcial...")
                lats.extend([None] * (len(candidates_df) - len(lats)))
                lons.extend([None] * (len(candidates_df) - len(lons)))
                break

            lat, lon = get_coordinates(row['Stadium'], row['City'], row['Country'], geolocator)
            lats.append(lat)
            lons.append(lon)
            
            processed_count += 1
            if processed_count % 10 == 0:
                print(f"   ... {processed_count} procesados")
            
            time.sleep(1.1) # Respetar API

        candidates_df['Latitude'] = lats
        candidates_df['Longitude'] = lons

        # 6. Guardar (Parquet y CSV)
        # Filtramos los que no tienen coordenadas para el dataset final limpio
        final_df = candidates_df.dropna(subset=['Latitude'])
        
        clean_key = "clean/world_cup_candidates.parquet"
        parquet_buffer = io.BytesIO()
        final_df.to_parquet(parquet_buffer, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=clean_key, Body=parquet_buffer.getvalue())
        
        print(f"✅ Guardado exitoso: {clean_key} ({len(final_df)} estadios con coordenadas)")
        return {"statusCode": 200, "body": "Proceso completado"}
        
    except Exception as e:
        print(f"❌ Error Fatal: {str(e)}")
        raise e
