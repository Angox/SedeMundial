import os

# --- CONFIGURACIÓN CRÍTICA PARA LAMBDA ---
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
import concurrent.futures
import math

# Inicializamos clientes
s3_client = boto3.client('s3')
location_client = boto3.client('location')

S3_BUCKET = os.environ['S3_BUCKET_NAME']

# Configuración de Datasets
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

# Diccionario de corrección (Sigue siendo útil para la búsqueda inicial)
COUNTRY_MAPPING = {
    'United States of America': 'United States',
    'USA': 'United States',
    'US': 'United States',
    'United Mexican States': 'Mexico',
    'Argentine Republic': 'Argentina',
    'French Republic': 'France',
    'Italian Republic': 'Italy',
    'Republic of South Africa': 'South Africa',
    'Türkiye': 'Turkey',
    'Burma': 'Myanmar',
    'New Zeland': 'New Zealand',
    'DPR Korea': 'North Korea',
    'Korea': 'South Korea',
    'England': 'United Kingdom',
    'Scotland': 'United Kingdom',
    'Wales': 'United Kingdom'
}

# ==========================================
# 1. FUNCIONES DE INGESTA
# ==========================================

def upload_directory_to_s3(local_path, s3_folder_name, specific_file=None):
    files = glob.glob(f"{local_path}/**", recursive=True)
    for local_file in files:
        if os.path.isfile(local_file):
            filename = os.path.basename(local_file)
            if specific_file and filename != specific_file:
                continue
            
            s3_key = f"raw/{s3_folder_name}/{filename}"
            print(f"   📤 Subiendo: {filename} -> s3://{S3_BUCKET}/{s3_key}")
            s3_client.upload_file(local_file, S3_BUCKET, s3_key)

def handler(event, context):
    try:
        print(f"🚀 Iniciando Ingesta...")
        if os.path.exists("/tmp/datasets"):
            shutil.rmtree("/tmp/datasets", ignore_errors=True)

        for dataset_handle, config in DATASETS_CONFIG.items():
            print(f"\n⬇️ Descargando: {dataset_handle}...")
            try:
                path = kagglehub.dataset_download(dataset_handle)
                upload_directory_to_s3(path, config['s3_folder'], config['file_filter'])
            except Exception as e:
                print(f"❌ Error en {dataset_handle}: {e}")
                continue 
            
        return {"statusCode": 200, "body": "Ingesta Completada"}
    except Exception as e:
        print(f"❌ FATAL ERROR: {str(e)}")
        raise e

# ==========================================
# 2. FUNCIONES DE PROCESAMIENTO Y LIMPIEZA
# ==========================================

def read_csv_from_s3_robust(bucket, key):
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'ISO-8859-1']
    for encoding in encodings:
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj['Body'], encoding=encoding)
        except Exception:
            continue
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'], encoding='utf-8', encoding_errors='replace')

def standardize_country(country_name):
    if pd.isna(country_name): return "Unknown"
    name = str(country_name).strip()
    return COUNTRY_MAPPING.get(name, name)

def normalize_text(text):
    if pd.isna(text): return ""
    text = str(text).lower().strip()
    remove_words = ['stadium', 'estadio', 'stadion', 'arena', 'fc', 'club']
    for word in remove_words:
        text = text.replace(word, '')
    text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
    return text.strip()

def get_coordinates_and_enrich(stadium, city, country, index_name):
    """
    Obtiene Coordenadas + ISO Country + Region + Dirección Oficial
    Devuelve un diccionario con los datos enriquecidos.
    """
    text = f"{stadium}, {city}, {country}"
    
    # Función auxiliar para procesar la respuesta de AWS
    def extract_data(resp):
        if not resp['Results']: return None
        
        place = resp['Results'][0]['Place']
        point = place['Geometry']['Point'] # [Lon, Lat]
        
        return {
            'lat': point[1],
            'lon': point[0],
            'iso_country': place.get('Country', None), # Ej: "ESP", "USA"
            'region': place.get('Region', None),       # Ej: "Madrid", "California"
            'official_address': place.get('Label', None) # Ej: "Santiago Bernabéu, Av. Concha Espina..."
        }

    try:
        # Intento 1: Búsqueda completa
        response = location_client.search_place_index_for_text(
            IndexName=index_name, Text=text, MaxResults=1
        )
        data = extract_data(response)
        if data: return data
            
        # Intento 2: Fallback (Solo ciudad)
        response = location_client.search_place_index_for_text(
            IndexName=index_name, Text=f"{city}, {country}", MaxResults=1
        )
        data = extract_data(response)
        if data: return data
            
    except Exception:
        pass
        
    # Si todo falla, devolvemos vacíos
    return {'lat': None, 'lon': None, 'iso_country': None, 'region': None, 'official_address': None}

def spatial_deduplication(df, distance_threshold_deg=0.003):
    print("📍 Iniciando Deduplicación Geoespacial...")
    
    def name_quality(name):
        return -100 if '\ufffd' in str(name) or '?' in str(name) else len(str(name))

    df['name_score'] = df['Stadium'].apply(name_quality)
    df = df.sort_values(by=['Capacity', 'name_score'], ascending=[False, False])
    
    kept_indices = []
    seen_coords = []
    
    for idx, row in df.iterrows():
        lat, lon = row['Latitude'], row['Longitude']
        if pd.isna(lat) or pd.isna(lon): continue
            
        is_duplicate = False
        for slat, slon in seen_coords:
            dist = math.sqrt((lat - slat)**2 + (lon - slon)**2)
            if dist < distance_threshold_deg:
                is_duplicate = True
                break
        
        if not is_duplicate:
            kept_indices.append(idx)
            seen_coords.append((lat, lon))
            
    return df.loc[kept_indices].drop(columns=['name_score'])

def cleaner_handler(event, context):
    try:
        print("⚽ Iniciando Pipeline ETL + Geo Enriquecido...")
        place_index = os.environ.get('PLACE_INDEX', 'stadiums-place-index')

        # 1. Carga de Datos
        sources = [
            {'key': 'raw/rahuldabholkar_world-of-stadiums/all_stadiums.csv', 'type': 'rahul'},
            {'key': 'raw/imtkaggleteam_football-stadiums/Football Stadiums.csv', 'type': 'imtk'},
            {'key': 'raw/antimoni_football-stadiums/Football Stadiums.csv', 'type': 'antimoni'}
        ]

        dfs = []
        for src in sources:
            try:
                df = read_csv_from_s3_robust(S3_BUCKET, src['key'])
                col_map = {'stadium_name': 'Stadium', 'location': 'City', 'country': 'Country', 'total_capacity': 'Capacity'}
                df = df.rename(columns={k: v for k,v in col_map.items() if k in df.columns})
                
                if src['type'] == 'rahul' and 'sport_played' in df.columns:
                    df = df[df['sport_played'].str.contains('Football|Soccer', case=False, na=False)]
                
                if set(['Stadium', 'City', 'Capacity']).issubset(df.columns):
                    df['Capacity'] = df['Capacity'].astype(str).str.replace(',', '').str.extract(r'(\d+)')[0]
                    df['Capacity'] = pd.to_numeric(df['Capacity'], errors='coerce').fillna(0).astype(int)
                    if 'Country' in df.columns:
                        df['Country'] = df['Country'].apply(standardize_country)
                    dfs.append(df[['Stadium', 'City', 'Country', 'Capacity']])
            except Exception as e:
                print(f"⚠️ Error leyendo {src['key']}: {e}")

        if not dfs: return {"statusCode": 500, "body": "No Data"}

        # 2. Fusión y Pre-filtrado
        full_df = pd.concat(dfs, ignore_index=True)
        candidates = full_df[full_df['Capacity'] >= 40000].copy()
        
        # Deduplicación preliminar
        candidates['norm_name'] = candidates['Stadium'].apply(normalize_text)
        candidates['norm_country'] = candidates['Country'].apply(normalize_text)
        candidates = candidates.drop_duplicates(subset=['norm_name', 'norm_country'])
        candidates = candidates.drop(columns=['norm_name', 'norm_country'])
        
        print(f"🏆 Candidatos a geolocalizar y enriquecer: {len(candidates)}")

        # 3. GEOCODIFICACIÓN + ENRIQUECIMIENTO
        print("🌍 Geolocalizando en paralelo...")
        rows = candidates.to_dict('records')
        enriched_results = [None] * len(rows)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_map = {
                executor.submit(
                    get_coordinates_and_enrich, 
                    r['Stadium'], r['City'], r.get('Country',''), place_index
                ): i for i, r in enumerate(rows)
            }
            
            for future in concurrent.futures.as_completed(future_map):
                idx = future_map[future]
                try:
                    enriched_results[idx] = future.result()
                except:
                    enriched_results[idx] = {'lat': None, 'lon': None} # Fallback vacío

        # 4. Asignación de nuevas columnas enriquecidas
        candidates['Latitude'] = [r['lat'] for r in enriched_results]
        candidates['Longitude'] = [r['lon'] for r in enriched_results]
        candidates['ISO_Country'] = [r['iso_country'] for r in enriched_results]
        candidates['Region'] = [r['region'] for r in enriched_results]
        candidates['Official_Address'] = [r['official_address'] for r in enriched_results]

        # Filtrar no encontrados
        candidates = candidates.dropna(subset=['Latitude'])

        # 5. Deduplicación Geoespacial
        final_df = spatial_deduplication(candidates)
        
        print(f"✅ Estadios finales enriquecidos: {len(final_df)}")

        # 6. Guardar
        clean_key = "clean/world_cup_candidates.parquet"
        buf = io.BytesIO()
        final_df.to_parquet(buf, index=False)
        s3_client.put_object(Bucket=S3_BUCKET, Key=clean_key, Body=buf.getvalue())
        
        return {"statusCode": 200, "body": f"OK. {len(final_df)} estadios enriquecidos."}

    except Exception as e:
        print(f"❌ Error: {e}")
        raise e
