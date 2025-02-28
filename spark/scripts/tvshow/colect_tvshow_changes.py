import os
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# %%
# Carregar variáveis de ambiente
load_dotenv()
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")

if not TMDB_API_TOKEN:
    raise ValueError("Token da API TMDB não encontrado no .env")

# Configurar Spark
spark = SparkSession.builder \
    .appName("TMDB_TV_Daily_Changes") \
    .master("spark://spark:7077") \
    .config("spark.executor.memory", "6g")  \
    .config("spark.executor.cores", "1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# %%
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_API_TOKEN}"
}

# Schema para TV
schema = StructType([
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("id", IntegerType(), True),
    StructField("origin_country", ArrayType(StringType()), True),
    StructField("original_language", StringType(), True),
    StructField("original_name", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("first_air_date", StringType(), True),
    StructField("name", StringType(), True),
    StructField("vote_average", DoubleType(), True)
])

# %%
def get_last_10_days():
    today = datetime.now()
    return [today - timedelta(days=i) for i in range(10, 0, -1)]

def fetch_changed_ids(target_date):
    changed_ids = set()
    page = 1
    total_pages = 1
    date_str = target_date.strftime("%Y-%m-%d")
    
    logger.info(f"Coletando dados para: {date_str}")
    
    while page <= total_pages:
        try:
            url = f"https://api.themoviedb.org/3/tv/changes?start_date={date_str}&end_date={date_str}&page={page}"
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                logger.error(f"Erro na página {page}: {response.text}")
                break

            data = response.json()
            total_pages = data.get("total_pages", 1)
            
            logger.info(f"Data: {date_str} - Página {page}/{total_pages}")
            
            changed_ids.update(item['id'] for item in data.get("results", []))
            
            total_pages = min(total_pages, 500)
            
            page += 1
            time.sleep(0.25)
            
        except Exception as e:
            logger.error(f"Erro na página {page}: {str(e)}")
            break

    logger.info(f"IDs únicos coletados para {date_str}: {len(changed_ids)}")
    return changed_ids

def fetch_tv_details(tv_id):
    try:
        url = f"https://api.themoviedb.org/3/tv/{tv_id}?language=en-US"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "genre_ids": [g["id"] for g in data.get("genres", [])],
                "id": data.get("id"),
                "origin_country": data.get("origin_country", []),
                "original_language": data.get("original_language"),
                "original_name": data.get("original_name"),
                "overview": data.get("overview"),
                "first_air_date": data.get("first_air_date"),
                "name": data.get("name"),
                "vote_average": data.get("vote_average", 0.0)
            }
        else:
            logger.error(f"Erro {response.status_code} na série {tv_id}")
            return None
            
    except Exception as e:
        logger.error(f"Erro na série {tv_id}: {str(e)}")
        return None

# %%
def main():
    all_ids = set()
    all_tv_shows = []
    
    # Fase 1: Coleta de IDs
    for day in get_last_10_days():
        daily_ids = fetch_changed_ids(day)
        all_ids.update(daily_ids)
        logger.info(f"Total acumulado após {day.strftime('%Y-%m-%d')}: {len(all_ids)}")
    
    logger.info(f"Total final de IDs únicos: {len(all_ids)}")
    
    # Fase 2: Detalhes
    for idx, tv_id in enumerate(all_ids, 1):
        if tv_data := fetch_tv_details(tv_id):
            all_tv_shows.append(tv_data)
            if idx % 100 == 0:
                logger.info(f"Processados {idx}/{len(all_ids)} IDs")
        time.sleep(0.01)
    
    if all_tv_shows:
        df = spark.createDataFrame(all_tv_shows, schema=schema)
        df.show(5)
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save("s3a://bronze/tv_changes/")
        logger.info(f"Dados salvos. Total de registros: {len(all_tv_shows)}")
    else:
        logger.warning("Nenhum dado para salvar")

if __name__ == "__main__":
    main()
    spark.stop()