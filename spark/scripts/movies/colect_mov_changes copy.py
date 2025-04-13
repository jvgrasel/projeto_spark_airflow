# %%
import os
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
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

MINIO_SERVER = "http://minio:9000"

# Configurar Spark
spark = SparkSession.builder \
    .appName("TMDB_Daily_Changes") \
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

# Schema simplificado
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("vote_average", StringType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True)
])

# %%
def get_last_10_days():
    today = datetime.now()
    return [today - timedelta(days=i) for i in range(3, 0, -1)]

def fetch_changed_ids(target_date):
    changed_ids = set()  # Alterado para set para evitar duplicatas
    page = 1
    total_pages = 1
    date_str = target_date.strftime("%Y-%m-%d")
    
    logger.info(f"Coletando dados para: {date_str}")
    
    while page <= total_pages:
        try:
            url = f"https://api.themoviedb.org/3/movie/changes?start_date={date_str}&end_date={date_str}&page={page}"
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                logger.error(f"Erro na página {page}: {response.text}")
                break

            data = response.json()
            total_pages = data.get("total_pages", 1)
            
            logger.info(f"Data: {date_str} - Página {page}/{total_pages}")
            
            # Adiciona IDs diretamente ao set
            changed_ids.update(item['id'] for item in data.get("results", []))
            
            # Controle de segurança para limite de páginas
            total_pages = min(total_pages, 500)
            
            page += 1
            time.sleep(0.25)
            
        except Exception as e:
            logger.error(f"Erro na página {page}: {str(e)}")
            break

    logger.info(f"IDs únicos coletados para {date_str}: {len(changed_ids)}")
    return changed_ids
def fetch_movie_details(movie_id):
    try:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            return {
                "id": data.get("id"),
                "title": data.get("title"),
                "overview": data.get("overview"),
                "release_date": data.get("release_date"),
                "vote_average": str(data.get("vote_average", 0.0)),
                "genre_ids": [g["id"] for g in data.get("genres", [])]
            }
        else:
            logger.error(f"Erro {response.status_code} no filme {movie_id}")
            return None
            
    except Exception as e:
        logger.error(f"Erro no filme {movie_id}: {str(e)}")
        return None

# %%
def main():
    all_ids = set()  # Set global para armazenar todos os IDs únicos
    all_movies = []
    
    # Fase 1: Coleta de todos os IDs únicos
    for day in get_last_10_days():
        daily_ids = fetch_changed_ids(day)
        all_ids.update(daily_ids)
        logger.info(f"Total acumulado após {day.strftime('%Y-%m-%d')}: {len(all_ids)}")
    
    logger.info(f"Total final de IDs únicos: {len(all_ids)}")
    
    # Fase 2: Processamento dos detalhes
    for idx, movie_id in enumerate(all_ids, 1):
        if movie_data := fetch_movie_details(movie_id):
            all_movies.append(movie_data)
            if idx % 100 == 0:  # Log a cada 100 processamentos
                logger.info(f"Processados {idx}/{len(all_ids)} IDs")
        time.sleep(0.01)
    
    if all_movies:
        df = spark.createDataFrame(all_movies, schema=schema)
        df.show(5)
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save("s3a://bronze/movies_changes/")
        logger.info(f"Dados salvos. Total de registros: {len(all_movies)}")
    else:
        logger.warning("Nenhum dado para salvar")

if __name__ == "__main__":
    main()
    spark.stop()