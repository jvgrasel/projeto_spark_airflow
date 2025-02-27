import os
import time
import calendar
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")

if not TMDB_API_TOKEN:
    raise ValueError("ERRO: Token da API do TMDB não encontrado.")

# Configuração do Spark
spark = SparkSession.builder \
    .appName("TVShows_70_79") \
    .master("spark://spark:7077") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Configuração da API
url = "https://api.themoviedb.org/3/discover/tv"
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_API_TOKEN}"
}

# Testar conexão com a API
test_response = requests.get("https://api.themoviedb.org/3/configuration", headers=headers)
if test_response.status_code != 200:
    raise Exception(f"Falha na conexão com TMDB API: {test_response.text}")

tv_shows_list = []

# Schema para programas de TV
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

# Intervalo de anos
start_year = 1970
end_year = 1979

for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        first_day = datetime(year, month, 1)
        last_day = datetime(year, month, calendar.monthrange(year, month)[1])
        
        current_day = first_day
        while current_day <= last_day:
            start_date = current_day.strftime("%Y-%m-%d")
            end_date = start_date
            logger.info(f"Coletando dados para {start_date}...")

            page = 1
            while True:
                params = {
                    "include_adult": "false",
                    "include_null_first_air_dates": "false",
                    "language": "en-US",
                    "page": page,
                    "sort_by": "popularity.desc",
                    "first_air_date.gte": start_date,
                    "first_air_date.lte": end_date
                }

                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    data = response.json()
                    tv_shows = data.get("results", [])

                    if not tv_shows:
                        break

                    # Filtra apenas os campos necessários
                    filtered_shows = []
                    for show in tv_shows:
                        filtered_shows.append({
                            "genre_ids": show.get("genre_ids", []),
                            "id": show.get("id"),
                            "origin_country": show.get("origin_country", []),
                            "original_language": show.get("original_language"),
                            "original_name": show.get("original_name"),
                            "overview": show.get("overview"),
                            "first_air_date": show.get("first_air_date"),
                            "name": show.get("name"),
                            "vote_average": show.get("vote_average")
                        })

                    tv_shows_list.extend(filtered_shows)

                    total_pages = data.get("total_pages", 1)
                    if page >= total_pages:
                        break
                    page += 1
                else:
                    logger.error(f"Erro na requisição ({start_date} - Página {page}): {response.status_code}")
                    break
                
                time.sleep(0.01)
            
            current_day += timedelta(days=1)

logger.info(f"Total de programas de TV coletados: {len(tv_shows_list)}")

# Criar DataFrame
df = spark.createDataFrame(tv_shows_list, schema=schema) if tv_shows_list else spark.createDataFrame([], schema)

# Escrever no MinIO
df.write \
  .format("parquet") \
  .mode("overwrite") \
  .save("s3a://bronze/tv_shows_70_79/")

spark.stop()