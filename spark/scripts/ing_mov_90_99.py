# %%
import os
import time
import calendar
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# %%

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Obter o token da API do ambiente
TMDB_API_TOKEN = os.getenv("TMDB_API_TOKEN")

if not TMDB_API_TOKEN:
    raise ValueError("ERRO: Token da API do TMDB não encontrado. Verifique seu arquivo .env.")

MINIO_SERVER = "http://minio:9000"

# Configuração do Spark Session com MinIO
spark = SparkSession.builder \
    .appName("Movies_90_99") \
    .master("spark://spark:7077") \
    .config("spark.executor.memory", "8g")  \
    .config("spark.executor.cores", "1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()


# %%
# URL e headers da API
url = "https://api.themoviedb.org/3/discover/movie"
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_API_TOKEN}"
}
# Testar conexão com a API
test_response = requests.get("https://api.themoviedb.org/3/configuration", headers=headers)
if test_response.status_code != 200:
    raise Exception(f"Falha na conexão com TMDB API: {test_response.text}")

# Lista para armazenar os dados de todas as páginas
movies_list = []

# Define o intervalo de anos de 1990 a 1999
start_year = 1990
end_year = 1999
# Loop para cada ano de 1990 a 1999
for year in range(start_year, end_year + 1):
    # Loop para cada mês (de janeiro a dezembro)
    for month in range(1, 13):
        # Define o primeiro e o último dia do mês
        first_day = datetime(year, month, 1)
        last_day = datetime(year, month, calendar.monthrange(year, month)[1])
        
        # Loop para cada dia do mês
        current_day = first_day
        while current_day <= last_day:
            start_date = current_day.strftime("%Y-%m-%d")
            end_date = start_date  # Busca apenas um dia por vez
            logger.info(f"Coletando dados para {start_date}...")


            page = 1

            while True:
                params = {
                    "include_adult": "false",
                    "include_video": "false",
                    "language": "en-US",
                    "page": page,
                    "sort_by": "popularity.desc",
                    "primary_release_date.gte": start_date,
                    "primary_release_date.lte": end_date
                }

                response = requests.get(url, headers=headers, params=params)

                if response.status_code == 200:
                    data = response.json()
                    movies = data.get("results", [])

                    if not movies:  # Se não houver mais filmes, sai do loop
                        break

                    movies_list.extend(movies)  # Adiciona os filmes à lista

                    total_pages = data.get("total_pages", 1)  # Obtém o número total de páginas

                    if page >= total_pages:  # Se atingirmos a última página, encerramos o loop
                        break

                    page += 1  # Incrementa a página para a próxima requisição
                else:
                    print(f"Erro ao acessar API ({year}-{month}-{current_day.day} - Página {page}):", response.status_code, response.text)
                    break
                # Adiciona o delay de 1 segundo entre as requisições
                time.sleep(0.25)

            # Avança para o próximo dia
            current_day += timedelta(days=1)
logger.info(f"Total de filmes coletados: {len(movies_list)}")
# Definição do schema para o DataFrame do PySpark
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("vote_average", StringType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True)
])

# Criando o DataFrame '
df = spark.createDataFrame(movies_list, schema=schema) if movies_list else spark.createDataFrame([], schema)
# Escreve no MinIO
df.write \
  .format("parquet") \
  .mode("overwrite") \
  .save("s3a://bronze/movie_90_99/")

spark.stop()


