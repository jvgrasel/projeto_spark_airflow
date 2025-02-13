import os
import calendar
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from datetime import datetime, timedelta
def load_env_variables():
    """Carrega as variáveis de ambiente do arquivo .env."""
    return {
        "TMDB_API_TOKEN": os.getenv("TMDB_API_TOKEN"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
        "MINIO_SERVER": os.getenv("MINIO_SERVER")
    }
def validate_env_variables(env_vars):
    """Valida se as variáveis de ambiente foram carregadas corretamente."""
    for key, value in env_vars.items():
        if not value:
            raise ValueError(f"ERRO: {key} não encontrado. Verifique seu arquivo .env.")
def create_spark_session(minio_config):
    """Cria e retorna uma SparkSession configurada para conexão com MinIO."""
    return SparkSession.builder \
        .appName("Movies_20_21") \
        .master("spark://spark:7077") \
        .config("spark.executor.memory", "12g")  \
        .config("spark.executor.cores", "2") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_config["MINIO_SERVER"]) \
        .config("spark.hadoop.fs.s3a.access.key", minio_config["MINIO_ROOT_USER"]) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_config["MINIO_ROOT_PASSWORD"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
def fetch_movies_by_date(api_token, start_date, end_date):
    """Busca os filmes lançados entre start_date e end_date da API TMDB."""
    url = "https://api.themoviedb.org/3/discover/movie"
    headers = {"accept": "application/json", "Authorization": f"Bearer {api_token}"}
    page = 1
    movies = []
    
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
        
        if response.status_code != 200:
            print(f"Erro ao acessar API ({start_date}):", response.status_code, response.text)
            break
        
        data = response.json()
        results = data.get("results", [])
        if not results:
            break
        
        movies.extend(results)
        
        if page >= data.get("total_pages", 1):
            break
        
        page += 1
    
    return movies
def fetch_movies_for_range(api_token, start_year, end_year):
    """Busca filmes para todos os dias dentro do intervalo de anos especificado."""
    movies_list = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            first_day = datetime(year, month, 1)
            last_day = datetime(year, month, calendar.monthrange(year, month)[1])
            current_day = first_day
            
            while current_day <= last_day:
                movies_list.extend(fetch_movies_by_date(api_token, current_day.strftime("%Y-%m-%d"), current_day.strftime("%Y-%m-%d")))
                current_day += timedelta(days=1)
    
    return movies_list
def create_spark_dataframe(spark, movies_list):
    """Cria um DataFrame do PySpark com os dados dos filmes."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("vote_average", StringType(), True),
        StructField("genre_ids", ArrayType(IntegerType()), True)
    ])
    return spark.createDataFrame(movies_list, schema=schema) if movies_list else None
def write_to_minio(df):
    """Escreve os dados no MinIO no formato Parquet."""
    if df:
        df.write \
            .format("parquet") \
            .mode("overwrite") \
            .save("s3a://bronze/movie_20_21/")
        print("Escrita no MinIO concluída com sucesso!")
    else:
        print("Nenhum dado para escrever no MinIO.")
def main():
    """Função principal para executar o pipeline de ETL."""
    env_vars = load_env_variables()
    validate_env_variables(env_vars)
    
    spark = create_spark_session(env_vars)
    
    movies_list = fetch_movies_for_range(env_vars["TMDB_API_TOKEN"], 2020, 2021)
    df = create_spark_dataframe(spark, movies_list)
    write_to_minio(df)
    
    spark.stop()
def handler():
    """Ponto de entrada para execução."""
    try:
        main()
    except Exception as e:
        print(f"Erro na execução: {e}")
if __name__ == "__main__":
    handler()
