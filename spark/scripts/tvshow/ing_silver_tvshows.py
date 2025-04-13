import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum, array, when
from pyspark.sql.functions import coalesce


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração da Spark Session
spark = SparkSession.builder \
    .appName("TVShows_Silver_90_99") \
    .master("spark://spark:7077") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .getOrCreate()

# Funções de apoio 
def log_null_counts(df, df_name):
    """Conta a quantidade de valores nulos por coluna em um DataFrame"""
    logger.info(f"Verificando nulos em {df_name}...")
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
    for column in df.columns:
        logger.info(f"Coluna {column}: {null_counts[column]} nulos")

def handle_nulls(df):
    """Tratamento completo para TV Shows"""
    logger.info("Iniciando tratamento de nulos...")
    
    # Preenchimento mútuo entre name e original_name
    df = df.withColumn("temp_name", coalesce(col("name"), col("original_name")))
    df = df.withColumn("temp_original", coalesce(col("original_name"), col("name")))
    
    # Remove colunas originais e renomeia
    df = df.drop("name", "original_name") \
           .withColumnRenamed("temp_name", "name") \
           .withColumnRenamed("temp_original", "original_name")
    
    # Filtra registros problemáticos
    initial_count = df.count()
    df = df.filter(
        col("id").isNotNull() & 
        ~(col("name").isNull() & col("original_name").isNull())
    )
    logger.info(f"Removidos {initial_count - df.count()} registros inválidos")
    
    # Preenche outros valores nulos
    fill_config = {
        'overview': 'Sem informação.',
        'first_air_date': '1900-01-01',
        'vote_average': 0.0,
        'original_language': 'en'
    }
    
    df = df.na.fill(fill_config)
    
    # Garante arrays vazios para origin_country
    df = df.withColumn("origin_country",
    when(col("origin_country").isNull(), array().cast(ArrayType(StringType())))
    .otherwise(col("origin_country")))
    
    # Trata array vazio para genre_ids
    df = df.withColumn("genre_ids", 
        when(col("genre_ids").isNull(), array().cast(ArrayType(IntegerType())))
        .otherwise(col("genre_ids")))
    
    logger.info("Tratamento de nulos concluído")
    return df

# Processamento principal
try:
    logger.info("Iniciando processo Bronze -> Silver para TV Shows...")
    
    # Leitura dos dados brutos
    logger.info("Lendo dados da camada Bronze...")
    df = spark.read.parquet("s3a://bronze/tv_shows_*/")
    
    # Log inicial
    logger.info(f"Total de registros brutos: {df.count()}")
    log_null_counts(df, "Dataset Bruto")
    
    # Tratamento de dados
    df_clean = handle_nulls(df)
    
    # Deduplicação
    logger.info("Removendo duplicatas...")
    df_deduplicado = df_clean.dropDuplicates(["id"])
    logger.info(f"Registros após deduplicação: {df_deduplicado.count()}")
    
    # Log final
    log_null_counts(df_deduplicado, "Dataset Processado")
    
    # Escrita na Silver
    logger.info("Escrevendo na camada Silver...")
    (df_deduplicado
        .repartition(50)
        .write
        .parquet("s3a://silver/tv_shows_70_26/", 
                mode="overwrite",
                compression="snappy"))
    
    # # Verificação final
    logger.info("Verificando escrita...")
    df_read = spark.read.parquet("s3a://silver/tv_shows_70_26/")
    logger.info(f"Total de registros escritos: {df_read.count()}")
    df_read.show(5, truncate=False)
    
except Exception as e:
    logger.error(f"Erro durante o processamento: {str(e)}")
    raise

finally:
    logger.info("Encerrando sessão Spark...")
    spark.stop()

logger.info("Processo concluído com sucesso!")