# %%
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, sum, array, when
from datetime import datetime, timedelta
from dotenv import load_dotenv
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# %%
spark = SparkSession.builder \
    .appName("Movies_change_update") \
    .master("spark://spark:7077") \
    .config("spark.executor.memory", "6g")  \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.hadoop.fs.s3a.directory.operations.purge.uploads", "true") \
    .getOrCreate()

# %%
def log_null_counts(df, df_name):
    """Conta a quantidade de valores nulos por coluna em um DataFrame"""
    logger.info(f"Verificando nulos em {df_name}...")
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
    for column in df.columns:
        logger.info(f"Coluna {column}: {null_counts[column]} nulos")

def handle_nulls(df):
    """Realiza o tratamento de nulos para todas as colunas"""
    logger.info("Iniciando tratamento de nulos...")
    
    # Remove registros com ID e titles nulos
    initial_count = df.count()
    df = df.filter(col("id").isNotNull() & col("title").isNotNull())
    logger.info(f"Removidos {initial_count - df.count()} registros com ID nulo")

    # Preenche valores nulos para cada coluna
    fill_values = {
        'overview': 'Sem informação.',
        'release_date': '1900-01-01',
        'vote_average': '0.0'
    }
    
    df = df.na.fill(fill_values)
    
    # Trata array vazio para genre_ids
    df = df.withColumn("genre_ids", 
        when(col("genre_ids").isNull(), array().cast(ArrayType(IntegerType())))
        .otherwise(col("genre_ids")))
    
    logger.info("Tratamento de nulos concluído")
    return df


# %%
try:
    logger.info("Iniciando processamento...")
    
    # Leitura dos dados
    logger.info("Lendo dados da camada bronze...")
    df_change = spark.read.parquet("s3a://bronze/movies_changes/")
    logger.info("Lendo dados da camada silver...")
    df_70_26 = spark.read.parquet("s3a://silver/movies_70_26/").cache()
    
    # Log inicial de nulos
    log_null_counts(df_change, "Raw Changes")
    log_null_counts(df_70_26, "Raw Silver")
    
    # Tratamento de nulos
    logger.info("Processando dados da camada bronze...")
    df_change = handle_nulls(df_change)
    
    logger.info("Processando dados da camada silver...")
    df_70_26 = handle_nulls(df_70_26)
    
    # Log pós-tratamento
    log_null_counts(df_change, "Processed Changes")
    log_null_counts(df_70_26, "Processed Silver")

    # Processamento principal
    logger.info("Realizando join e merge...")
    df_filtered = df_70_26.join(df_change, on="id", how="left_anti")
    df_updated = df_filtered.unionByName(df_change)
    
    # Verificação
    logger.info(f"Registros originais: {df_70_26.count()}")
    logger.info(f"Registros de mudança: {df_change.count()}")
    logger.info(f"Registros atualizados: {df_updated.count()}")
    
    # Verificação final de nulos
    log_null_counts(df_updated, "Dataset Final")
    df_updated.show(10, truncate=False)

    # Escrita dos dados
    logger.info("Escrevendo dados atualizados...")
    (df_updated 
    .repartition(50)
    .write
    .option("parquet.block.size", 256 * 1024 * 1024) # 256MB por bloco
    .parquet("s3a://silver/movies_70_26/", mode="overwrite"))
    
    # Verificação final
    logger.info("Verificando escrita...")
    df_read = spark.read.parquet("s3a://silver/movies_70_26/")
    logger.info(f"Total de registros escritos: {df_read.count()}")
    df_read.show(5, truncate=False)

except Exception as e:
    logger.error(f"Erro durante o processamento: {str(e)}")
    raise

finally:
    logger.info("Encerrando sessão Spark...")
    spark.stop()

logger.info("Processo concluído com sucesso!")



# %%
