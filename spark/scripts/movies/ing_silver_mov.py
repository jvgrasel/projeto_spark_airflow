# %%
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType  # Importação adicionada
from pyspark.sql.functions import col, sum, array, when
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# %%
spark = SparkSession.builder \
    .appName("Movies_70_26") \
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
    
    # Remove registros com ID nulo
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
    logger.info("Iniciando processo de ingestão Bronze -> Silver...")
    
    # Leitura dos dados
    logger.info("Lendo arquivos da camada bronze...")
    df = spark.read.parquet("s3a://bronze/movie_*")
    
    # Log inicial
    logger.info(f"Total de registros brutos: {df.count()}")
    log_null_counts(df, "Dataset Bruto")
    
    # Tratamento de nulos
    df_clean = handle_nulls(df)
    
    # Deduplicação
    logger.info("Removendo duplicatas...")
    initial_count = df_clean.count()
    df_deduplicado = df_clean.dropDuplicates(["id"])
    final_count = df_deduplicado.count()
    
    logger.info(f"Registros removidos por duplicidade: {initial_count - final_count}")
    log_null_counts(df_deduplicado, "Dataset Processado")
    
    # Escrita dos dados
    logger.info("Escrevendo na camada silver...")
    (df_deduplicado
        .repartition(50)  # Controla o número de arquivos de saída
        .write
        .option("parquet.block.size", 256 * 1024 * 1024)  # 256MB por bloco
        .parquet("s3a://silver/movies_70_26/", mode="overwrite"))
    
    # # Verificação final
    # logger.info("Verificando escrita...")
    # df_read = spark.read.parquet("s3a://silver/movies_70_26/")
    # logger.info(f"Total de registros escritos: {df_read.count()}")
    # df_read.show(5, truncate=False)

except Exception as e:
    logger.error(f"Erro durante o processamento: {str(e)}")
    raise

finally:
    logger.info("Encerrando sessão Spark...")
    spark.stop()

logger.info("Ingestão concluída com sucesso!")


