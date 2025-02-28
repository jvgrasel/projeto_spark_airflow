from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from datetime import datetime, timedelta


# %%
spark = SparkSession.builder \
    .appName("Movies_70_26") \
    .master("spark://spark:7077") \
    .config("spark.executor.memory", "6g")  \
    .config("spark.executor.cores", "1") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# %%
# Lê todos os diretórios que começam com 'movie_' na camada bronze
df = spark.read.parquet("s3a://bronze/movie_*")

# Remove linhas com IDs duplicados mantendo a primeira ocorrência
df_deduplicado = df.dropDuplicates(["id"])

# %%
# Escreve no MinIO
df_deduplicado.write \
  .format("parquet") \
  .mode("overwrite") \
  .save("s3a://silver/movies_70_26/")


spark.stop()


