# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from datetime import datetime, timedelta

# %%
spark = SparkSession.builder \
    .appName("Movies_change_update") \
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
# Lê o Parquet que contêm as atualizações e os filmes a serem atualizados
df_change = spark.read.parquet("s3a://bronze/movies_changes/")
df_70_26 = spark.read.parquet("s3a://silver/movies_70_26/").cache()

# Remove os registros do df_70_26 que possuem ID presente no df_change
df_filtered = df_70_26.join(df_change, on="id", how="left_anti")

# Adiciona os registros atualizados do df_change ao df_filtered
df_updated = df_filtered.unionByName(df_change)

# %%
# Escreve no MinIO
df_updated.write \
  .format("parquet") \
  .mode("overwrite") \
  .save("s3a://silver/movies_70_26/")

spark.stop()




