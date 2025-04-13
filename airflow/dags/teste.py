from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 12),
    "retries": 1, 
    "retry_delay": timedelta(minutes=10),  # Tempo maior para recuperação
    "email": ["fattetv@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,  # Agora notifica também em novas tentativas
}

dag = DAG(
    "update_mov_silver_v2",
    default_args=default_args,
    schedule_interval="@daily",  # Sugestão de agendamento diário
    catchup=False,
    max_active_runs=2,
)

MINIO_ROOT_USER = Variable.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = Variable.get("MINIO_ROOT_PASSWORD")

spark_task = SparkSubmitOperator(
    task_id="process_movie_updates",
    application="/opt/bitnami/spark/scripts/movies/movies_change_update.py",
    conn_id="spark_default",
    verbose=False,  # Reduz logs desnecessários
    driver_memory="6g",  # Aumento de memória
    executor_memory="8g",
    packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",  # Versões alinhadas
    conf={
        "spark.hadoop.fs.s3a.access.key": MINIO_ROOT_USER,
        "spark.hadoop.fs.s3a.secret.key": MINIO_ROOT_PASSWORD,
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.committer.name": "magic",
        "spark.sql.parquet.output.committer.class": "org.apache.hadoop.mapreduce.lib.output.DirectParquetOutputCommitter",
        "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"
    },
    dag=dag,
)