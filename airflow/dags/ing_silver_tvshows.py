from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable  # Importar para ler variáveis
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # Tempo entre as tentativas
    "email": ["fattetv@gmail.com"],  # Lista de destinatários
    "email_on_failure": True,  # Notificar em caso de falha
    "email_on_retry": False,  # Não notificar em tentativas
}

dag = DAG(
    "silver_tvs_70_26",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

# Recuperar credenciais do Airflow Variables
MINIO_ROOT_USER = Variable.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = Variable.get("MINIO_ROOT_PASSWORD")

spark_task = SparkSubmitOperator(
    task_id="run_spark_script",
    application="/opt/bitnami/spark/scripts/tvshow/ing_silver_tvshows.py",
    conn_id="spark_default",
    verbose=True,
    driver_memory="4g",
    packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    conf={
        "spark.hadoop.fs.s3a.access.key": MINIO_ROOT_USER,
        "spark.hadoop.fs.s3a.secret.key": MINIO_ROOT_PASSWORD,
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    },
    dag=dag,
)