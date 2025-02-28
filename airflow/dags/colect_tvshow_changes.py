from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
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
    "colect_Tv_Show_changes",
    default_args=default_args,
    schedule_interval="0 0 */9 * *",  # Executa a cada 9 dias à meia-noite
    catchup=False,
    max_active_runs=1,
)

# Recuperar credenciais do MinIO
MINIO_ROOT_USER = Variable.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = Variable.get("MINIO_ROOT_PASSWORD")

spark_task = SparkSubmitOperator(
    task_id="processar_dados_movimentacoes",
    application="/opt/bitnami/spark/scripts/tvshow/colect_tvshow_changes.py",
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

spark_task