from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["fattetv@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

dag = DAG(
    "colect_and_process_movies_updates",
    default_args=default_args,
    schedule_interval="0 0 */3 * *",
    catchup=False,
    max_active_runs=1,
)

# Recuperar credenciais do MinIO
MINIO_ROOT_USER = Variable.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = Variable.get("MINIO_ROOT_PASSWORD")

# Configurações comuns para ambas as tarefas
spark_config = {
    "conn_id": "spark_default",
    "verbose": True,
    "driver_memory": "4g",
    "packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "conf": {
        "spark.hadoop.fs.s3a.access.key": MINIO_ROOT_USER,
        "spark.hadoop.fs.s3a.secret.key": MINIO_ROOT_PASSWORD,
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    },
    "dag": dag,
}

# Primeira tarefa - colect_mov_changes.py
colect_updates = SparkSubmitOperator(
    task_id="colect_movie_updates",
    application="/opt/bitnami/spark/scripts/movies/colect_mov_changes.py",
    **spark_config
)

# Segunda tarefa - movies_change_update.py
update_data = SparkSubmitOperator(
    task_id="update_movie_silver",
    application="/opt/bitnami/spark/scripts/movies/movies_change_update.py",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    **spark_config
)

# Definir a ordem de execução
colect_updates >> update_data