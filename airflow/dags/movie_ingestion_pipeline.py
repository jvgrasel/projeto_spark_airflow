from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 12),
    "retries": 1,
    "email": ["fattetv@gmail.com"],  # Lista de destinatários
    "email_on_failure": True,  # Notificar em caso de falha
    "email_on_retry": False,  # Não notificar em tentativas
}

dag = DAG(
    "movie_ingestion_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["movies"],
)

# Lista de scripts a serem executados
scripts = [
    "ing_mov_00_09.py",
    "ing_mov_10_19.py",
    "ing_mov_20_26.py",
    "ing_mov_70_79.py",
    "ing_mov_80_89.py",
    "ing_mov_90_99.py"
]

# Credenciais (carregadas uma única vez)
MINIO_ROOT_USER = Variable.get("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = Variable.get("MINIO_ROOT_PASSWORD")

# Cria tarefas dinamicamente
previous_task = None

for script in scripts:
    task_id = f"run_{script.replace('.py', '')}"
    
    task = SparkSubmitOperator(
        task_id=task_id,
        application=f"/opt/bitnami/spark/scripts/{script}",
        conn_id="spark_default",
        verbose=True,
        driver_memory="2g",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        conf={
            "spark.hadoop.fs.s3a.access.key": MINIO_ROOT_USER,
            "spark.hadoop.fs.s3a.secret.key": MINIO_ROOT_PASSWORD,
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
        },
        dag=dag,
        trigger_rule="all_done"  # Executa mesmo se a anterior falhar
    )
    
    if previous_task:
        previous_task >> task
        
    previous_task = task