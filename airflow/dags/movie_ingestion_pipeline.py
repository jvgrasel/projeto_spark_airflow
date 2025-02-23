from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime

# Função de notificação de erro
def alerta_erro(context):
    send_email(
        to="joathan94@yahoo.com",
        subject=f"⚠️ Falha na Tarefa: {context['task_instance'].task_id}",
        html_content=f"""
            <h3>Erro na Execução da Tarefa</h3>
            <p><b>DAG:</b> {context['dag'].dag_id}</p>
            <p><b>Tarefa:</b> {context['task_instance'].task_id}</p>
            <p><b>Horário:</b> {context['execution_date']}</p>
        """
    )

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 12),
    "retries": 1,
    "on_failure_callback": alerta_erro
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