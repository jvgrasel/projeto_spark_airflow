from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 12),
    "retries": 1,
}

dag = DAG(
    "ing_mov_20_21",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

run_spark_script = BashOperator(
    task_id="run_spark_script",
    bash_command="spark-submit "
                 "--packages org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-s3:1.12.720 "  # SDK reduzido
                 "/opt/bitnami/spark/scripts/ing_mov_20_21.py",
    dag=dag,
)

run_spark_script