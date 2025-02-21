FROM apache/airflow:2.10.5

# Instalar dependências
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==5.0.0 \
    && pip install requests python-dotenv pyspark

USER root

# Instala o OpenJDK 17
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

# Define a variável de ambiente JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Remove o chown já que o Spark não está neste container
# ENV SPARK_HOME=/opt/bitnami/spark  # Opcional se não estiver usando Spark internamente

USER airflow
