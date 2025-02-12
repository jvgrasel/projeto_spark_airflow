# 📊 Data Engineering Project - [Nome do Projeto]

Este projeto foi desenvolvido para processar e analisar dados do The Movie Database(TMDB). Utiliza Apache Spark, Airflow e PostgreSQL para transformar dados brutos em insights acionáveis.

📌 **Principais funcionalidades**:
- Coleta de dados via API.
- Processamento com Spark e armazenamento em camadas Bronze, Silver e Gold.
- Orquestração de pipelines com Airflow.
- Visualização de dados com Metabase.

## 🏗️ Arquitetura

O projeto segue a arquitetura de Data Lakehouse, organizando os dados em três camadas:

1. **Bronze**: Dados brutos armazenados no MinIO.
2. **Silver**: Dados limpos e transformados com Spark.
3. **Gold**: Dados prontos para análise no PostgreSQL.

🚀 **Tecnologias Utilizadas**:
- **Docker**: Gerenciamento dos serviços do projeto.
- **Apache Airflow**: Orquestração dos pipelines de dados.
- **Apache Spark**: Processamento e transformação dos dados.
- **PostgreSQL**: Banco de dados para armazenamento e consultas analíticas.
- **MinIO**: Armazenamento de dados como Data Lake.
- **Metabase**: Visualização de dados e dashboards.

### 🔥 **Diagrama**
![Arquitetura](docs/arquitetura.png)  <!-- Adicione uma imagem da arquitetura, se possível -->
## 📂 Estrutura de Diretórios

```
📦 projeto-de-engenharia-de-dados
├── airflow/
│   ├── dags/
│   ├── logs/
│   │   ├── dag_processor_manager/
│   │   ├── scheduler/
├── config/
├── data_lake/
│   ├── bronze/
│   │   ├── movie_00_10/
│   │   ├── movie_70_80/
│   │   ├── movie_80_90/
│   │   ├── movie_90_00/
│   ├── silver/
│   ├── gold/
├── docker/
├── env/
├── metabase/
│   ├── config/
├── postgres/
│   ├── backups/
├── spark/
│   ├── jobs/
│   ├── scripts/
├── tests/
│   ├── integration/
│   ├── unit/
└── README.md
```

## 🚀 Como Rodar o Projeto

### 1️⃣ **Pré-requisitos**
Antes de iniciar, certifique-se de ter os seguintes softwares instalados:
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Python 3.9+](https://www.python.org/)
- [API](https://www.themoviedb.org/settings/api)

### 2️⃣ **Passos para execução**

Clone o repositório:
```bash
git clone https://github.com/seu-usuario/seu-repositorio.git
cd seu-repositorio

Ajuste o arquivo "api.env" com o seu token de autenticação e sua chave de acesso. 

docker-compose up -d
```

Acesse os serviços:

- Airflow: http://localhost:8080 (user: airflow, senha: airflow)
- MinIO: http://localhost:9001 (user: minioadmin, senha: minioadmin)
- Metabase: http://localhost:3000
- Jupyter http://localhost:8888


---

### **5️⃣ Pipelines de Dados**
Descreva os principais fluxos de dados no projeto.  

```md
## ⚙️ Pipelines de Dados

1️⃣ **Ingestão**: Os dados são coletados via API e armazenados no MinIO (camada Bronze).  
2️⃣ **Processamento**: O Apache Spark processa os dados e os salva na camada Silver.  
3️⃣ **Armazenamento**: Dados transformados são carregados no PostgreSQL (camada Gold).  
4️⃣ **Orquestração**: O Airflow gerencia a execução automática dos pipelines.  

🛠 **Principais DAGs do Airflow**:
- `ingestao_dados.py`: Coleta os dados brutos da API.
- `processamento_spark.py`: Transforma os dados na camada Silver.
- `carga_postgres.py`: Carrega os dados finais no PostgreSQL.
```

## 📊 Exemplos de Uso

Após rodar o pipeline, você pode consultar os dados processados no PostgreSQL:

```sql
SELECT * FROM gold.tabela_principal LIMIT 10;
```
Ou usar PySpark para carregar os dados:
```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Projeto").getOrCreate()
df = spark.read.parquet("data_lake/gold/dataset_final.parquet")
df.show()
```

---

### **7️⃣ Contribuição e Contato** 

```md
## 🤝 Contribuição

Contribuições são bem-vindas! Para contribuir:
1. Faça um fork do repositório.
2. Crie uma nova branch (`git checkout -b minha-feature`).
3. Commit suas mudanças (`git commit -m "Adicionei nova feature"`).
4. Envie um pull request.
```
## 📬 Contato
Dúvidas ou sugestões? Me encontre em:
📧 **Email:** joathan94@yahoo.com   
📘 **LinkedIn:** [Joathan V Grasel](https://www.linkedin.com/in/jgrasel/)
