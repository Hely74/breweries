from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# =========================
# CONFIGURAÇÕES GERAIS
# =========================
default_args = {
    'owner': 'data_eng',
    'depends_on_past': False,
    'email': ['hellen.ls74@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

# =========================
# CAMINHO DOS SCRIPTS (WINDOWS)
# =========================
BASE_PATH = "C:/Users/helle/OneDrive/Documentos"

# =========================
# DEFINIÇÃO DA DAG
# =========================
with DAG(
    dag_id='brewery_medallion_pipeline',
    default_args=default_args,
    description='Pipeline ETL Bronze -> Silver -> Gold para dados de cervejarias',
    schedule_interval='0 2 * * *',  # executa todo dia às 02:00
    start_date=datetime(2024, 1, 1),  # boa prática
    catchup=False,
    tags=['data-engineering', 'medallion', 'brewery']
) as dag:

    # =========================
    # TASK 1 - BRONZE
    # =========================
    extract_bronze = BashOperator(
        task_id='extract_bronze',
        bash_command=f'python "{BASE_PATH}/extract_breweries_bronze.py"',
        execution_timeout=timedelta(minutes=10)
    )

    # =========================
    # TASK 2 - SILVER
    # =========================
    transform_silver = BashOperator(
        task_id='transform_silver',
        bash_command=f'python "{BASE_PATH}/save_breweries_silver.py"',
        execution_timeout=timedelta(minutes=10)
    )

    # =========================
    # TASK 3 - GOLD
    # =========================
    aggregate_gold = BashOperator(
        task_id='aggregate_gold',
        bash_command=f'python "{BASE_PATH}/save_breweries_gold.py"',
        execution_timeout=timedelta(minutes=10)
    )

    # =========================
    # ORDEM DE EXECUÇÃO
    # =========================
    extract_bronze >> transform_silver >> aggregate_gold