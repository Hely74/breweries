from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG arguments
default_args = {
    'owner': 'data_eng',
    'depends_on_past': False,
    'email': ['hellen.ls74@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

# DAG definition
with DAG(
    dag_id='brewery_medallion_pipeline',
    default_args=default_args,
    description='Pipeline Bronze -> Silver -> Gold para dados de cervejarias',
    schedule_interval='0 2 * * *',  # executa todo dia às 2h da manhã
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['brewery', 'medallion', 'ETL']
) as dag:

    # Task 1: Extrair dados e salvar Bronze
    extract_bronze = BashOperator(
        task_id='extract_bronze',
        bash_command='C:\Users\helle\OneDrive\Documentos/extract_breweries_bronze.py'
    )

    # Task 2: Processar Bronze e salvar Silver
    save_silver = BashOperator(
        task_id='save_silver',
        bash_command='C:\Users\helle\OneDrive\Documentos/save_breweries_silver.py'
    )

    # Task 3: Agregar Silver e salvar Gold
    save_gold = BashOperator(
        task_id='save_gold',
        bash_command='C:\Users\helle\OneDrive\Documentos/save_breweries_gold.py'
    )

    # Define a ordem de execução
    extract_bronze >> save_silver >> save_gold