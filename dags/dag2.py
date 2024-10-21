import sys
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Adicione o caminho do diretório 'dags' ao sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'tarefas'))

# Agora você pode importar as funções corretamente
from tarefas import passo1_bronze, passo2_silver, passo3_gold

with DAG(
    dag_id='medallion_architecture',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id='passo1',
        python_callable=passo1_bronze
    )
    
    task2 = PythonOperator(
        task_id='passo2',
        python_callable=passo2_silver
    )
    
    task3 = PythonOperator(
        task_id='passo3',
        python_callable=passo3_gold
    )

    task1 >> task3
    task2 >> task3
