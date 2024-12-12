import sys
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.models import Variable
from airflow.hooks.base import BaseHook

aws_conn = BaseHook.get_connection('aws_default')
aws_access_key = aws_conn.login
aws_secret_key = aws_conn.password
aws_session_token = Variable.get("AWS_SESSION_TOKEN", default_var="")

spark_submit_base = f'''
    export AWS_ACCESS_KEY_ID={aws_access_key};
    export AWS_SECRET_ACCESS_KEY={aws_secret_key};
    export AWS_SESSION_TOKEN={aws_session_token};
    /opt/spark/bin/spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.508 \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
        --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
        --conf spark.hadoop.fs.s3a.region=us-east-1 \
        /home/beatriz/Documentos/airflow/dags/tarefas/tarefas.py 
''' # change the path to your scripts path

sys.path.append(os.path.join(os.path.dirname(__file__), 'tarefas'))

from tarefas import passo1_bronze, passo2_silver, passo3_gold

with DAG(
    dag_id='medallion_architecture',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False
) as dag:

    spark_submit_task = BashOperator(
    task_id='spark_submit',
    bash_command=spark_submit_base,
    dag=dag
    )
    
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

    spark_submit_task >> task1 >> task2 >> task3