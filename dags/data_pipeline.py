from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from scripts.data_ingestion import ingest_data
from scripts.data_augmentation import transform_data
from scripts.train_model import deploy_model

default_args = {
    'owner': 'my_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@monthly')

# Define the tasks
task_ingest = PythonOperator(task_id='ingest_data', python_callable=ingest_data, dag=dag)
task_transform = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
taks_model_deployment = PythonOperator(task_id='deploy_model', python_callable=deploy_model, dag=dag)

task_ingest >> task_transform >> taks_model_deployment