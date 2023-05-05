from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add the parent directory of the DAG file to the Python path
dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(dag_dir, os.pardir))
sys.path.append(parent_dir)

# Add the scripts directory to the Python path
scripts_dir = os.path.join(parent_dir, 'scripts')
sys.path.append(scripts_dir)

from scripts.data_ingestion import ingest_data
from scripts.data_augmentation import transform_data
from scripts.train_model import deploy_model

default_args = {
    'owner': 'my_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 3),
}

dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@once')

# Define the tasks
task_ingest = PythonOperator(task_id='ingest_data', python_callable=ingest_data, dag=dag)
task_transform = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)
taks_model_deployment = PythonOperator(task_id='deploy_model', python_callable=deploy_model, dag=dag)

task_ingest >> task_transform >> taks_model_deployment