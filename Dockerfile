FROM apache/airflow:2.1.2

RUN pip3 install -r requirements_data-pipeline.txt

