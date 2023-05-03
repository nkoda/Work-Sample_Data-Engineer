FROM apache/airflow:2.1.2

RUN pip3 install -r data_pipeline_requirements.txt

