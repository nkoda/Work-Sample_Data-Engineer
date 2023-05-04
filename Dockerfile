FROM python:3.9
# supervisord setup                       
RUN apt-get update && apt-get install -y supervisor                       C
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
#dependencies
COPY . /
RUN pip install --no-cache-dir -r requirements_data-pipeline.txt
# Airflow setup                       
ENV AIRFLOW_HOME=/app/airflow
RUN pip install apache-airflow                       
COPY ./dags/data_pipeline.py $AIRFLOW_HOME/dags/
RUN airflow initdb
EXPOSE 8080
CMD ["/usr/bin/supervisord"]