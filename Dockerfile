FROM python:3.9-slim

# Install supervisord and copy the configuration file
RUN apt-get update && apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy the code into the container
COPY ./requirements_data-pipeline.txt /

# Install the required packages
RUN pip install --no-cache-dir -r requirements_data-pipeline.txt

# Airflow setup
ENV AIRFLOW_HOME=/app/airflow

WORKDIR $AIRFLOW_HOME/

# Copy the DAG file to the DAGs directory
RUN mkdir -p scripts
RUN mkdir -p data
RUN mkdir -p web_api

COPY ./dags/data_pipeline.py dags/
COPY ./scripts scripts/
COPY ./data data/
COPY ./web_api /
COPY ./logs logs/
ENV PYTHONPATH=$PYTHONPATH:scripts/


WORKDIR $AIRFLOW_HOME/data/raw
#unzip data
RUN apt-get update -y \
  && apt-get install -y unzip \
  && unzip archive.zip

WORKDIR $AIRFLOW_HOME
# Initialize the Airflow database
RUN airflow db init

# # Disable DAG examples
COPY ./airflow.cfg airflow.cfg

RUN airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Expose the Airflow webserver port
EXPOSE 8080

