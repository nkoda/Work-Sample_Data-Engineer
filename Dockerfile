FROM python:3.9

# Install supervisord and copy the configuration file
RUN apt-get update && apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy the code into the container
COPY ./requirements_data-pipeline.txt /

# Install the required packages
RUN pip install --no-cache-dir -r requirements_data-pipeline.txt

# Airflow setup
ENV AIRFLOW_HOME=/app/airflow
RUN pip install apache-airflow

# Disable DAG examples
COPY ./airflow.cfg $AIRFLOW_HOME/airflow.cfg
RUN sed -i 's/^load_examples = .*/load_examples = false/' $AIRFLOW_HOME/airflow.cfg
COPY ./dags/* $AIRFLOW_HOME/dags/

# Copy the DAG file to the DAGs directory
RUN mkdir -p $AIRFLOW_HOME/scripts
RUN mkdir -p $AIRFLOW_HOME/data
RUN mkdir -p $AIRFLOW_HOME/web_api

COPY ./dags/data_pipeline.py $AIRFLOW_HOME/dags/
COPY ./scripts $AIRFLOW_HOME/scripts
COPY ./data $AIRFLOW_HOME/data
COPY ./web_api $AIRFLOW_HOME/
ENV PYTHONPATH=$PYTHONPATH:/app/airflow/dags/scripts

#unzip data
RUN apt-get update -y \
  && apt-get install -y unzip \
  && unzip $AIRFLOW_HOME/data/raw/archive.zip \
  && rm $AIRFLOW_HOME/data/raw/archive.zip

# Initialize the Airflow database
RUN airflow db init

RUN airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Expose the Airflow webserver port
EXPOSE 8080

