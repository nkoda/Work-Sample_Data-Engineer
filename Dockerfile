FROM python:3.9

# Install supervisord and copy the configuration file
RUN apt-get update && apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Copy the code into the container
COPY . /app

# Install the required packages
WORKDIR /app
RUN pip install --no-cache-dir -r requirements_data-pipeline.txt

# Airflow setup
ENV AIRFLOW_HOME=/app/airflow
RUN pip install apache-airflow

# Copy the DAG file to the DAGs directory
COPY ./dags/data_pipeline.py $AIRFLOW_HOME/dags/

# Initialize the Airflow database
RUN airflow db init

# Expose the Airflow webserver port
EXPOSE 8080

# Start supervisord
CMD ["/usr/bin/supervisord"]
