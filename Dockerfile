# Use the Apache Airflow base image
FROM apache/airflow:2.1.2

# Copy the requirements file to the image
COPY requirements_data-pipeline.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements_data-pipeline.txt

# Copy the DAG file to the DAGs directory
COPY ./dags/data_pipeline.py /opt/airflow/dags/

# Set the entry point to the Airflow scheduler
ENTRYPOINT ["airflow"]

# Set the command to run the scheduler and webserver in daemon mode
CMD ["scheduler", "--daemon", "&&", "webserver", "--daemon"]
