# Use the Apache Airflow base image
FROM apache/airflow:2.3.3

# Set the AIRFLOW_HOME environment variable
ENV AIRFLOW_HOME=/opt/airflow

# Install the Apache Airflow CLI
RUN pip install apache-airflow

# Copy the requirements file to the image
COPY requirements_data-pipeline.txt .

# Install the required packages
RUN pip install --no-cache-dir -r requirements_data-pipeline.txt

# Copy file structure
COPY . /

# Copy the DAG file to the DAGs directory
COPY ./dags/data_pipeline.py /opt/airflow/dags/

USER root

# Install the unzip utility (if not already installed)
RUN apt-get update -y \
  && apt-get install -y unzip \
  && unzip /data/raw/archive.zip \
  && rm /data/raw/archive.zip

USER airflow
