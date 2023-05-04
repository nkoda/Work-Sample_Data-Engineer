# Use the Apache Airflow base image
FROM apache/airflow:2.3.3


# Copy the requirements file to the image
COPY requirements_data-pipeline.txt .

# Copy file structure
COPY . /

# Install the required packages
RUN pip install --no-cache-dir -r requirements_data-pipeline.txt

# Copy the DAG file to the DAGs directory
COPY ./dags/data_pipeline.py /opt/airflow/dags/

USER root
# Install the unzip utility (if not already installed)
RUN ls -a
RUN apt-get update -y
RUN apt-get install -y unzip
RUN unzip /data/raw/archive.zip

user airflow

# Set the entry point to the Airflow scheduler
ENTRYPOINT ["airflow"]

# Set the command to run the scheduler and webserver in daemon mode
CMD ["scheduler", "--daemon", "&&", "webserver", "--daemon"]
