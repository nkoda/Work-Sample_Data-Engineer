# Data Pipeline for Raw Stock Market Data Ingestion, Feature Engineering, and Machine Learning Training

This data pipeline project focuses on ingesting and processing raw stock market data, performing feature engineering on top of the processed data, training a predictive model, and building an API service to serve the trained model. The API is currently hosted on [Render](https://regression-tree-api.onrender.com/predict?vol_moving_avg=12345&adj_close_rolling_med=25). The pipeline is designed using a DAG (Directed Acyclic Graph) oriented tool, Apache Airflow, to streamline data processing and management with tracking data lineage, ensuring data integrity, and minimizing errors during processing.

The model was trained using LightGBM, an open-source gradient boosting framework that uses tree-based learning algorithms. LightGBM is designed to be memory-efficient and fast, making it a good choice for large datasets.

To improve the runtime for data ingestion and feature engineering, the ThreadPoolExecutor module was used to enable parallel processing of data. This allowed for faster loading of data into memory during the training process. Additionally, a subset of the dataframe was used during the feature engineering to improve runtime by reducing the amount of calculations made.


![alt text](https://github.com/nkoda/Work-Sample_Data-Engineer/blob/main/docs/data_pipeline.drawio.svg?raw=true)


## Pipeline Structure and Dependencies
The pipeline consists of four stages, each building on top of the previous one:
1. Ingestion of raw stock market datasets from [Kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset).
2. Raw data processing, creating a structured Parquet file.
3. Feature engineering, creating a new Parquet file with additional columns.
4. Machine learning training, including training metrics and saving the trained model.
5. (Seperate service from the pipeline) API Service, serving the trained model. Accessible via [Render.](https://regression-tree-api.onrender.com/predict?vol_moving_avg=12345&adj_close_rolling_med=25)

## Pipeline Specification and DAG Diagram

The pipeline is defined in Airflow using DAG (Directed Acyclic Graph), where each stage is represented by a task and dependencies between tasks are represented by edges. The pipeline specification can be found in `dags/data_pipeline.py`, and a visual diagram representation can be found in the `docs/data_pipeline.drawio.svg`.

## Prerequisites needed before running the Pipeline:

- Docker: The pipeline is designed to be run using Docker, so you will need to have Docker installed on your system. You can download and install Docker from the official Docker website: https://www.docker.com/get-started

- Docker requirements: Upon testing, the container needs sufficient resources to complete the DAG pipeline. If your container instance is failing to complete the DAG, please allocate more RAM / CPU cores for your Docker container. You will know if you need more resources when the Airflow task fails with "Negsignal.SIGKILL" appearing in the task logs. Refer to this Stackoverflow [post](https://stackoverflow.com/questions/69231797/airflow-dag-fails-when-pythonoperator-with-error-negsignal-sigkill)

- Git LFS: The pipeline uses Git LFS (Large File Storage) to manage large files. You will need to have Git LFS installed on your system. You can download and install Git LFS from the official Git LFS website: https://git-lfs.github.com/

- Local Instance: If you plan to run Airflow locally, you can find all the necessary python packages to run the scripts under `requirements_data-pipeline.txt`. Additionally be sure to set the `dags_folder` in the AIRFLOW_HOME directory to point to this repositories `/dags` directory. This can be done by modifying the airflow.cfg file located in `cd ~/ariflow`.

Note: If you are using a Linux-based operating system, you may need to install Git LFS using your package manager. For example, on Ubuntu, you can install Git LFS by running the following command:

```bash
sudo apt-get install git-lfs
```
## Running the Pipeline

To run the pipeline, follow the instructions below:

1. Clone the repository:

```
git clone https://github.com/nkoda/Work-Sample_Data-Engineer.git
cd Work-Sample_Data-Engineer
```

2. Run Docker Compose:

```
docker-compose up --build
```

3. Access the Airflow web interface at http://localhost:8080 and enable the `data_pipeline` DAG. The login credentials is the string "admin" for both the username and password.

4. Run the pipeline by pressing the trigger button highlighted in red in the image below:
![alt text](https://github.com/nkoda/Work-Sample_Data-Engineer/blob/main/docs/airflow_server.png?raw=true)

## Resulting Artifacts

After running the pipeline, the following artifacts will be produced:

1. A Parquet file with the processed raw data, saved in `data/processed/preprocessed_data.parquet`.
2. A Parquet file with the added features, saved in `data/training/augmented_data.parquet`.
3. A saved machine learning model, saved in `web_api/ml-model/lightgbm_predictor.joblib`.
4. Logs for each step of the ETL process are found in the `logs/` directory. Training metrics are specifically saved in `logs/training.log`.

## API Service

The API is seperate from the data pipeline therefore not intended to be ran directly in the Docker container. As such there are seperate instructions to run a local API service.
A cloud hosted API of this ML model can be found on [Render.](https://regression-tree-api.onrender.com/predict?vol_moving_avg=12345&adj_close_rolling_med=25)

1. Enter the web_api directory:
```
cd ml-model/
```
2. install the required packages:
```
pip install requirements_web-api.txt
```

3. Start the flask development server:
```
flask run --port=5000
```

The service will be available at `http://localhost:5000`.

The following API endpoint is available:

- `GET /predict?vol_moving_avg={vol_moving_avg}&adj_close_rolling_med={adj_close_rolling_med}`: Returns the predicted trading volume for the given values of `vol_moving_avg` and `adj_close_rolling_med`.


## Improvement Suggestions

The following improvement suggestions can be made for future iterations of the pipeline:

1. Implement unit tests to validate trustworthiness.
2. Implement end-to-end testing for the entire pipeline.
3. Add support for additional data sources.
4. Improve the performance of the machine learning training step.
5. Add more features to the predictive model.
6. Add data validation and error handling for the API service.
