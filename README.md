# Data Pipeline for Raw Stock Market Data Ingestion, Feature Engineering, and Machine Learning Training

This data pipeline project focuses on ingesting and processing raw stock market data, performing feature engineering on top of the processed data, training a predictive model, and building an API service to serve the trained model API is currently hosted on [Render](https://regression-tree-api.onrender.com/predict?vol_moving_avg=12345&adj_close_rolling_med=25). The pipeline is designed using a DAG (Directed Acyclic Graph) oriented tool, Apache Airflow, to streamline data processing and management with tracking data lineage, ensuring data integrity, and minimizing errors during processing.

The model was trained using LightGBM, an open-source gradient boosting framework that uses tree-based learning algorithms. LightGBM is designed to be memory-efficient and fast, making it a good choice for large datasets.

To improve the runtime for data ingestion and feature engineering, the ThreadPoolExecutor module was used to enable parallel processing of data. This allowed for faster loading of data into memory during the training process. Additionally, a subset of the dataframe was used during the feature engineering to improve runtime by reducing the amount of calculations made.



## Pipeline Structure and Dependencies
The pipeline consists of four stages, each building on top of the previous one:
1. Ingestion of raw stock market datasets from [Kaggle](https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset).
2. Raw data processing, creating a structured Parquet file.
3. Feature engineering, creating a new Parquet file with additional columns.
4. Machine learning training, including training metrics and saving the trained model.
5. API service, serving the trained model.

## Pipeline Specification and DAG Diagram

The pipeline is defined in Airflow using DAG (Directed Acyclic Graph), where each stage is represented by a task and dependencies between tasks are represented by edges. The pipeline specification and diagram can be found in `dags/data_pipeline.py`.

## Prerequisites needed before running the Pipeline:

- Docker: The pipeline is designed to be run using Docker, so you will need to have Docker installed on your system. You can download and install Docker from the official Docker website: https://www.docker.com/get-started

- Git LFS: The pipeline uses Git LFS (Large File Storage) to manage large files. You will need to have Git LFS installed on your system. You can download and install Git LFS from the official Git LFS website: https://git-lfs.github.com/

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

## Resulting Artifacts

After running the pipeline, the following artifacts will be produced:

1. A Parquet file with the processed raw data, saved in `data/processed/preprocessed_data.parquet`.
2. A Parquet file with the added features, saved in `data/training/augmented_data.parquet`.
3. A saved machine learning model, saved in `web_api/random-forest_predictor.joblib`.
4. Logs for each step of the ETL process are found in the `logs/` directory. Training metrics are specifically saved in `logs/training.log`.

## API Service

After running the pipeline, start the API service by running:

```
python api/server.py
```

The service will be available at `http://localhost:5000`.

The following API endpoint is available:

- `GET /predict?vol_moving_avg={vol_moving_avg}&adj_close_rolling_med={adj_close_rolling_med}`: Returns the predicted trading volume for the given values of `vol_moving_avg` and `adj_close_rolling_med`.

## Testing

Unit tests are provided for the data processing and feature engineering tasks. To run the tests, execute the following command:

```
python -m unittest discover tests
```

Functional testing of the API service can be performed using tools like Postman or cURL. A sample test script is provided in `tests/test_api.py`.

## Improvement Suggestions

The following improvement suggestions can be made for future iterations of the pipeline:

1. Increase the coverage of unit tests.
2. Implement end-to-end testing for the entire pipeline.
3. Add support for additional data sources.
4. Improve the performance of the machine learning training step.
5. Add more features to the predictive model.
6. Add data validation and error handling for the API service.
