import os
import pandas as pd
import concurrent.futures
from util.data_handling import import_csv_as_df, export_df_as_parquet
import logging
import time

# log setups
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# add console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# add file handler
log_file_path = '../logs/data_ingestion.log'
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

# data paths for persistence
data_directory = '../data/'
data_ingestion_output_path = os.path.join(data_directory, 'processed')
etfs_data_path = os.path.join('raw', 'etfs')
stocks_data_path = os.path.join('raw', 'stocks')

def import_data(directory, file_name):
    """Import data from a CSV file as a Pandas DataFrame.

    Args:
        directory (str): The directory where the CSV file is located.
        file_name (str): The name of the CSV file to import.

    Returns:
        pandas.DataFrame: The DataFrame containing the imported data.
    """
    logger.info(f"Importing data from {file_name}")
    df = import_csv_as_df(directory, file_name)
    df['Symbol'] = file_name.replace('.csv', '')
    return df

def combine_dir_data(path, num_threads = 4):
    """Combine data from all CSV files in a directory into a single DataFrame.

    Args:
        path (str): The directory containing the CSV files.

    Returns:
        pandas.DataFrame: The DataFrame containing the combined data.
    """
    #load all data
    logger.info(f"Combining data from {path}")
    abs_path = os.path.join(data_directory, path)
    csv_files = [_ for _ in os.listdir(abs_path) if _.endswith('csv')]
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(import_data, path, file_name) for file_name in csv_files]
    dataframes = [future.result() for future in futures]
    result = pd.concat(dataframes, ignore_index=True)
    return result

def ingest_data():
    """Airflow callable function to initiate ingesting data worflow.
    The workflow consists of reading various raw data >> 
    combine all sources of data >> 
    save data as a parquet.
    Returns:
        None
    """
    logger.info("Starting data preprocessing.")
    start_time = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(combine_dir_data, path) for path in [etfs_data_path, stocks_data_path]]
    result = pd.concat([future.result() for future in futures], ignore_index=True)
    export_df_as_parquet(result, 'processed', 'preprocessed_data')
    elapsed_time = time.time() - start_time
    logger.info(f"Data preprocessing complete. Elapsed time: {elapsed_time:.2f} seconds")


