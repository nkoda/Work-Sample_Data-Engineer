import os
import pandas as pd
import logging
import concurrent.futures
from util.data_handling import import_csv_as_df, export_df_as_parquet

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

if __name__ == '__main__':
    logger.info("Starting data preprocessing.")
    pd_etfs_data = combine_dir_data(etfs_data_path)
    pd_stocks_data = combine_dir_data(stocks_data_path)
    result = pd.concat([pd_etfs_data, pd_stocks_data], ignore_index=True)
    export_df_as_parquet(result, 'processed', 'preprocessed_data')
    logger.info("Data preprocessing complete.")


