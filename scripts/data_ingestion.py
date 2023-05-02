import os
import pandas as pd
import logging
from util.data_handling import import_csv_as_df, export_df_as_parquet

# set up logging
logging.basicConfig(filename='data_preprocessing.log', level=logging.INFO)

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
    logging.info(f"Importing data from {file_name}")
    df = import_csv_as_df(directory, file_name)
    df['Symbol'] = file_name.replace('.csv', '')
    return df

def combine_dir_data(path):
    """Combine data from all CSV files in a directory into a single DataFrame.

    Args:
        path (str): The directory containing the CSV files.

    Returns:
        pandas.DataFrame: The DataFrame containing the combined data.
    """
    #load all data
    logging.info(f"Combining data from {path}")
    abs_path = os.path.join(data_directory, path)
    files = os.listdir(abs_path)
    dfs = [''] * len(files)
    for index, file in enumerate(files):
        if file.endswith('.csv'):
            df = import_data(path, file)
            dfs[index] = df
    result = pd.concat(dfs, ignore_index=True)
    return result

if __name__ == '__main__':
    logging.info("Starting data preprocessing.")
    pd_etfs_data = combine_dir_data(etfs_data_path)
    pd_stocks_data = combine_dir_data(stocks_data_path)
    result = pd.concat([pd_etfs_data, pd_stocks_data], ignore_index=True)
    export_df_as_parquet(result, 'processed', 'preprocessed_data')
    logging.info("Data preprocessing complete.")

