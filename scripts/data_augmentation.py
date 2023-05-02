import os
import pandas as pd
import pyarrow as pa
import logging
from util.data_handling import import_parquet_as_df, export_df_as_parquet

from data_preprocessing import data_preprocessing_output_path
data_directory = '../data/'
data_augmentation_output_path = os.path.join(data_directory, 'feature_engineering')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def manipulate_data(operation, dataframe):
    try:
        operation(dataframe)
        logger.info(f"{operation.__name__} successful")
    except Exception as e:
        logger.error(f"{operation.__name__} failed with error: {e}")

def calculate_volume_moving_average(dataframe, window=3):
    dataframe['vol_moving_avg'] = (
        dataframe
        .groupby('Symbol')['volume']
        .rolling(window=window)
        .mean()
    )
    return True

def calculate_adj_rolling_median(dataframe, window=3):
    dataframe['adj_close_rolling_med'] = (
        dataframe
        .groupby('Symbol')['adj']
        .rolling(window=window)
        .median()
    )
    return True

def read_data(file_name):
    try:
        import_parquet_as_df(data_preprocessing_output_path, file_name)
        logger.info(f"Successfully retrieved {file_name}.parquet")
    except Exception as e:
        logger.error(f"Failed to open {file_name}.parquet. Error message: {str(e)}")

def save_data(dataframe, file_name):
    try:
        export_df_as_parquet(dataframe, data_augmentation_output_path, file_name)
        logger.info(f"Data successfully saved to {file_name}.parquet")
    except Exception as e:
        logger.error(f"Failed to save {file_name}.parquet. Error message: {str(e)}")

if __name__ == '__main__':
    table = pa.parquet.read_table()
    df = table.to_pandas()
    df = df.sort_values(by='Date')
    manipulate_data(calculate_volume_moving_average, df)
    manipulate_data(calculate_adj_rolling_median, df)
    save_data(df, 'manipulated_data')