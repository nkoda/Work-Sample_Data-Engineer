import os
import logging
from util.data_handling import import_parquet_as_df, export_df_as_parquet

# data paths for persistence
from data_ingestion import data_ingestion_output_path
data_directory = '../data/'
data_augmentation_output_path = os.path.join(data_directory, 'training')

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
log_file_path = '../logs/data_augmentation.log'
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

def manipulate_data(operation, dataframe):
    """Manipulates the given dataframe using the given operation function.
    
    If an exception is raised, logs an error message.
    Otherwise, logs a success message with the name of the operation function.
    
    Args:
        operation: A function that takes a pandas DataFrame as input and returns a bool.
        dataframe: The pandas DataFrame to manipulate.
    """
    try:
        logger.info(f"Attempting {operation.__name__}")
        operation(dataframe)
        logger.info(f"{operation.__name__} successful")
    except Exception as e:
        logger.error(f"{operation.__name__} failed with error: {e}")


def calculate_volume_moving_average(dataframe, window='30D'):
    """Calculates the volume moving average for the given DataFrame and returns True.
    
    Adds a new 'vol_moving_avg' column to the DataFrame, which is the rolling mean of
    the 'volume' column for each 'Symbol' group, using the given window size.
    
    Args:
        dataframe: The pandas DataFrame to calculate the volume moving average for.
        window: The size of the rolling window used to calculate the moving average.
    
    Returns:
        True, indicating that the calculation was successful.
    """
    dataframe['vol_moving_avg'] = (
        dataframe[['Symbol', 'Date', 'Volume']]
        .groupby('Symbol', as_index=False)
        .rolling(window, on='Date')
        .mean()['Volume']
    )
    return True

def calculate_adj_rolling_median(dataframe, window='30D'):
    """Calculates the adjusted rolling median for the given DataFrame and returns True.
    
    Adds a new 'adj_close_rolling_med' column to the DataFrame, which is the rolling median of
    the 'adj' column for each 'Symbol' group, using the given window size.
    
    Args:
        dataframe: The pandas DataFrame to calculate the adjusted rolling median for.
        window: The size of the rolling window used to calculate the rolling median.
    
    Returns:
        True, indicating that the calculation was successful.
    """
    dataframe['adj_close_rolling_med'] = (
        dataframe[['Symbol', 'Date', 'Adj Close']]
        .groupby('Symbol', as_index=False)
        .rolling(window, on='Date')
        .median()['Adj Close']
    )
    return True


def read_data(file_name):
    """Reads a parquet file with the given name and returns a pandas DataFrame.
    
    If an exception is raised, logs an error message.
    Otherwise, logs a success message with the name of the parquet file.
    
    Args:
        file_name: The name of the parquet file to read.
    
    Returns:
        The pandas DataFrame read from the parquet file.
    """
    try:
        df = import_parquet_as_df(data_ingestion_output_path, file_name)
        logger.info(f"Successfully retrieved {file_name}.parquet")
        return df
    except Exception as e:
        logger.error(f"Failed to open {file_name}.parquet. Error message: {str(e)}")


def save_data(dataframe, file_name):
    """Saves a given dataframe as a parquet file with the specified file name.

    Args:
        dataframe (pandas.DataFrame): The dataframe to be saved.
        file_name (str): The name of the parquet file to be saved.

    Raises:
        Exception: If the dataframe cannot be saved as a parquet file.

    Returns:
        None
    """
    try:
        export_df_as_parquet(dataframe, 'training', file_name)
        logger.info(f"Data successfully saved to {file_name}.parquet")
    except Exception as e:
        logger.error(f"Failed to save {file_name}.parquet. Error message: {str(e)}")
        raise e

if __name__ == '__main__':
    logger.info("Initializing data augmentation process")
    df = read_data('preprocessed_data')
    manipulate_data(calculate_volume_moving_average, df)
    manipulate_data(calculate_adj_rolling_median, df)
    save_data(df, 'augmented_data')
