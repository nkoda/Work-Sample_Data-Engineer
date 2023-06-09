import os
import sys
# Add the root directory to sys.path
root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)
import logging
import time
import concurrent.futures
from util.data_handling import import_parquet_as_df, export_df_as_parquet

# data paths for persistence
current_dir = os.path.dirname(os.path.abspath(__file__))
from data_ingestion import data_ingestion_output_path
data_directory = os.path.join(current_dir, '..', 'data')
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
# Construct the path to the logs directory
logs_dir = os.path.join(current_dir, "../logs")
log_file_path = os.path.join(logs_dir, "data_augmentation.log")
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
        column_name, column_value = operation(dataframe)
        logger.info(f"{operation.__name__} successful")
        return column_name, column_value
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
        An tuple containing the name of the column, and a pd.Series holding the data.
    """
    # dataframe['vol_moving_avg'] = (
    column = (
        dataframe[['Symbol', 'Date', 'Volume']]
        .groupby('Symbol', as_index=False)
        .rolling(window, on='Date')
        .mean()['Volume']
    )
    return ['vol_moving_avg', column]

def calculate_adj_rolling_median(dataframe, window='30D'):
    """Calculates the adjusted rolling median for the given DataFrame and returns True.
    
    Adds a new 'adj_close_rolling_med' column to the DataFrame, which is the rolling median of
    the 'adj' column for each 'Symbol' group, using the given window size.
    
    Args:
        dataframe: The pandas DataFrame to calculate the adjusted rolling median for.
        window: The size of the rolling window used to calculate the rolling median.
    
    Returns:
        An tuple containing the name of the column, and a pd.Series holding the data.
    """
    # dataframe['adj_close_rolling_med'] = (
    column = (
        dataframe[['Symbol', 'Date', 'Adj Close']]
        .groupby('Symbol', as_index=False)
        .rolling(window, on='Date')
        .median()['Adj Close']
    )
    return ['adj_close_rolling_med', column]


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
        logger.info(f"Attempting to retrieve {file_name}.parquet")
        logger.info(f"Path {os.path.exists(os.path.join(data_directory, 'processed'))}")
        df = import_parquet_as_df('processed', file_name)
        logger.info(f"Successfully retrieved {file_name}.parquet")
        logger.info(f"{file_name}.parquet statistics: ")
        logger.info(df.describe().to_string())
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

        logger.info(f"Attempting to save data as {file_name}.parquet")
        export_df_as_parquet(dataframe, 'training', file_name)
        logger.info(f"Data successfully saved to {file_name}.parquet")
    except Exception as e:
        logger.error(f"Failed to save {file_name}.parquet. Error message: {str(e)}")
        raise e

def transform_data():
    """Airflow callable function to initiate data transformation workflow.
    The workflow consists of reading data >> transform data >> save data as a parquet.

    Raises:
        Exception: If a data transform has failed.

    Returns:
        None
    """
    logger.info("Initializing data augmentation process")
    start_time = time.time()
    df = read_data('preprocessed_data')
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        futures.append(
            executor.submit(
                manipulate_data,
                calculate_volume_moving_average,
                df
            ))
        futures.append(
            executor.submit(
                manipulate_data, 
                calculate_adj_rolling_median, 
                df
            ))
        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                column_name, column_value = future.result()
                df[column_name] = column_value
            except Exception as e:
                logger.error(f"Error executing manipulate_data: {e}")
                continue
    save_data(df, 'augmented_data')
    elapsed_time = time.time() - start_time
    logger.info(f"Data Augmentation complete. Elapsed time: {elapsed_time:.2f} seconds")