import os
import pandas as pd
import pyarrow as pa
from pyarrow import csv, parquet


current_dir = os.path.dirname(os.path.abspath(__file__))
data_directory = os.path.join(current_dir, '..', '..', 'data')

def path(parent_directory, file_name, extension):
    """
    Returns the full file path given a parent directory, file name and extension

    Args:
    parent_directory (str): the name of the parent directory
    file_name (str): the name of the file without extension
    extension (str): the file extension, including the period (e.g. '.csv', '.parquet')

    Returns:
    str: the full file path including the parent directory, file name and extension
    """
    return os.path.join(
        data_directory,
        parent_directory,
        file_name + extension
    )

def import_csv_as_df(parent_directory, file_name):
    """
    Imports a CSV file as a Pandas DataFrame

    Args:
    parent_directory (str): the name of the parent directory holding the file
    file_name (str): the name of the CSV file without extension

    Returns:
    pandas.DataFrame: the DataFrame containing the data from the CSV file
    """
    if file_name.endswith('.csv'):
        file_name = file_name.replace('.csv', '')
    file_path = path(parent_directory, file_name, '.csv')
    table = csv.read_csv(file_path)
    df = table.to_pandas()
    df = validate_data_types(df)
    return df

def validate_data_types(dataframe):
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])
    return dataframe

def import_parquet_as_df(parent_directory, file_name):
    """
    Imports a Parquet file as a Pandas DataFrame

    Args:
    parent_directory (str): the name of the parent directory
    file_name (str): the name of the Parquet file without extension

    Returns:
    pandas.DataFrame: the DataFrame containing the data from the Parquet file, sorted by 'Date'
    """
    file_path = path(parent_directory, file_name, '.parquet')
    table = parquet.read_table(file_path)
    df = table.to_pandas()
    df = validate_data_types(df)
    df = df.sort_values(by='Date')
    return df

def export_df_as_parquet(dataframe, parent_directory, file_name):
    """
    Exports a Pandas DataFrame as a Parquet file

    Args:
    dataframe (pandas.DataFrame): the DataFrame to export
    parent_directory (str): the name of the parent directory
    file_name (str): the name of the Parquet file without extension
    """
    file_path = path(parent_directory, file_name, '.parquet')
    table = pa.Table.from_pandas(dataframe)
    parquet.write_table(table, file_path)
