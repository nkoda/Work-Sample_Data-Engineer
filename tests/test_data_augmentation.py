import os
import pytest
import pyarrow as pa
from pyarrow import parquet
import pandas as pd
import numpy as np
import sys
# Add the root directory to sys.path
root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)
from scripts.data_augmentation import (
    manipulate_data,
    calculate_volume_moving_average,
    calculate_adj_rolling_median,
    read_data,
    save_data,
)

current_dir = os.path.dirname(os.path.abspath(__file__))
data_directory = os.path.join(current_dir, '..', 'data')

@pytest.fixture
def parquet_fixture():
    # Set up a Parquet file for testing
    parquet_path = os.path.join(data_directory, 'processed', 'data.parquet')
    df = pd.DataFrame({
        'Date': [
            '2015-07-13',
            '2015-07-14',
            '2015-07-15',
            '2015-07-16',
            '2015-07-17'
        ],
        'Open': [
            40.29999923706055,
            40.08000183105469,
            40.08000183105469,
            40.08000183105469,
            40.18000030517578
        ],
        'Volume': [1200, 0, 0, 0, 400]
    })

    table = pa.Table.from_pandas(df)
    parquet.write_table(table, parquet_path)
    yield parquet_path
    # Teardown - remove the created Parquet file
    os.remove(parquet_path)

def test_manipulate_data():
    # Create a test DataFrame
    data = {
        'Symbol': ['AAPL', 'AAPL', 'AAPL'],
        'Date': ['2021-01-01', '2021-01-02', '2021-01-03'],
        'Volume': [100, 200, 300],
        'Adj Close': [50.0, 51.0, 52.0]
    }
    df = pd.DataFrame(data)

    # Define the operation function
    def operation(dataframe):
        dataframe['new_column'] = dataframe['Volume'] + 100
        return 'new_column', dataframe['new_column']

    # Call the manipulate_data function
    column_name, column_val = manipulate_data(operation, df)
    expected_column_val = pd.Series([200, 300, 400], name='new_column')
    # Assert the result
    assert column_name == 'new_column'
    assert column_val.equals(expected_column_val)

def test_calculate_volume_moving_average():
    # Create a test DataFrame
    data = {
        'Symbol': ['AAPL', 'AAPL', 'AAPL', 'AAPL'],
        'Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04'],
        'Volume': [100, 200, 300, 400],
        'Adj Close': [50.0, 51.0, 52.0, 53.0]
    }
    df = pd.DataFrame(data)

    # Call the calculate_volume_moving_average function
    column_name, column_val = calculate_volume_moving_average(df, window=2)

    # Assert the result
    expected_column = pd.Series([np.nan, 150.0, 250.0, 350.0], name='vol_moving_avg')
    assert column_val.equals(expected_column)

def test_calculate_adj_rolling_median():
    # Create a test DataFrame
    data = {
        'Symbol': ['AAPL', 'AAPL', 'AAPL'],
        'Date': ['2021-01-01', '2021-01-02', '2021-01-03'],
        'Volume': [100, 200, 300],
        'Adj Close': [50.0, 51.0, 52.0]
    }
    df = pd.DataFrame(data)

    # Call the calculate_adj_rolling_median function
    column_name, column_val = calculate_adj_rolling_median(df, window=2)

    # Assert the result
    expected_column = pd.Series([np.nan, 50.5, 51.5], name='adj_close_rolling_med')
    assert column_val.equals(expected_column)

def test_read_data(parquet_fixture):
    # Call the read_data function
    result = read_data('data')
    # Assert the result
    assert isinstance(result, pd.DataFrame)

def test_save_data():
    # Create a test DataFrame
    data = {
        'Symbol': ['AAPL', 'AAPL', 'AAPL'],
        'Date': ['2021-01-01', '2021-01-02', '2021-01-03'],
        'Volume': [100, 200, 300],
        'Adj Close': [50.0, 51.0, 52.0]
    }
    df = pd.DataFrame(data)

    # Call the save_data function
    save_data(df, 'test_save_data')

    # Check if the saved file exists
    saved_file_path = os.path.join(data_directory, 'training', 'test_save_data.parquet')
    assert os.path.exists(saved_file_path)

    # Clean up - delete the saved file
    os.remove(saved_file_path)