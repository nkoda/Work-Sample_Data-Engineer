import os
import pytest
import pandas as pd
from scripts.data_ingestion import import_data, combine_dir_data, save_data

current_dir = os.path.dirname(os.path.abspath(__file__))
data_directory = os.path.join(current_dir, '..', 'data')
data_ingestion_output_path = os.path.join(data_directory, 'processed')

@pytest.fixture
def csv_fixture():
    # Set up a CSV file for testing
    csv_path = os.path.join(data_directory, 'test_data', 'test.csv')
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
        'High': [
            40.29999923706055,
            40.08000183105469,
            40.08000183105469,
            40.08000183105469,
            40.18000030517578
        ],
        'Low': [
            40.060001373291016,
            40.08000183105469,
            40.08000183105469,
            40.08000183105469,
            40.150001525878906
        ],
        'Close': [
            40.08000183105469,
            40.08000183105469,
            40.08000183105469,
            40.08000183105469,
            40.150001525878906
        ],
        'Adj Close': [
            31.722698211669922,
            31.722698211669922,
            31.722698211669922,
            31.722698211669922,
            31.778106689453125
        ],
        'Volume': [1200, 0, 0, 0, 400]
    })
    df.to_csv(csv_path, index=False)
    yield csv_path
    # Teardown - remove the created CSV file
    os.remove(csv_path)
    

def test_import_data(csv_fixture):
    directory, file_name = os.path.split(csv_fixture)
    df = import_data(directory, file_name)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert 'Date' in df.columns
    assert 'Open' in df.columns
    assert 'High' in df.columns
    assert 'Low' in df.columns
    assert 'Close' in df.columns
    assert 'Adj Close' in df.columns
    assert 'Volume' in df.columns

def test_combine_dir_data(csv_fixture):
    path = os.path.join(data_directory, 'test_data')
    df = combine_dir_data(path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert 'Date' in df.columns
    assert 'Open' in df.columns
    assert 'High' in df.columns
    assert 'Low' in df.columns
    assert 'Close' in df.columns
    assert 'Adj Close' in df.columns
    assert 'Volume' in df.columns


def test_save_data(csv_fixture):
    df = pd.DataFrame({'Symbol': ['ABC', 'DEF'], 'Value': [10, 20]})
    file_name = 'test_output'
    save_data(df, file_name)
    assert os.path.exists(os.path.join(data_ingestion_output_path, 'test_output.parquet'))
    os.remove(os.path.join(data_ingestion_output_path, 'test_output.parquet'))

