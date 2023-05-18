import os
import pandas as pd
import pyarrow as pa
from pyarrow import parquet
import pytest
from scripts.util.data_handling import (
    import_csv_as_df,
    import_parquet_as_df,
    path,
    export_df_as_parquet,
    validate_data_types
)

# Assuming your test file is in the same directory as the utility functions

current_dir = os.path.dirname(os.path.abspath(__file__))
data_directory = os.path.join(current_dir, '..', 'data')

etf_df = pd.DataFrame({
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

@pytest.fixture
def csv_fixture():
    # Set up a CSV file for testing
    csv_path = os.path.join(data_directory, 'test_data', 'toy-etf.csv')
    df = etf_df
    df.to_csv(csv_path, index=False)
    yield csv_path
    # Teardown - remove the created CSV file
    os.remove(csv_path)

@pytest.fixture
def parquet_fixture():
    # Set up a Parquet file for testing
    parquet_path = os.path.join(data_directory, 'test_data', 'data.parquet')
    df = etf_df
    table = pa.Table.from_pandas(df)
    parquet.write_table(table, parquet_path)
    yield parquet_path
    # Teardown - remove the created Parquet file
    os.remove(parquet_path)

def test_path(csv_fixture):
    assert os.path.exists(path('test_data', 'toy-etf', '.csv'))

def test_import_csv_as_df(csv_fixture):
    df = import_csv_as_df('test_data', 'toy-etf')
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert 'Date' in df.columns
    assert 'Open' in df.columns
    assert 'High' in df.columns
    assert 'Low' in df.columns
    assert 'Close' in df.columns
    assert 'Adj Close' in df.columns
    assert 'Volume' in df.columns

def test_validate_data_types():
    df = pd.DataFrame({'Date': ['2023-01-01'], 'Value': [10]})
    validated_df = validate_data_types(df)
    assert isinstance(validated_df, pd.DataFrame)
    assert validated_df['Date'].dtype == 'datetime64[ns]'

def test_import_parquet_as_df(parquet_fixture):
    df = import_parquet_as_df('test_data', 'data')
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5
    assert 'Date' in df.columns
    assert 'Open' in df.columns
    assert 'High' in df.columns
    assert 'Low' in df.columns
    assert 'Close' in df.columns
    assert 'Adj Close' in df.columns
    assert 'Volume' in df.columns

def test_export_df_as_parquet():
    df = pd.DataFrame({'Date': ['2023-01-01', '2023-01-02'], 'Value': [10, 20]})
    export_df_as_parquet(df, 'test_data', 'exported_data')
    exported_path = os.path.join(data_directory, 'test_data', 'exported_data.parquet')
    assert os.path.exists(exported_path)
    os.remove(exported_path)