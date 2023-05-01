import os
import pandas as pd
import pyarrow as pa

data_directory = '../data/'
data_directory_output = 'feature_engineering'

#a general function that manupulates the data
def manipulate_data(operation, dataframe):
    return operation(dataframe)

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

def save_data(dataframe, filename):
    file_path = os.path.join(
        data_directory, 
        'data_directory_output', 
        filename + ".parquet"
        )
    table = pa.Table.from_pandas(dataframe)
    dataframe.to_parquet(file_path)
    

if __name__ == '__main__':
    table = pa.parquet.read_table()
    df = table.to_pandas()
    df = df.sort_values(by='Date')
    manipulate_data(calculate_volume_moving_average, df)
    manipulate_data(calculate_adj_rolling_median, df)
    save_data(df, 'manipulated_data')