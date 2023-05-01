import os
import requests
import pandas as pd
import numpy as np
import pyarrow as pa

data_directory = '../data/'
data_directory_output = 'processed'

def import_data(directory, filename):
    path = os.path.join(directory, filename)
    schema = pa.csv.read_csv(path).schema
    table = pa.csv.read_(path, schema=schema)
    df = table.to_pandas()
    df['Symbol'] = filename.replace('.csv', '')
    return df

def combine_dir_data(path):
    #load all data
    dfs = []
    for file in os.listdir(path):
        if file.endswith('.csv'):
            df = import_data(path, file)
            dfs.append(df)
    result = pd.concat(dfs, ignore_index=True)
    return result

def save_data(dataframe, filename):
    file_path = os.path.join(
        data_directory, 
        data_directory_output, 
        filename + ".parquet"
        )
    dataframe.to_parquet(file_path)

if __name__ == '__main__':
    etfs_data_path = os.path.join(data_directory, 'raw', 'etfs')
    stocks_data_path = os.path.join(data_directory, 'raw', 'stocks')
    pd_etfs_data = combine_dir_data(etfs_data_path)
    pd_stocks_data = combine_dir_data(stocks_data_path)
    result = pd.concat([pd_etfs_data, pd_stocks_data], ignore_index=True)
    save_data(result, 'processed_data')





