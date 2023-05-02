import os
import pandas as pd
from util.data_handling import import_csv_as_df, export_df_as_parquet

data_directory = '../data/'
data_preprocessing_output_path = os.path.join(data_directory, 'processed')

def import_data(directory, file_name):
    df = import_csv_as_df(directory, file_name)
    df['Symbol'] = file_name.replace('.csv', '')
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

if __name__ == '__main__':
    etfs_data_path = os.path.join(data_directory, 'raw', 'etfs')
    stocks_data_path = os.path.join(data_directory, 'raw', 'stocks')
    pd_etfs_data = combine_dir_data(etfs_data_path)
    pd_stocks_data = combine_dir_data(stocks_data_path)
    result = pd.concat([pd_etfs_data, pd_stocks_data], ignore_index=True)
    export_df_as_parquet(result, 'processed', 'preprocessed_data')





