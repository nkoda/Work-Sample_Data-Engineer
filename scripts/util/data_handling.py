import os
import pandas as pd
import pyarrow as pa

data_directory = '../util/data/'

def path(parent_directory, file_name, extension):
    return os.path.join(
        data_directory,
        parent_directory,
        file_name + extension
    )

def import_csv_as_df(parent_directory, file_name):
    file_path = path(parent_directory, file_name, '.csv')
    schema = pa.csv.read_csv(file_path).schema
    table = pa.csv.read_(file_path, schema=schema)
    df = table.to_pandas()
    return df

def export_df_as_parquet(dataframe, parent_directory, file_name):
    file_path = path(parent_directory, file_name, '.parquet')
    table = pa.Table.from_pandas(dataframe)
    pa.parquet.write_table(table, file_path)

