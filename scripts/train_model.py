import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from util.data_handling import import_parquet_as_df
import joblib
import logging
import os

from data_augmentation import data_augmentation_output_path
model_destination_path = os.path.join('..','web_api','ml_model')

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def train_model(data):
    logging.info(f"Initializing model training.")
    # Assume `data` is loaded as a Pandas DataFrame
    # data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)

    # Remove rows with NaN values
    data.dropna(inplace=True)
    logging.info("Separating data for training.")
    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = data[features]
    y = data[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logging.info("Instantiating a Random Forest Regressor.")
    # Create a RandomForestRegressor model
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    logging.info("Training the model with the data.")
    # Train the model
    model.fit(X_train, y_train)

    logging.info("Assessing model performance.")
    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    logging.info(f"Finished model training.")
    return [model, mae, mse]


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
        logger.info(f"Attempting to access {file_name}.parquet")
        df = import_parquet_as_df(data_augmentation_output_path, file_name)
        logger.info(f"Successfully retrieved {file_name}.parquet")
        return df
    except Exception as e:
        logger.error(f"Failed to open {file_name}.parquet. Error message: {str(e)}")

def save_model(model):
    try:
        logger.info(f"Attempting to save model to {model_destination_path}") 
        path = os.path.join(model_destination_path, 'random-forest_predictor.jolib')
        with open(path, 'wb') as f:
            joblib.dump(model, f)
    except Exception as e:
        logger.error(f"Failed to save ml model to {path}. Error - {e}")

def log_model_metrics(mae, mse):
    logging.info(f"Random Forest Model's Mean Absolute Error: {mae}")
    logging.info(f"Random Forest Model's Mean Squared Error: {mse}")

if __name__ == '__main__':
    logger.info("Initializing ML model training process.")
    dataframe = read_data('augmented_data')
    [model, mae, mse] = train_model(dataframe)
    log_model_metrics(mae, mse)
    save_model(model)
    logger.info("Finished training model.")

    


