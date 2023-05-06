from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
from util.data_handling import import_parquet_as_df
import lightgbm as lgb
import joblib
import logging
import os
import time

from data_augmentation import data_augmentation_output_path
dir_path = os.path.dirname(os.path.realpath(__file__))
data_directory = os.path.join(dir_path, '..', 'data')
model_destination_path = os.path.join(dir_path, '..', 'web_api', 'ml-model')

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# add file handler
# Get the directory of the current file
current_dir = os.path.dirname(os.path.abspath(__file__))
# Construct the path to the logs directory
logs_dir = os.path.join(current_dir, "../logs")
log_file_path = os.path.join(logs_dir, "train_model.log")
fh = logging.FileHandler(log_file_path)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

def train_model(data):
    """Trains a gradient boosting regressor model using the provided dataset and returns the 
    trained model, along with its mean absolute error (MAE) and mean squared error (MSE) on a test set.

    Args:
        data (pandas.DataFrame): A pandas DataFrame containing the dataset. Must include the 
                                 columns 'Date', 'vol_moving_avg', 'adj_close_rolling_med', and 'Volume'.

    Returns:
        list: A list containing the trained LightGBM model, its MAE, and its MSE.

    Raises:
        ValueError: If the 'Date', 'vol_moving_avg', 'adj_close_rolling_med', or 
                    'Volume' columns are not present in the input data.
        ValueError: If any NaN values are present in the input data.

    Example:
        # Load the data into a pandas DataFrame
        data = pd.read_csv('my_dataset.csv')
        
        # Train the model
        model, mae, mse = train_model(data)
    """

    logger.info(f"Initializing model training.")
    # Assume `data` is loaded as a Pandas DataFrame
    data.set_index('Date', inplace=True)

    # Remove rows with NaN values
    data.dropna(inplace=True)
    logger.info("Separating data for training.")
    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = data[features]
    y = data[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logger.info("Instantiating a Gradient Boosting Regressor.")
    # Train the LightGBM model
    model_params = {
        'boosting_type':'gbdt',
        'num_leaves':31,
        "max_depth":-1,
        "learning_rate":0.1,
        "n_estimators":500,
    }
    model = lgb.LGBMRegressor(**model_params)

    # Convert probabilities to binary predictions
    logger.info("Training the model with the data.")
    # Train the model
    model.fit(X_train, y_train)

    logger.info("Assessing model performance.")
    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    logger.info(f"Finished model training.")
    return [model, mae, mse]


def read_data(file_name):
    """Reads a parquet file with the given name and returns a pandas DataFrame.
    
    If an exception is raised, logs an error message.
    Otherwise, logs a success message with the name of the parquet file.
    
    Args:
        file_name: The name of the parquet file to read.

    Raises:
        Exception: If the given parquet cannot be retrieved.
    
    Returns:
        The pandas DataFrame read from the parquet file.
    """
    try:
        logger.info(f"Attempting to access {file_name}.parquet")
        df = import_parquet_as_df(data_augmentation_output_path, file_name)
        logger.info(f"Successfully retrieved {file_name}.parquet")
        logger.info(f"{file_name}.parquet statistics: ")
        logger.info(df.describe().to_string())
        return df
    except Exception as e:
        logger.error(f"Failed to open {file_name}.parquet. Error message: {str(e)}")

def save_model(model):
    """
    Save a trained model using joblib.dump.

    Args:
        model: A trained machine learning model.
    
    Raises:
        Exception: If the model cannot be saved as a joblib file.

    Returns:
        None
    """
    try:
        logger.info(f"Attempting to save model to {model_destination_path}") 
        path = os.path.join(model_destination_path, 'lightgbm_predictor.joblib')
        with open(path, 'wb') as f:
            joblib.dump(model, f)
    except Exception as e:
        logger.error(f"Failed to save ml model to {path}. Error - {e}")

def log_model_metrics(mae, mse):
    """ Log the mean absolute error and mean squared error of a trained model.

    Args:
        mae : float
            The mean absolute error of the model.
        mse : float
            The mean squared error of the model.

    Returns:
        None
    """
    logger.info(f"Random Forest Model's Mean Absolute Error: {mae}")
    logger.info(f"Random Forest Model's Mean Squared Error: {mse}")

def deploy_model():
    """Airflow callable function to train then deploy model.

    Returns:
        None
    """
    logger.info("Initializing ML model training process.")
    start_time = time.time()
    dataframe = read_data('augmented_data')
    model, mae, mse = train_model(dataframe)
    log_model_metrics(mae, mse)
    save_model(model)
    elapsed_time = time.time() - start_time
    logger.info(f"Finished training model. Elapsed time: {elapsed_time:.2f}")