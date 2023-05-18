import pandas as pd
import pytest
import lightgbm as lgb

from scripts.train_model import train_model

def test_train_model():
    # Create a test DataFrame
    data = {
        'Date': ['2021-01-01', '2021-01-02', '2021-01-03'],
        'vol_moving_avg': [100, 200, 300],
        'adj_close_rolling_med': [50.0, 51.0, 52.0],
        'Volume': [1000, 2000, 3000]
    }
    df = pd.DataFrame(data)

    # Call the train_model function
    result = train_model(df)

    # Assert the result
    assert isinstance(result, list)
    assert len(result) == 3
    assert isinstance(result[0], lgb.LGBMRegressor)
    assert isinstance(result[1], float)
    assert isinstance(result[2], float)
