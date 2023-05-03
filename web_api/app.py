from flask import Flask, request
import joblib
import numpy as np

app = Flask(__name__)

def return_prediction(model, x_hat):
    prediction = model.predict(x_hat)
    return prediction

model = joblib.load('random-forest_predictor.jolib')

@app.route("/predict", methods=["GET"])
def prediction():
    vol_moving_avg = request.args.get('vol_moving_avg')
    adj_close_rolling_med = request.args.get('adj_close_rolling_med')
    context = (
        np.array([[vol_moving_avg, adj_close_rolling_med]])
        .astype(float)
        )
    results = return_prediction(model, context)
    return str(results)

if __name__ == '__main__':
    app.run()