from flask import Flask, request, jsonify
import joblib
import numpy as np
import concurrent.futures
import os

app = Flask(__name__)

def return_prediction(model, x_hat):
    prediction = model.predict(x_hat)
    return prediction

#loading the ML model
#path is initially set to shared Docker volume for pipeline reproducibility.
path = os.path.join('app','ml-models','lightgbm_predictor.joblib')
if os.path.exists(path):
    model = joblib.load(path)
else:
    #this will default to presaved model for web hosting.
    model = joblib.load(os.path.join("ml-model", "lightgbm_predictor.joblib"))
executor = concurrent.futures.ThreadPoolExecutor()

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return 'OK', 200

@app.route("/predict", methods=["GET", "POST"])
def prediction():
    # Get the query parameters
    vol_moving_avg = request.args.get('vol_moving_avg')
    adj_close_rolling_med = request.args.get('adj_close_rolling_med')

    try:
        # Validate the parameters
        if vol_moving_avg is None or adj_close_rolling_med is None:
            return jsonify({'error': 'Invalid query parameters: A parameter is missing]'}), 400
        vol_moving_avg = float(vol_moving_avg)
        adj_close_rolling_med = float(adj_close_rolling_med)
    except ValueError:
        return jsonify({'error': 'Invalid query parameters: arguments must be float'}), 400
    
    # Create the input array and get the prediction asynchronously
    input_array = np.array([[vol_moving_avg, adj_close_rolling_med]])
    future = executor.submit(return_prediction, model, input_array)
    prediction = future.result()[0]
    
    # Return the prediction
    return jsonify({'Volume Prediction': prediction}), 200

# Default 404 path
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

if __name__ == '__main__':
    app.run()