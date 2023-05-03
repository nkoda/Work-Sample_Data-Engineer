from flask import Flask, request, jsonify
import joblib
import numpy as np

app = Flask(__name__)

def return_prediction(model, x_hat):
    prediction = model.predict(x_hat)
    return prediction

#loading the ML model
model = joblib.load('random-forest_predictor.jolib')

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    return 'OK', 200

@app.route("/predict", methods=["GET"])
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
    
    # Create the input array and get the prediction
    input_array = np.array([[vol_moving_avg, adj_close_rolling_med]])
    prediction = model.predict(input_array)[0]
    
    # Return the prediction
    return jsonify({'Volume Prediction': prediction}), 200

# Default 404 path
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

if __name__ == '__main__':
    app.run()