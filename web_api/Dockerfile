FROM python:3.9

WORKDIR /app

# Install dependencies
COPY requirements_web-app.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY lightgbm_predictor.joblib lightgbm_predictor.joblib
# Copy the web_app files into the container
COPY app.py /

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=development
ENV PORT=5000

# Expose the port that Flask will run on
EXPOSE $PORT

# Start the Flask development server with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:"$PORT, "app:app"]
