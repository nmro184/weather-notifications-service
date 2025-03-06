# Use the Python 3.10 image
FROM python:3.10

# Set the working directory in the container to /app
WORKDIR /app

# Copy the requirements.txt file into the container at /app
COPY app/requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire app folder into the container at /app
COPY . .

# Set the command to run the Flask app using Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:5002", "app.notification_service:app"]
