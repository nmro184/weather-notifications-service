from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
import threading
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)  # This will log all messages at DEBUG level and higher
logger = logging.getLogger(__name__)

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "weather_topic"

# Store messages in a list instead of blocking the Flask route
alerts = []
weather_data = {}

def consume_kafka():
    """Background thread that continuously consumes Kafka messages."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    for message in consumer:
        logger.debug(f"Consumed message from Kafka: {message.value}")  # Log received message
        weather_data = message.value
        condition = weather_data["weather"][0]["main"]
        if condition in ["Rain", "Storm"]:
            alert_message = f"Weather Alert: {condition}!"
            alerts.append({"alert": alert_message})
            logger.info(f"Alert generated: {alert_message}")  # Log the alert generated
        logger.debug(f"Weather data processed: {weather_data}")  # Log the processed data

# Start Kafka consumer in a separate thread
thread = threading.Thread(target=consume_kafka, daemon=True)
thread.start()

@app.route("/health", methods=["GET"])
def health_check():
    """Check if the service is running."""
    logger.info("Health check request received.")  # Log health check requests
    return jsonify({"status": "Notification Service is running"})

@app.route("/alerts", methods=["GET"])
def get_alerts():
    """Return the latest alerts (if any)."""
    if alerts:
        logger.debug(f"Returning alerts: {alerts}")  # Log alerts being returned
        return jsonify(alerts)
    logger.debug("No alerts to return.")  # Log when no alerts are available
    return jsonify({"message": "No alerts yet"})

@app.route("/weather", methods=["GET"])
def get_weather():
    """Return the weather condition and temperature."""
    if weather_data:
        condition = weather_data["weather"][0]["main"]
        temperature = weather_data["main"]["temp"] - 273.15  # Convert from Kelvin to Celsius
        weather_status = "clear" if condition not in ["Rain", "Storm"] else "raining"
        
        logger.debug(f"Returning weather data: {weather_status}, {temperature}°C")  # Log the weather being returned
        return jsonify({
            "weather": weather_status,
            "temperature": temperature
        })
    logger.debug("No weather data to return.")  # Log when no weather data is available
    return jsonify({"message": "No weather data yet"})

if __name__ == "__main__":
    logger.info("Starting weather-notifications-service on port 5002")  # Log when the service starts
    app.run(host="0.0.0.0", port=5002, debug=True)
