from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
import threading
import os

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")   
TOPIC = "weather_topic"

# Store messages in a list instead of blocking the Flask route
alerts = []
weather_data = {}

def consume_kafka():
    """Background thread that continuously consumes Kafka messages"""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    for message in consumer:
        weather_data = message.value
        condition = weather_data["weather"][0]["main"]
        if condition in ["Rain", "Storm"]:
            alerts.append({"alert": f"Weather Alert: {condition}!"})
        print(f"Received: {weather_data}")  # Debugging

# Start Kafka consumer in a separate thread
thread = threading.Thread(target=consume_kafka, daemon=True)
thread.start()

@app.route("/health", methods=["GET"])
def health_check():
    """Check if the service is running."""
    return jsonify({"status": "Notification Service is running"})

@app.route("/alerts", methods=["GET"])
def get_alerts():
    """Return the latest alerts (if any)"""
    if alerts:
        return jsonify(alerts)
    return jsonify({"message": "No alerts yet"})

@app.route("/weather", methods=["GET"])
def get_weather():
    """Return the weather condition and temperature"""
    if weather_data:
        condition = weather_data["weather"][0]["main"]
        temperature = weather_data["main"]["temp"] - 273.15  # Convert from Kelvin to Celsius
        weather_status = "clear" if condition not in ["Rain", "Storm"] else "raining"
        
        
        return jsonify({
            "weather": weather_status,
            "temperature": temperature
        })
    return jsonify({"message": "No weather data yet"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
