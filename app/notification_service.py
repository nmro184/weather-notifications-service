from flask import Flask, jsonify
from kafka import KafkaConsumer
import json
import threading
import os
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "weather_topic"

alerts = []
weather_data = {}

def consume_kafka():
    """Background thread that continuously consumes Kafka messages."""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            group_id="weather-service-group",
            auto_offset_reset='earliest',  # Start from the earliest message if needed
            enable_auto_commit=True,  # Enable automatic offset commit
        )

        for message in consumer:
            logger.debug(f"Consumed message from Kafka: {message.value}")

            if 'weather' in message.value and 'main' in message.value['weather'][0]:
                weather_condition = message.value['weather'][0]['main']
                temperature = message.value['main']['temp'] - 273.15  # Convert from Kelvin to Celsius

                logger.debug(f"Weather condition: {weather_condition}, Temperature: {temperature}°C")

                # Check for specific weather conditions to send alerts
                if weather_condition in ['Rain', 'Storm']:
                    alert_message = f"Weather Alert: {weather_condition}!"
                    alerts.append({"alert": alert_message})
                    logger.info(f"Alert generated: {alert_message}")

                # Save the latest weather data
                global weather_data
                weather_data = message.value  # Update global weather data

                # Commit the offset after processing the message
                consumer.commit()

            else:
                logger.error("Invalid weather data structure received!")

    except Exception as e:
        logger.error(f"Error in consuming Kafka messages: {e}")

# Start Kafka consumer in a separate thread
thread = threading.Thread(target=consume_kafka, daemon=True)
thread.start()

@app.route("/health", methods=["GET"])
def health_check():
    """Check if the service is running."""
    logger.info("Health check request received.")
    return jsonify({"status": "Notification Service is running"})

@app.route("/alerts", methods=["GET"])
def get_alerts():
    """Return the latest alerts (if any)."""
    if alerts:
        logger.debug(f"Returning alerts: {alerts}")
        return jsonify(alerts)
    logger.debug("No alerts to return.")
    return jsonify({"message": "No alerts yet"})

@app.route("/weather", methods=["GET"])
def get_weather():
    """Return the weather condition and temperature."""
    if weather_data:
        condition = weather_data["weather"][0]["main"]
        temperature = weather_data["main"]["temp"] - 273.15  # Convert from Kelvin to Celsius
        weather_status = "clear" if condition not in ["Rain", "Storm"] else "raining"
        
        logger.debug(f"Returning weather data: {weather_status}, {temperature}°C")
        return jsonify({
            "weather": weather_status,
            "temperature": temperature
        })
    logger.debug("No weather data to return.")
    return jsonify({"message": "No weather data yet"})

if __name__ == "__main__":
    logger.info("Starting weather-notifications-service on port 5002")
    app.run(host="0.0.0.0", port=5002, debug=True)
