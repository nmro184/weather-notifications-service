from flask import Flask, jsonify
from kafka import KafkaConsumer
import json

app = Flask(__name__)

KAFKA_BROKER = "localhost:9092"  # Update if needed
TOPIC = "weather_topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

@app.route("/health", methods=["GET"])
def health_check():
    """Check if the service is running."""
    return jsonify({"status": "Notification Service is running"})

@app.route("/alerts", methods=["GET"])
def get_alerts():
    """Fetch alerts from Kafka messages in real time."""
    for message in consumer:
        weather_data = message.value
        condition = weather_data["weather"][0]["main"]
        if condition in ["Rain", "Storm"]:
            return jsonify({"alert": f"Weather Alert: {condition}!"})
    return jsonify({"message": "No alerts"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
