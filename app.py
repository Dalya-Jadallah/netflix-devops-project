from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import logging
import os
from datetime import datetime

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Get Kafka server from environment variable (with fallback)
KAFKA_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
producer = None

def get_kafka_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logging.info(f"✅ Connected to Kafka at {KAFKA_SERVER}")
        except Exception as e:
            logging.error(f"❌ Kafka connection failed: {e}")
            producer = None
    return producer

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

@app.route('/event', methods=['POST'])
def receive_event():
    try:
        event = request.get_json()
        if not event:
            return jsonify({"error": "No JSON data provided"}), 400
        
        event['processed_timestamp'] = str(datetime.now())
        
        kafka_producer = get_kafka_producer()
        if kafka_producer:
            kafka_producer.send('user-clicks', value=event)
            kafka_producer.flush()
            logging.info(f"📨 Event sent to Kafka: {event.get('user_id')} -> {event.get('movie_id')}")
        else:
            logging.warning("⚠️ Kafka not available, event logged only")
        
        return jsonify({"status": "received", "event_id": event.get('session_id')}), 200
    except Exception as e:
        logging.error(f"Error processing event: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/events/batch', methods=['POST'])
def receive_batch():
    try:
        events = request.get_json()
        if not isinstance(events, list):
            return jsonify({"error": "Expected list of events"}), 400
        
        kafka_producer = get_kafka_producer()
        count = 0
        
        for event in events:
            if kafka_producer:
                event['processed_timestamp'] = str(datetime.now())
                kafka_producer.send('user-clicks', value=event)
                count += 1
        
        if kafka_producer:
            kafka_producer.flush()
        
        logging.info(f"📦 Batch of {count} events sent to Kafka")
        return jsonify({"status": "received", "count": count}), 200
    except Exception as e:
        logging.error(f"Batch error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
