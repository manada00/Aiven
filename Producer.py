import json
import uuid
import random
from datetime import datetime
from confluent_kafka import Producer

# Configuration for connecting to your Kafka service on Aiven
config = {
    'bootstrap.servers': 'kafka-2bba8fa-mohab00-d674.e.aivencloud.com:25944',  # e.g., 'kafka-12345.aivencloud.com:12345'
    'security.protocol': 'SSL',
    'ssl.certificate.location': r'C:\Users\adelm\Desktop\Avien Interview\Kafka\service.cert',  # Path to your SSL certificate
    'ssl.key.location': r'C:\Users\adelm\Desktop\Avien Interview\Kafka\service.key',          # Path to your SSL key
    'ssl.ca.location': r'C:\Users\adelm\Desktop\Avien Interview\Kafka\ca (1).pem',       # Path to your CA certificate
}

# Initialize the producer
producer = Producer(config)

def delivery_report(err, msg):
    """Callback for reporting delivery success or failure."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_message():
    # Generate a unique key using UUID
    key = str(uuid.uuid4())

    # Create a payload with mock IoT sensor data
    payload = {
        'event_type': 'iot_sensor_reading',
        'sensor_id': random.randint(1, 1000),
        'temperature': round(random.uniform(-20.0, 50.0), 2),
        'humidity': round(random.uniform(0.0, 100.0), 2),
        'timestamp': datetime.now().isoformat()
    }

    return key, payload

# Specify the Kafka topic
topic = 'my_iot_topic'

# Produce and send messages
for _ in range(20):
    key, value = generate_message()
    producer.produce(
        topic,
        key=json.dumps(key),
        value=json.dumps(value),
        callback=delivery_report
    )
    producer.poll(0)

# Ensure all messages are sent
producer.flush()
