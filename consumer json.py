import json
from confluent_kafka import Consumer, KafkaException

# Configuration for connecting to your Kafka service on Aiven
config = {
    'bootstrap.servers': 'kafka-2bba8fa-mohab00-d674.e.aivencloud.com:25944',  # e.g., 'kafka-12345.aivencloud.com:12345'
    'security.protocol': 'SSL',
    'ssl.certificate.location': r'C:\Users\adelm\Desktop\Avien Interview\Kafka\service.cert',  # Path to your SSL certificate
    'ssl.key.location': r'C:\Users\adelm\Desktop\Avien Interview\Kafka\service.key',          # Path to your SSL key
    'ssl.ca.location': r'C:\Users\adelm\Desktop\Avien Interview\Kafka\ca (1).pem',            # Path to your CA certificate
    'group.id': 'my_consumer_group',  # Unique identifier for the consumer group
    'auto.offset.reset': 'earliest'   # Start reading at the earliest available message
}

# Initialize the consumer
consumer = Consumer(config)

# Specify the Kafka topic to subscribe to
topic = 'my_iot_topic'
consumer.subscribe([topic])

def consume_messages():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Process the message
                key = json.loads(msg.key().decode('utf-8'))
                value = json.loads(msg.value().decode('utf-8'))
                print(f"Consumed message from topic {msg.topic()}: key = {key}, value = {value}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets
        consumer.close()

# Start consuming messages
consume_messages()
