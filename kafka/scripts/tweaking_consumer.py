from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer

# Konfigurasi consumer
consumer = AvroConsumer({
   "bootstrap.servers"  : "localhost:9092",
   "group.id"           : "consumer-avro",
   "schema.registry.url": "http://localhost:8081",
})

# Subscribe to the topics
consumer.subscribe(["tweaking-kafka"])

# Define a fraud detection threshold
FRAUD_THRESHOLD = 50

try:
   while True:
       msg = consumer.poll(timeout=1)  # Wait for a message for up to 1 second

       if msg:
           # Extract the message value
           value = msg.value()
           key = msg.key()

           # Print the received message
           print(f"Received message: {value} (key: {key})")

           # Fraud detection logic: Flag as fraud if 'id' is above the threshold
           if value.get("id") > FRAUD_THRESHOLD:
               print(f"Fraud detected in message with id {value['id']}!")

finally:
   consumer.close()
