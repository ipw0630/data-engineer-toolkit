# filename: kafka/scripts/arsitektur_consumer.py
# run: python kafka/scripts/arsitektur_consumer.py

from confluent_kafka import Consumer

# Konfigurasi consumer
consumer = Consumer({
   "bootstrap.servers": "localhost:9092",
   "group.id"         : "dibimbing",
})

# Subscribe ke topik
consumer.subscribe(["arsitektur-kafka"])

try:
   while True:
       # consume pesan
       msg = consumer.poll(timeout=1)  # Tunggu pesan selama 1 detik

       # Print isi pesan
       if msg:
           print(f"Received message: {msg.value().decode()}")

finally:
   # Close consumer
   consumer.close()