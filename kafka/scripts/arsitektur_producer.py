# filename: kafka/scripts/arsitektur_producer.py
# run: python kafka/scripts/arsitektur_producer.py

from confluent_kafka import Producer

# Konfigurasi producer
producer = Producer({
   "bootstrap.servers": "localhost:9092",
})

# Callback untuk memeriksa status pengiriman
def delivery_report(err, msg):
   if err is not None:
       print(f"Pesan tidak terkirim: {err}")
   else:
       print(f"Pesan terkirim ke {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# Kirim pesan
producer.produce(
   topic    = "arsitektur-kafka",
   value    = "Halo Kafka!",
   callback = delivery_report,
)

# Tunggu sampai pesan terkirim
producer.flush()