# filename: kafka/scripts/arsitektur_group_producer.py
# run: python kafka/scripts/arsitektur_group_producer.py

import time
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
for i in range(100):
   producer.produce(
       topic    = "arsitektur-kafka",
       key      = str(i),
       value    = f"Halo Kafka ke-{i}",
       callback = delivery_report,
   )

   # Tunggu sampai pesan terkirim
   producer.flush()

   time.sleep(0.5)