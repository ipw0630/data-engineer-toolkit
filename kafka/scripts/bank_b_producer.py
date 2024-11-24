# filename: kafka/scripts/bank_b_producer.py
# run: python kafka/scripts/bank_b_producer.py

import json
import time
import random
import string
from confluent_kafka import Producer

producer = Producer({
   "bootstrap.servers": "localhost:9092",
})

def delivery_report(err, msg):
   if err is not None:
       print(f"Pesan tidak terkirim: {err}")
   else:
       print(f"Pesan terkirim ke {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

for i in range(100):
   producer.produce(
       topic = "transaksi-bank-b",
       value = json.dumps({
           "user_id": "".join(random.choices(string.ascii_lowercase, k=3)),
           "amount" : int(random.uniform(100_000, 10_000_000)) * (1000 if random.random() > 0.95 else 1),
       }),
       callback = delivery_report,
   )

   producer.flush()
   time.sleep(0.5)