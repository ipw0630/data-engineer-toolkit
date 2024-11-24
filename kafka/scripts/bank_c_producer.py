# filename: kafka/scripts/bank_c_producer.py
# run: python kafka/scripts/bank_c_producer.py

import json
import time
import random
from datetime import datetime
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
       topic = "transaksi-bank-c",
       value = json.dumps({
           "user_id" : random.choice(range(100)),
           "amount"  : int(random.uniform(100_000, 10_000_000)) * (1000 if random.random() > 0.95 else 1),
           "datetime": str(datetime.fromtimestamp(datetime(2024, 1, 1).timestamp() + 5000 * i)),
       }),
       callback = delivery_report,
   )

   producer.flush()
   time.sleep(0.5)