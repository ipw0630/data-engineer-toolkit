# filename: kafka/scripts/bank_central_consumer.py
# run: python kafka/scripts/bank_central_consumer.py

import json
from confluent_kafka import Consumer
from datetime import datetime

consumer = Consumer({
   "bootstrap.servers": "localhost:9092",
   "group.id"         : "bank-central-fraud-analysis",
})

consumer.subscribe(["transaksi-bank-a", "transaksi-bank-b", "transaksi-bank-c"])

try:
   while True:
       msg = consumer.poll(timeout=1)  # Tunggu pesan selama 1 detik

       if msg:
           transaction = json.loads(msg.value().decode())
           log = "{}: {}: {}".format(datetime.fromtimestamp(msg.timestamp()[1]/1000), msg.topic(), transaction)

           if transaction["amount"] > 1_000_000_000:
               print("############################### indicated fraud ###############################")
               print(log)
               print("###############################################################################")
           else:
               print(log)

finally:
   consumer.close()