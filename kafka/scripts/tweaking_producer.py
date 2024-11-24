# filename: kafka/scripts/tweaking_consumer.py
# run: python kafka/scripts/tweaking_consumer.py

import json
import time
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

key_schema = {
   "namespace": "example.avro",
   "type"     : "record",
   "name"     : "Key",
   "fields"   : [
       {"name": "key_id", "type": "int"}
   ]
}

value_schema = {
   "namespace": "example.avro",
   "type"     : "record",
   "name"     : "Value",
   "fields"   : [
       {"name": "id", "type": "int"},
       {"name": "name", "type": "string"}
   ]
}

producer = AvroProducer(
   config={
       "bootstrap.servers"  : "localhost:9092",
       "schema.registry.url": "http://localhost:8081",
   },
   default_key_schema   = avro.loads(json.dumps(key_schema)),
   default_value_schema = avro.loads(json.dumps(value_schema)),
)

def delivery_report(err, msg):
   if err is not None:
       print(f"Pesan tidak terkirim: {err}")
   else:
       print(f"Pesan terkirim ke {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

# Loop to send 100 messages
for i in range(100):
   key = {"key_id": i}
   value = {"id": i, "name": f"message {i}"}

   # Add fraud detection logic: consider fraudulent if id is even
   if value["id"] % 2 == 0:
       print(f"Fraud detected in message with id {value['id']}")

   producer.produce(
       topic    = "tweaking-kafka",
       key      = key,
       value    = value,
       callback = delivery_report,
   )

   producer.flush()
   time.sleep(2.5)