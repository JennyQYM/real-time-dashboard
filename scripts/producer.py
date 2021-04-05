# coding: utf-8
import json
from kafka.producer import KafkaProducer
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda m: json.dumps(m).encode("utf-8"))


while True:
    cur = {"CreatedTime":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"Type":random.choice(["M","F"])}
    print(cur)
    producer.send('test', cur)
    time.sleep(1)
