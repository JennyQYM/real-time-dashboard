from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('result', bootstrap_servers=['localhost:9092'], api_version='4.2.0')
for msg in consumer:
    #print((msg.value).decode('utf8'))
    print(json.loads(msg.value.decode('utf8'))['TotalMale'])
