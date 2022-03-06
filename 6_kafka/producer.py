from ensurepip import bootstrap
from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=["localhost:9092"], 
                        value_serializer=lambda x : dumps(x).encode('utf-8'))

for num in range(1000):
    data = {'number': num}
    producer.send('demo_1', value=data)
    print("producing")
    sleep(1)
