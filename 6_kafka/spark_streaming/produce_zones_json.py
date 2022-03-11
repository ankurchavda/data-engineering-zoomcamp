import csv
from datetime import datetime, timedelta
from json import dumps
from kafka import KafkaProducer
from time import sleep
from datetime import datetime

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x, default=str).encode('utf-8'))

file = open('../avro/data/zones.csv')

csvreader = csv.reader(file)
header = next(csvreader)

for row in csvreader:
    key = {"locationId": int(row[0])}
    
    value = {"locationId": int(row[0]),
            "borough": str(row[1]),
            "zone": str(row[2]),
            "service_zone": str(row[3]),
            "zones_datetime" : datetime.now() + timedelta(minutes=10)
    }
    
    producer.send('zones.json', value=value, key=key)
    print("producing")
    sleep(1)