import csv
from time import sleep

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

def load_avro_schema_from_file():
    key_schema = avro.load("taxi_ride_key.avsc")
    value_schema = avro.load("taxi_ride_value.avsc")

    return key_schema, value_schema


def send_record():

    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": 1
    }

    producer = AvroProducer(
        producer_config,
        default_key_schema=key_schema,
        default_value_schema=value_schema
    )

    file = open('data/rides.csv')

    csv_reader = csv.reader(file)
    header = next(csv_reader)

    for row in csv_reader:
        key = {
            "vendorId" : int(row[0])
        }
        value = {
            "vendorId" : int(row[0]),
            "passenger_count" : int(row[3]),
            "trip_distance": float(row[4]),
            "payment_type": int(row[9]),
            "total_amount": float(row[16])
        }

        try:
            producer.produce(topic='yellow_taxi_rides', key=key, value=value)
        except Exception as err:
            print(f"Exception occurred while producing record value - {value}: {err}")
        else:
            print(f"Successfully produced record value - {value}")

        producer.flush()
        sleep(1)

if __name__== "__main__":
    send_record()