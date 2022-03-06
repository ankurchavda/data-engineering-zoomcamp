from re import L
from confluent_kafka.avro import AvroConsumer

def read_messages():
    consumer_config={
        "bootstrap.servers" : "localhost:9092",
        "schema.registry.url" : "http://localhost:8081",
        "group.id" : "taxirides.avro.consumer.1",
        "auto.offset.reset" : "earliest"
    }

    consumer = AvroConsumer(
        consumer_config
    )

    consumer.subscribe(["yellow_taxi_rides"])

    while True:
        try:
            message = consumer.poll(5)
        except Exception as err:
            print(f"Exception occurred while trying to poll messages - {err}")
        else:
            if message:
                print(
                    f"Successfully polled a record from "
                    f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()} \n"
                    f"Message key: {message.key()} || Message Value: {message.value()}"
                )
                consumer.commit()
            else:
                print("No new messages at this point. Trying again later.")
    
    consumer.close()

if __name__ == "__main__":
    read_messages()