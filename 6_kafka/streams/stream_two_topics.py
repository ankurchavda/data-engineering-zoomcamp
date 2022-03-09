import faust
from taxi_rides_model import TaxiRide

app = faust.App('stream_first_topic', broker='kafka://localhost:9092')
topic =  app.topic('yellow_taxi_ride.json','yellow_taxi_ride.json.second', value_type=TaxiRide)

@app.agent(topic)
async def process_topic_1(stream):
    async for event in stream:
        print(event)


if __name__=="__main__":
    app.main()