import faust
from taxi_rides_model import TaxiRide

app = faust.App("count.vendors", broker="kafka://localhost:9092")
topic = app.topic("yellow_taxi_ride.json", value_type=TaxiRide)

vendor_rides = app.Table("vendor_rides", default=int) #default int returns 0, so a new key is initialized with value of zero

@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.vendorId):
        vendor_rides[event.vendorId] += 1

if __name__ == "__main__":
    app.main()