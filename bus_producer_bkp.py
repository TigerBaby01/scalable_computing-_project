import time
import json
import requests
from kafka import KafkaProducer
from google.transit import gtfs_realtime_pb2  # Decodes the binary protobuf stream
 
# Core Architecture Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'dublin-bus-delays'
 
# Official NTA Production Endpoint for live Trip Updates
NTA_API_URL = 'https://api.nationaltransport.ie/gtfsr/v2/TripUpdates'
HEADERS = {
    'Cache-Control': 'no-cache',
    'x-api-key': 'b2df814e137e4242ba6ce002d7a5c2d1'  # Paste your NTA subscription key here
}
 
def json_serializer(data):
    return json.dumps(data).encode('utf-8')
 
# Initialize Production Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=json_serializer
)
 
print("Connecting to Transport for Ireland Live GTFS-R Feed...")
print(f"Streaming authenticated live data into Kafka topic: '{TOPIC_NAME}'...\n")
 
def fetch_live_bus_data():
    while True:
        try:
            # Poll the NTA feed
            response = requests.get(NTA_API_URL, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                # Parse the binary protobuf payload into readable structures
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)
                records_sent = 0
                for entity in feed.entity:
                    if entity.HasField('trip_update'):
                        trip_update = entity.trip_update
                        route_id = trip_update.trip.route_id
                        trip_id = trip_update.trip.trip_id
                        # Loop through stop execution updates to find delay vectors
                        for stop_time_update in trip_update.stop_time_update:
                            # Capture departure delay if present, otherwise fall back to arrival delay
                            delay_seconds = 0
                            if stop_time_update.HasField('departure'):
                                delay_seconds = stop_time_update.departure.delay
                            elif stop_time_update.HasField('arrival'):
                                delay_seconds = stop_time_update.arrival.delay
                            # Construct the clean metrics structure for our analytics layers
                            payload = {
                                "timestamp": int(time.time()),
                                "route_id": str(route_id),
                                "trip_id": str(trip_id),
                                "stop_id": str(stop_time_update.stop_id),
                                "delay_seconds": int(delay_seconds)
                            }
                            # Publish transaction securely to Kafka
                            producer.send(TOPIC_NAME, value=payload)
                            records_sent += 1
                print(f"[LIVE INGESTION] Successfully decoupled and sent {records_sent} telemetry points to Kafka.")
            else:
                print(f"[API ERROR] NTA Portal responded with status code: {response.status_code}")
        except Exception as e:
            print(f"[CRITICAL FAILURE] Pipeline exception caught: {str(e)}")
        # The NTA fair use/throttling policy requests polling intervals of 30-60s
        time.sleep(30)
 
if __name__ == "__main__":
    fetch_live_bus_data()
