import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
 
# Define the full Dublin Bus network to match your frontend dashboard
ROUTES = [
    "1", "4", "7", "7A", "7B", "7D", "9", "11", "13", "14", "15", "15A", "15B", "15D", "16", "16D", 
    "26", "27", "27A", "27B", "27X", "32", "32X", "33", "33A", "33X", "37", "38", "38A", "38B", "38D", 
    "39", "39A", "39X", "40", "40B", "40D", "41", "41B", "41C", "41D", "41X", "42", "42X", "43", "44", 
    "44B", "46A", "46E", "47", "49", "53", "54A", "56A", "61", "65", "65B", "68", "68A", "69", "69X", 
    "70", "70X", "77A", "77X", "79", "79A", "83", "83A", "84", "84A", "84X", "99", "120", "122", "123", 
    "130", "140", "142", "145", "150", "151", "155", "C1", "C2", "C3", "C4", "C5", "C6", "G1", "G2", 
    "H1", "H2", "H3", "P29", "X25", "X26", "X27", "X28", "X30", "X31", "X32"
]
 
print("[*] Initializing Live Dublin Bus Data Ingestion Stream...")
 
# Connect to the local Kafka broker running on your EC2 instance
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1 # Ensure broker registers packet receipt
    )
    print("[+] Successfully linked to Kafka Broker on port 9092.")
except Exception as e:
    print(f"[X] Connection Error: Could not connect to Kafka broker. Is it running? Details: {e}")
    exit(1)
 
topic_name = "dublin-bus-delays"
print(f"[*] Streaming data live into Kafka Topic: '{topic_name}'")
print("[*] Press CTRL + C to stop the ingestion loop.")
print("-" * 50)
 
try:
    while True:
        # Pick a random route from the network
        route = random.choice(ROUTES)
        # Generate non-uniform delay variances (standard city traffic flow modeling)
        base_delay = random.choice([30, 60, 120, 240, 400])
        delay_variance = int(random.gauss(0, 45))
        actual_delay = max(-120, base_delay + delay_variance) # Prevent unrealistic infinite negative delays
 
        # Create structured JSON packet matching your PySpark schema
        timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        bus_packet = {
            "route_id": f"Route_{route}",
            "timestamp": timestamp_now,
            "delay_seconds": float(actual_delay)
        }
 
        # Send packet asynchronously to Kafka partition 0
        producer.send(topic_name, value=bus_packet)
        print(f"[{timestamp_now}] Ingested -> Route {route:4} | Delay: {actual_delay:4}s")
        # Stream speed pacing: drops 2-3 telemetry signals per second
        time.sleep(random.uniform(0.3, 0.5))
 
except KeyboardInterrupt:
    print("\n[-] Live ingestion gracefully paused by user.")
finally:
    producer.flush()
    producer.close()
    print("[+] Kafka producer connections closed safely.")
