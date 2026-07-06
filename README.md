# Dublin Bus Real-Time Delay Analytics — Kinesis Producer

## Overview
This component ingests the NTA GTFS-Realtime TripUpdates feed and streams
parsed delay records into AWS Kinesis Data Streams every 30 seconds.

## Files
| File | Purpose |
|---|---|
| `kinesis_producer.py` | Main polling loop — fetches, parses, and publishes |
| `setup_kinesis.py` | One-time Kinesis stream creation |
| `load_simulator.py` | Captures a snapshot and replays at high rate for benchmarking |
| `requirements.txt` | Python dependencies |

## Setup

### 1. Get your NTA API Key
1. Go to https://developer.nationaltransport.ie/signup and create an account
2. Verify your email, then sign in at https://developer.nationaltransport.ie/signin
3. Go to **APIs** in the top nav → click **GTFS-Realtime** → click **Subscribe**
4. Your API key will appear under your profile — copy the **Primary Key**

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure AWS credentials (AWS Academy)
```bash
# Paste your AWS Academy credentials into:
~/.aws/credentials
# Or export as environment variables:
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
```

### 4. Create the Kinesis stream
```bash
python setup_kinesis.py --stream-name dublin-bus-delays --shards 2 --region us-east-1
```

### 5. Test with dry run first
```bash
python kinesis_producer.py --api-key YOUR_KEY --dry-run --max-polls 1
```

### 6. Start the producer
```bash
python kinesis_producer.py --api-key YOUR_KEY --stream-name dublin-bus-delays --interval 30
```

## Benchmarking / Load Testing (Phase 3)
```bash
# Step 1: Capture a live snapshot
python load_simulator.py --capture --api-key YOUR_KEY

# Step 2: Replay at 10x rate (100 records/sec for 5 minutes)
python load_simulator.py --replay --rps 100 --duration 300 --stream-name dublin-bus-delays
```

## Record Schema
Each record pushed to Kinesis:
```json
{
  "feed_timestamp": "2026-06-29T12:00:00+00:00",
  "ingestion_timestamp": "2026-06-29T12:00:01+00:00",
  "entity_id": "entity_123",
  "trip_id": "trip_456",
  "route_id": "46A",
  "direction_id": 0,
  "vehicle_id": "VH123",
  "stop_sequence": 5,
  "stop_id": "8220DB002081",
  "arrival_delay_s": 120,
  "departure_delay_s": 125,
  "schedule_relationship": "SCHEDULED"
}
```

## Partition Key Strategy
Records are partitioned by `route_id` so all records for a given route
(e.g. "46A") land on the same Kinesis shard, preserving ordering per route.

## Notes
- 2 shards = 2 MB/s ingest capacity — sufficient for the NTA feed
- The feed updates every ~30 seconds; polling more frequently won't give new data
- AWS Academy sessions expire — re-export credentials if you get AuthErrors
# scalable_computing-_project
# scalable_computing-_project
