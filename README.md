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
In your AWS Academy Learner Lab, click **AWS Details** → **Show** next to *AWS CLI* — you'll see three credential lines. Copy them, then run:

```bash
# Create the folder and file (safe to run even if they already exist)
mkdir -p ~/.aws
nano ~/.aws/credentials
```

Paste your credentials in this exact format:
```
[default]
aws_access_key_id = ASIA...
aws_secret_access_key = ...
aws_session_token = ...
```

Save with `Ctrl+O` → Enter → `Ctrl+X`. Verify it works with:
```bash
aws sts get-caller-identity
```

> ⚠️ AWS Academy sessions expire every few hours. Re-paste fresh credentials from the **AWS Details** panel each time you restart your lab session.

If you get `zsh: command not found: aws`, the AWS CLI is not installed yet. Install it first:

**Mac:**
```bash
curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
sudo installer -pkg AWSCLIV2.pkg -target /
```

**Windows (PowerShell as Admin):**
```powershell
msiexec.exe /i https://awscli.amazonaws.com/AWSCLIV2.msi
```

**Linux:**
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

Then close and reopen your terminal, and confirm it works:
```bash
aws --version
# Expected: aws-cli/2.x.x ...
```

### 4. Create the Kinesis stream
```bash
python setup_kinesis.py --stream-name dublin-bus-delays --shards 2 --region us-east-1
```

### 5. Fix Python version (required — Python 3.14 is not supported)

The `protobuf` library used for GTFS-RT does not support Python 3.14 yet.
You need Python 3.11 or 3.12. Use `pyenv` to install and switch:

```bash
# Install pyenv if you don't have it
brew install pyenv

# Install Python 3.12
pyenv install 3.12

# Use 3.12 in this project folder only
pyenv local 3.12

# Confirm the version
python3 --version
# Expected: Python 3.12.x

# Reinstall dependencies under 3.12
pip3 install -r requirements.txt
```

### 6. Test with dry run first
```bash
python3 kinesis_producer.py --api-key YOUR_KEY --dry-run --max-polls 1
```

### 7. Start the producer
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
