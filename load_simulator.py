"""
load_simulator.py
------------------
Replays a captured GTFS-RT snapshot at a controlled, amplified rate
to simulate high-load conditions for benchmarking.

This is used in Phase 3 to measure:
  - Kinesis throughput at varying ingestion rates
  - Speed layer latency under load
  - EMR auto-scaling triggers

Usage:
    # First capture a snapshot
    python load_simulator.py --capture --api-key YOUR_KEY

    # Then replay at 10x rate to simulate load
    python load_simulator.py --replay --multiplier 10 --stream-name dublin-bus-delays
"""

import json
import time
import random
import argparse
import logging
import boto3
import requests
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

NTA_GTFS_RT_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
SNAPSHOT_FILE = "gtfs_snapshot.json"


def capture_snapshot(api_key: str):
    """Fetch one feed poll and save as JSON for replay."""
    from kinesis_producer import fetch_gtfs_rt, parse_trip_updates

    logger.info("Capturing snapshot from NTA feed...")
    feed = fetch_gtfs_rt(api_key)
    if not feed:
        logger.error("Failed to fetch feed.")
        return

    records = parse_trip_updates(feed)
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(records, f, default=str, indent=2)
    logger.info(f"Saved {len(records)} records to {SNAPSHOT_FILE}")


def replay_snapshot(
    stream_name: str,
    region: str,
    multiplier: int,
    target_rps: int,
    duration_s: int,
):
    """
    Replay the captured snapshot at a controlled rate.

    Args:
        multiplier   Noise factor — each record is duplicated with slight delay jitter
        target_rps   Records per second target
        duration_s   How long to run the replay (seconds)
    """
    with open(SNAPSHOT_FILE) as f:
        base_records = json.load(f)

    client = boto3.client("kinesis", region_name=region)
    logger.info(
        f"Replaying {len(base_records)} base records at {target_rps} rps "
        f"for {duration_s}s (multiplier={multiplier})"
    )

    start = time.time()
    sent_total = 0
    batch = []
    interval = 1.0 / target_rps  # seconds between records

    while time.time() - start < duration_s:
        # Pick a random base record and jitter the delay value
        rec = random.choice(base_records).copy()
        rec["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
        if rec.get("arrival_delay_s") is not None:
            rec["arrival_delay_s"] += random.randint(-30, 120)  # jitter ±
        rec["_simulated"] = True

        batch.append({
            "Data": json.dumps(rec, default=str).encode("utf-8"),
            "PartitionKey": str(rec.get("route_id", "unknown")),
        })

        if len(batch) >= 500:
            try:
                resp = client.put_records(StreamName=stream_name, Records=batch)
                failed = resp.get("FailedRecordCount", 0)
                sent_total += len(batch) - failed
                if failed:
                    logger.warning(f"{failed} records failed (throttled?)")
                logger.info(f"Sent batch — total sent: {sent_total}")
            except ClientError as e:
                logger.error(f"Kinesis error: {e}")
            batch = []

        time.sleep(interval)

    # Flush remaining
    if batch:
        client.put_records(StreamName=stream_name, Records=batch)
        sent_total += len(batch)

    elapsed = time.time() - start
    logger.info(
        f"Replay complete — sent {sent_total} records in {elapsed:.1f}s "
        f"({sent_total/elapsed:.1f} rps actual)"
    )


def main():
    parser = argparse.ArgumentParser(description="GTFS-RT load simulator for benchmarking")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--capture", action="store_true", help="Capture a live snapshot")
    mode.add_argument("--replay", action="store_true", help="Replay snapshot to Kinesis")

    parser.add_argument("--api-key", help="NTA API key (required for --capture)")
    parser.add_argument("--stream-name", default="dublin-bus-delays")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--multiplier", type=int, default=5,
                        help="Record duplication multiplier for load simulation")
    parser.add_argument("--rps", type=int, default=100,
                        help="Target records per second during replay")
    parser.add_argument("--duration", type=int, default=300,
                        help="Replay duration in seconds (default: 300 = 5 minutes)")
    args = parser.parse_args()

    if args.capture:
        if not args.api_key:
            parser.error("--api-key is required for --capture")
        capture_snapshot(args.api_key)
    else:
        replay_snapshot(
            stream_name=args.stream_name,
            region=args.region,
            multiplier=args.multiplier,
            target_rps=args.rps,
            duration_s=args.duration,
        )


if __name__ == "__main__":
    main()
