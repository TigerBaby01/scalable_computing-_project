"""
kinesis_producer.py
--------------------
Dublin Bus Real-Time Delay Analytics — Kinesis Producer
NCI MSc Cloud Computing CA — Scalable Cloud Programming

Polls the NTA GTFS-Realtime TripUpdates feed every 30 seconds,
parses protobuf records, enriches each with metadata, and pushes
JSON records into an AWS Kinesis Data Stream.

Author: [Your Name]
Date:   June 2026
"""

import json
import time
import logging
import argparse
import boto3
import requests
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NTA_GTFS_RT_URL = "https://api.nationaltransport.ie/gtfsr/v2/TripUpdates"
DEFAULT_STREAM_NAME = "dublin-bus-delays"
DEFAULT_POLL_INTERVAL = 30          # seconds between polls
DEFAULT_REGION = "us-east-1"        # AWS Academy default region
MAX_BATCH_SIZE = 500                # Kinesis PutRecords limit
MAX_RECORD_BYTES = 1_000_000        # 1 MB Kinesis limit per record

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# NTA Feed Fetcher
# ---------------------------------------------------------------------------

def fetch_gtfs_rt(api_key: str) -> gtfs_realtime_pb2.FeedMessage | None:
    """
    Fetch the NTA GTFS-RT TripUpdates protobuf feed.
    Returns a parsed FeedMessage or None on error.
    """
    headers = {
        "x-api-key": api_key,
        "Ocp-Apim-Subscription-Key": api_key,
        "Cache-Control": "no-cache",
    }
    try:
        response = requests.get(NTA_GTFS_RT_URL, headers=headers, timeout=15)
        response.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        logger.info(f"Fetched feed: {len(feed.entity)} entities")
        return feed
    except requests.RequestException as e:
        logger.error(f"HTTP error fetching NTA feed: {e}")
        return None
    except Exception as e:
        logger.error(f"Protobuf parse error: {e}")
        return None


# ---------------------------------------------------------------------------
# Record Parser
# ---------------------------------------------------------------------------

def parse_trip_updates(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """
    Extract TripUpdate records from the GTFS-RT feed.
    Returns a list of flat JSON-serialisable dicts — one per StopTimeUpdate.

    Fields emitted:
        feed_timestamp      — feed header timestamp (UTC ISO-8601)
        ingestion_timestamp — wall-clock time this record was processed
        entity_id           — GTFS entity ID
        trip_id             — GTFS trip ID
        route_id            — bus route (e.g. "46A", "39A")
        direction_id        — 0 = outbound, 1 = inbound
        vehicle_id          — vehicle label if present
        stop_sequence       — stop order within the trip
        stop_id             — GTFS stop ID
        arrival_delay_s     — arrival delay in seconds (negative = early)
        departure_delay_s   — departure delay in seconds
        schedule_relationship — SCHEDULED / SKIPPED / NO_DATA
    """
    records = []
    feed_ts = datetime.fromtimestamp(
        feed.header.timestamp, tz=timezone.utc
    ).isoformat()
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        tu = entity.trip_update
        trip = tu.trip
        vehicle_label = tu.vehicle.label if tu.HasField("vehicle") else None

        for stu in tu.stop_time_update:
            arrival_delay = (
                stu.arrival.delay if stu.HasField("arrival") else None
            )
            departure_delay = (
                stu.departure.delay if stu.HasField("departure") else None
            )

            # Map schedule_relationship int → human-readable string
            sr_map = {0: "SCHEDULED", 1: "SKIPPED", 2: "NO_DATA"}
            schedule_rel = sr_map.get(stu.schedule_relationship, "UNKNOWN")

            record = {
                "feed_timestamp": feed_ts,
                "ingestion_timestamp": ingestion_ts,
                "entity_id": entity.id,
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "direction_id": trip.direction_id,
                "vehicle_id": vehicle_label,
                "stop_sequence": stu.stop_sequence,
                "stop_id": stu.stop_id,
                "arrival_delay_s": arrival_delay,
                "departure_delay_s": departure_delay,
                "schedule_relationship": schedule_rel,
            }
            records.append(record)

    logger.info(f"Parsed {len(records)} StopTimeUpdate records")
    return records


# ---------------------------------------------------------------------------
# Kinesis Publisher
# ---------------------------------------------------------------------------

class KinesisProducer:
    """
    Batched Kinesis PutRecords publisher.
    Uses route_id as the partition key so all records for a route
    land on the same shard (preserving order per route).
    """

    def __init__(self, stream_name: str, region: str):
        self.stream_name = stream_name
        self.client = boto3.client("kinesis", region_name=region)
        logger.info(
            f"KinesisProducer initialised — stream={stream_name}, region={region}"
        )

    def _build_kinesis_record(self, record: dict) -> dict:
        data = json.dumps(record, default=str).encode("utf-8")
        if len(data) > MAX_RECORD_BYTES:
            logger.warning(
                f"Record for trip {record.get('trip_id')} exceeds 1 MB — skipping"
            )
            return None
        return {
            "Data": data,
            # Partition by route so shard ordering is preserved per route
            "PartitionKey": str(record.get("route_id", "unknown")),
        }

    def put_records(self, records: list[dict]) -> dict:
        """
        Send records in batches of MAX_BATCH_SIZE (500).
        Returns aggregated stats: sent, failed, batches.
        """
        kinesis_records = [
            r for r in (self._build_kinesis_record(rec) for rec in records)
            if r is not None
        ]

        stats = {"sent": 0, "failed": 0, "batches": 0}

        for i in range(0, len(kinesis_records), MAX_BATCH_SIZE):
            batch = kinesis_records[i: i + MAX_BATCH_SIZE]
            try:
                response = self.client.put_records(
                    StreamName=self.stream_name,
                    Records=batch,
                )
                failed = response.get("FailedRecordCount", 0)
                sent = len(batch) - failed
                stats["sent"] += sent
                stats["failed"] += failed
                stats["batches"] += 1

                if failed > 0:
                    logger.warning(
                        f"Batch {stats['batches']}: {failed} records failed "
                        f"(provisioned throughput exceeded?)"
                    )
                else:
                    logger.info(
                        f"Batch {stats['batches']}: {sent} records sent successfully"
                    )

            except ClientError as e:
                logger.error(f"Kinesis PutRecords error: {e}")
                stats["failed"] += len(batch)

        return stats


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

def run(api_key: str, stream_name: str, region: str, poll_interval: int,
        max_polls: int = 0, dry_run: bool = False):
    """
    Main loop: poll → parse → publish, every poll_interval seconds.

    Args:
        api_key       NTA Open Data API key
        stream_name   Kinesis stream name
        region        AWS region
        poll_interval Seconds between polls
        max_polls     Stop after N polls (0 = run forever)
        dry_run       Parse records but do NOT send to Kinesis
    """
    producer = None if dry_run else KinesisProducer(stream_name, region)

    poll_count = 0
    total_sent = 0
    total_failed = 0

    logger.info(
        f"Starting producer | stream={stream_name} | interval={poll_interval}s "
        f"| dry_run={dry_run}"
    )

    while True:
        poll_count += 1
        logger.info(f"--- Poll #{poll_count} ---")

        feed = fetch_gtfs_rt(api_key)
        if feed:
            records = parse_trip_updates(feed)

            if dry_run:
                logger.info(f"[DRY RUN] Would send {len(records)} records")
                if records:
                    logger.info(f"Sample record: {json.dumps(records[0], indent=2)}")
            else:
                stats = producer.put_records(records)
                total_sent += stats["sent"]
                total_failed += stats["failed"]
                logger.info(
                    f"Cumulative — sent={total_sent}, failed={total_failed}"
                )

        if max_polls and poll_count >= max_polls:
            logger.info(f"Reached max_polls={max_polls}. Exiting.")
            break

        logger.info(f"Sleeping {poll_interval}s until next poll...")
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Dublin Bus GTFS-RT → AWS Kinesis producer"
    )
    parser.add_argument(
        "--api-key", required=True,
        help="NTA Open Data API key"
    )
    parser.add_argument(
        "--stream-name", default=DEFAULT_STREAM_NAME,
        help=f"Kinesis stream name (default: {DEFAULT_STREAM_NAME})"
    )
    parser.add_argument(
        "--region", default=DEFAULT_REGION,
        help=f"AWS region (default: {DEFAULT_REGION})"
    )
    parser.add_argument(
        "--interval", type=int, default=DEFAULT_POLL_INTERVAL,
        help=f"Poll interval in seconds (default: {DEFAULT_POLL_INTERVAL})"
    )
    parser.add_argument(
        "--max-polls", type=int, default=0,
        help="Stop after N polls (default: 0 = run forever)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and log records without sending to Kinesis"
    )
    args = parser.parse_args()

    run(
        api_key=args.api_key,
        stream_name=args.stream_name,
        region=args.region,
        poll_interval=args.interval,
        max_polls=args.max_polls,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
