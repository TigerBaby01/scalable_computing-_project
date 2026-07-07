"""
setup_kinesis.py
-----------------
Creates the Kinesis Data Stream for the Dublin Bus pipeline.
Run this once before starting the producer.

Usage:
    python setup_kinesis.py --stream-name dublin-bus-delays --shards 2
"""

import boto3
import argparse
import logging
import time
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def create_stream(stream_name: str, shard_count: int, region: str):
    client = boto3.client("kinesis", region_name=region)

    # Check if stream already exists
    try:
        desc = client.describe_stream_summary(StreamName=stream_name)
        status = desc["StreamDescriptionSummary"]["StreamStatus"]
        logger.info(f"Stream '{stream_name}' already exists — status: {status}")
        return
    except client.exceptions.ResourceNotFoundException:
        pass

    logger.info(f"Creating Kinesis stream '{stream_name}' with {shard_count} shard(s)...")
    client.create_stream(StreamName=stream_name, ShardCount=shard_count)

    # Wait until ACTIVE
    waiter = client.get_waiter("stream_exists")
    waiter.wait(StreamName=stream_name)
    logger.info(f"Stream '{stream_name}' is now ACTIVE.")

    # Tag for cost tracking
    arn = client.describe_stream_summary(StreamName=stream_name)[
        "StreamDescriptionSummary"
    ]["StreamARN"]
    client.add_tags_to_stream(
        StreamName=stream_name,
        Tags={
            "Project": "DublinBusDelayAnalytics",
            "Module": "ScalableCloudProgramming",
            "College": "NCI",
        },
    )
    logger.info(f"Tagged stream ARN: {arn}")


def main():
    parser = argparse.ArgumentParser(description="Setup Kinesis stream for Dublin Bus pipeline")
    parser.add_argument("--stream-name", default="dublin-bus-delays")
    parser.add_argument("--shards", type=int, default=2,
                        help="Number of shards (2 = ~2 MB/s ingest, ~4 MB/s read)")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    create_stream(args.stream_name, args.shards, args.region)


if __name__ == "__main__":
    main()
