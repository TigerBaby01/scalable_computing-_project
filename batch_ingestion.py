import os

import json

from pyspark.sql import SparkSession

from pyspark.sql.functions import col
 
def save_raw_batch(batch_df, batch_id):

    lake_path = "/home/ubuntu/kafka_2.13-3.5.1/metadata/s3_historical_lake"

    os.makedirs(lake_path, exist_ok=True)

    records = batch_df.collect()

    if records:

        log_file = os.path.join(lake_path, f"raw_batch_{batch_id}.json")

        with open(log_file, "w") as f:

            for row in records:

                # Saves the raw string payload exactly as it came from Kafka

                f.write(row['raw_payload'] + "\n")

        print(f"[BATCH INGESTION] Saved {len(records)} raw records to {log_file}")
 
# Initialize Spark Session

spark = SparkSession.builder \

    .appName("DublinBusBatchIngestion") \

    .getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")
 
# Read directly from the Kafka Topic

kafka_stream_df = spark.readStream \

    .format("kafka") \

    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \

    .option("subscribe", "dublin-bus-delays") \

    .option("startingOffsets", "latest") \

    .load()
 
# Select the raw string value

raw_payload_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as raw_payload")
 
# Run the stream sink to dump raw logs

query = (raw_payload_df.writeStream

    .foreachBatch(save_raw_batch)

    .start())
 
query.awaitTermination()
 
