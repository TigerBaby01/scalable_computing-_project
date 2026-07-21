import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, timestamp_seconds, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import boto3

# ==========================================
# 1. BATCH LAYER: Data Lake Logging Function
# ==========================================
def write_batch_to_dynamodb(batch_df, batch_id):
    """
    This function processes each micro-batch. It simultaneously appends raw 
    windowed aggregates to a local JSON data lake (Batch Layer) and updates 
    the active records inside AWS DynamoDB (Speed Layer).
    """
    lake_path = "/home/ubuntu/kafka_2.13-3.5.1/metadata/s3_historical_lake"
    os.makedirs(lake_path, exist_ok=True)
    
    # Collect data points from this micro-batch partition
    records = batch_df.collect()
    
    if records:
        # Save a historical copy locally as a JSON-lines file
        log_file = os.path.join(lake_path, f"batch_{batch_id}.json")
        with open(log_file, "w") as f:
            for row in records:
                f.write(json.dumps(row.asDict()) + "\n")
        print(f"[BATCH LAYER] Successfully backed up {len(records)} records to {log_file}")
        
        # Initialize Boto3 DynamoDB resource targeting N. Virginia
        print(f"[SERVING LAYER] Processing micro-batch {batch_id}...")
        try:
            dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
            table = dynamodb.Table('DublinBusRouteDelays')
            
            # Batch write items directly to DynamoDB
            with table.batch_writer() as batch:
                for row in records:
                    batch.put_item(
                        Item={
                            'route_id': str(row['route_id']),
                            'window_end': str(row['window_end']),
                            'window_start': str(row['window_start']),
                            'average_delay_seconds': str(round(row['average_delay_seconds'], 2))
                        }
                    )
            print(f"[SPEED LAYER] Successfully pushed updates to DynamoDB.")
        except Exception as e:
            print(f"[DYNAMODB LOCAL WRITE ERROR] Failed sending batch: {e}")
    else:
        print(f"[SERVING LAYER] Micro-batch {batch_id} is empty. Awaiting stream telemetry...")

# ==========================================
# 2. CORE INFRASTRUCTURE: Spark Context Setup
# ==========================================
# Initialize PySpark Session with pre-loaded Kafka Ingestion Dependencies
spark = SparkSession.builder \
    .appName("DublinBusRealTimeStreamingEngine") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema matching incoming live bus tracking simulator JSON payloads
bus_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("bus_id", StringType(), True),
    StructField("delay_seconds", IntegerType(), True),
    StructField("timestamp", DoubleType(), True)
])

print("Connecting PySpark to Kafka topic: 'dublin-bus-delays'...")

# ==========================================
# 3. STREAM PROCESSING: Window Ingestion Logic
# ==========================================
# Connect stream consumer to local Kafka Broker loopback socket
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "dublin-bus-delays") \
    .option("startingOffsets", "latest") \
    .load()

# Parse binary payload strings to columns and convert epoch metrics to Spark Timestamps
parsed_stream_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), bus_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp_parsed", timestamp_seconds(col("timestamp")))

# Apply real-time sliding window calculations (e.g., 5-minute windows sliding every 1 minute)
windowed_delays = parsed_stream_df \
    .groupBy(
        window(col("timestamp_parsed"), "5 minutes", "1 minute"),
        col("route_id")
    ) \
    .agg(avg("delay_seconds").alias("average_delay_seconds")) \
    .select(
        col("route_id"),
        col("window.start").cast(StringType()).alias("window_start"),
        col("window.end").cast(StringType()).alias("window_end"),
        col("average_delay_seconds")
    )

print("Launching Real-Time Sliding Window Analytics Engine linked with Cloud Serving Layer...")

# ==========================================
# 4. EXECUTION EXEC: Trigger Architecture Pipeline
# ==========================================
query = (windowed_delays.writeStream
    .outputMode("complete")
    .foreachBatch(write_batch_to_dynamodb)
    .start())

query.awaitTermination()
