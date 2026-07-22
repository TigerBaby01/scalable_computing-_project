import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    from_json, 
    timestamp_seconds, 
    window, 
    avg, 
    regexp_extract, 
    trim
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
 
# ==========================================
# 1. BATCH LAYER: Data Lake Logging Function
# ==========================================
def backup_batch_to_lake(batch_df, batch_id):
    """
    Appends micro-batch records to a local JSON data lake (Batch Layer).
    """
    lake_path = "/home/ubuntu/kafka_2.13-3.5.1/metadata/s3_historical_lake"
    os.makedirs(lake_path, exist_ok=True)
    records = batch_df.collect()
    if records:
        log_file = os.path.join(lake_path, f"batch_{batch_id}.json")
        with open(log_file, "w") as f:
            for row in records:
                f.write(json.dumps(row.asDict()) + "\n")
        print(f"[BATCH LAYER] Successfully backed up {len(records)} records to {log_file}")
    else:
        print(f"[BATCH LAYER] Micro-batch {batch_id} is empty. Awaiting stream telemetry...")
 
# ==========================================
# 2. CORE INFRASTRUCTURE: Spark Context Setup
# ==========================================
spark = SparkSession.builder \
    .appName("DublinBusRealTimeStreamingEngine") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")
 
# Define schema matching live bus tracking payloads
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
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.43.158:9092") \
    .option("subscribe", "dublin-bus-delays") \
    .option("startingOffsets", "latest") \
    .load()
 
parsed_stream_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), bus_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp_parsed", timestamp_seconds(col("timestamp")))
 
# -------------------------------------------------------------------
# CLEANING LAYER: Clean GTFS route codes and remove delay outliers
# -------------------------------------------------------------------
filtered_stream_df = parsed_stream_df \
    .withColumn("clean_route_id", trim(regexp_extract(col("route_id"), r"(\d+[a-zA-Z]?)", 1))) \
    .withColumn("route_id", col("clean_route_id")) \
    .filter(
        (col("delay_seconds") >= -300) & 
        (col("delay_seconds") <= 3600) & 
        (col("route_id") != "")
    )
 
windowed_delays = filtered_stream_df \
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
 
print("Launching Real-Time Speed Layer & Local Data Lake Pipeline...")
 
# ==========================================
# 4. EXECUTION: Streams Setup
# ==========================================
 
# 1. Batch Lake Backup Stream
lake_query = windowed_delays.writeStream \
    .outputMode("complete") \
    .foreachBatch(backup_batch_to_lake) \
    .start()
 
# 2. In-Memory Serving Layer Stream
memory_query = windowed_delays.writeStream \
    .format("memory") \
    .queryName("serving_layer_speed") \
    .outputMode("complete") \
    .start()
 
print("[SPEED LAYER] In-memory serving table 'serving_layer_speed' active.")
 
# ==========================================
# 5. LIVE SERVING LAYER DISPLAY LOOP
# ==========================================
try:
    while memory_query.isActive and lake_query.isActive:
        time.sleep(10)
        print("\n=== [SERVING LAYER] LIVE SPEED TABLE (In-Memory) ===")
        spark.sql("""
            SELECT
                route_id,
                round(average_delay_seconds, 2) as avg_delay_sec,
                window_start,
                window_end
            FROM serving_layer_speed
            ORDER BY average_delay_seconds DESC
            LIMIT 10
        """).show(truncate=False)
except KeyboardInterrupt:
    print("[SPEED LAYER] Stopping queries...")
    memory_query.stop()
    lake_query.stop()
