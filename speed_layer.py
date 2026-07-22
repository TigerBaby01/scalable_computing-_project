import time

import json

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, from_json, window, avg

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
 
# Define schema

bus_schema = StructType([

    StructField("route_id", StringType(), True),

    StructField("timestamp", TimestampType(), True),

    StructField("delay_seconds", DoubleType(), True)

])
 
# Initialize Spark Session

spark = SparkSession.builder.appName("DublinBusSpeedLayer").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
 
# Read Kafka Stream

kafka_stream_df = spark.readStream \

    .format("kafka") \

    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \

    .option("subscribe", "dublin-bus-delays") \

    .option("startingOffsets", "latest") \

    .load()
 
# Parse JSON payloads

parsed_df = kafka_stream_df \

    .selectExpr("CAST(value AS STRING) as json_str") \

    .select(from_json(col("json_str"), bus_schema).alias("data")) \

    .select("data.*")
 
# Calculate 5-Minute Sliding Windows shifting every 1 minute

windowed_df = parsed_df \

    .groupBy(

        window(col("timestamp"), "5 minutes", "1 minute"), 

        col("route_id")

    ) \

    .agg(avg("delay_seconds").alias("average_delay_seconds"))
 
# Write directly to an In-Memory Table

# Note: 'complete' mode is used here so the memory table shows full current window aggregates

query = windowed_df.writeStream \

    .format("memory") \

    .queryName("serving_layer_speed") \

    .outputMode("complete") \

    .start()
 
print("[SPEED LAYER] In-memory serving table 'serving_layer_speed' starting...")
 
# Periodically query and display the top live route delays in console

try:

    while query.isActive:

        time.sleep(10)

        print("\n=== [SERVING LAYER] LIVE SPEED TABLE (In-Memory) ===")

        spark.sql("""

            SELECT 

                route_id, 

                round(average_delay_seconds, 2) as avg_delay_sec,

                window.start as window_start,

                window.end as window_end

            FROM serving_layer_speed 

            ORDER BY average_delay_seconds DESC

            LIMIT 10

        """).show(truncate=False)

except KeyboardInterrupt:

    print("[SPEED LAYER] Stopping stream...")

    query.stop()
 
