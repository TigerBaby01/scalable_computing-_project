import json

import redis

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, from_json, window, avg

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
 
# Define schema

bus_schema = StructType([

    StructField("route_id", StringType(), True),

    StructField("timestamp", TimestampType(), True),

    StructField("delay_seconds", DoubleType(), True)

])
 
def send_to_redis(batch_df, batch_id):

    r = redis.Redis(host='localhost', port=6379, db=0)

    records = batch_df.collect()

    for row in records:

        route_id = row['route_id']

        avg_delay = round(row['average_delay_seconds'], 2)

        r.set(f"live:{route_id}", avg_delay)

    print(f"[SPEED LAYER] Successfully updated {len(records)} live routes in Redis.")
 
# Single line builder blocks to eliminate indentation bugs

spark = SparkSession.builder.appName("DublinBusSpeedLayer").getOrCreate()

spark.sparkContext.setLogLevel("WARN")
 
# Flattened read stream properties

kafka_stream_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "dublin-bus-delays").option("startingOffsets", "latest").load()
 
# Parse JSON strings matching schema

parsed_df = kafka_stream_df.selectExpr("CAST(value AS STRING) as json_str").select(from_json(col("json_str"), bus_schema).alias("data")).select("data.*")
 
# Calculate 5-Minute Sliding Windows shifting every 1 minute

windowed_df = parsed_df.groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("route_id")).agg(avg("delay_seconds").alias("average_delay_seconds"))
 
# Flattened streaming sink launch properties 

query = windowed_df.writeStream.outputMode("update").foreachBatch(send_to_redis).start()

query.awaitTermination()
 
