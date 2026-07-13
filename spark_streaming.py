import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, timestamp_seconds, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import boto3

# 1. Initialize Spark Session configured with native Kafka drivers
spark = SparkSession.builder \
    .appName("DublinBusRealTimeAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define schema to match incoming live JSON strings from Kafka
bus_payload_schema = StructType([
    StructField("timestamp", IntegerType(), True),
    StructField("route_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("stop_id", StringType(), True),
    StructField("delay_seconds", IntegerType(), True)
])

print("Connecting PySpark to Kafka topic: 'dublin-bus-delays'...")

# 3. Read streaming data from local Kafka broker
raw_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "dublin-bus-delays") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse the byte values into clean JSON elements
parsed_stream = raw_kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), bus_payload_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", timestamp_seconds(col("timestamp")))

# 5. H1 Criterion Metric: 15-Minute Sliding Window, evaluating data every 1 minute
# A 10-minute watermark dynamically handles data lag and cleans historical cache windows
windowed_delays = parsed_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "15 minutes", "1 minute"),
        col("route_id")
    ) \
    .agg(avg("delay_seconds").alias("average_delay_seconds")) \
    .select(
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("route_id"),
        col("average_delay_seconds")
    )

# 6. Serving Layer: Partitioned Batch Writing to Amazon DynamoDB
def write_partition_to_dynamodb(partition):
    """Establishes an isolated connection per partition to prevent network pickling errors"""
    try:
        # Boto3 client initialization happens inside worker nodes natively
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('DublinBusRouteDelays')
        
        # Batch writer automatically groups items to optimize I/O limits
        with table.batch_writer() as batch:
            for row in partition:
                batch.put_item(
                    Item={
                        'route_id': str(row['route_id']),
                        'window_end': str(row['window_end']),
                        'window_start': str(row['window_start']),
                        'average_delay_seconds': str(round(row['average_delay_seconds'], 2))
                    }
                )
    except Exception as e:
        print(f"[DYNAMODB PARTITION ERROR] Bulk write execution failed: {e}")

def write_batch_to_dynamodb(df, batch_id):
    """Processes micro-batch dataframes using distributed execution segments"""
    if df.count() > 0:
        print(f"[SERVING LAYER] Pushing micro-batch {batch_id} directly to AWS DynamoDB...")
        # rdd.foreachPartition streams data rows cleanly without serialization barriers
        df.rdd.foreachPartition(write_partition_to_dynamodb)

print("Launching Real-Time Sliding Window Analytics Engine linked with Cloud Serving Layer...")

# 7. Start streaming ingestion query pipeline
query = windowed_delays.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch_to_dynamodb) \
    .start()

query.awaitTermination()
