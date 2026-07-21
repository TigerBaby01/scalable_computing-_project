import sys

from pyspark.sql import SparkSession

from pyspark.sql.functions import col, avg, round as spark_round, count
 
def main():

    # 1. Initialize PySpark Session taking advantage of all local CPU cores

    spark = SparkSession.builder.appName("DublinBusBatchLayerBenchmark").config("spark.driver.memory", "2g").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
 
    lake_path = "/home/ubuntu/kafka_2.13-3.5.1/metadata/s3_historical_lake/*.json"
 
    # 2. Parallel Read of the 1.1 GB Historical Data Lake

    print("[BATCH BENCHMARK] Reading 1.1 GB JSON Data Lake in parallel...")

    df = spark.read.json(lake_path)
 
    # 3. Distributed Aggregate Computation

    batch_results = df.groupBy("route_id").agg(

        spark_round(avg("average_delay_seconds"), 2).alias("historical_avg_delay"),

        count("*").alias("total_window_batches_analyzed")

    ).orderBy("route_id")
 
    # 4. Trigger Action to execute computation over the entire lake

    print("[BATCH BENCHMARK] Processing complete. Summary results:")

    batch_results.show(20, truncate=False)
 
    spark.stop()
 
if __name__ == "__main__":

    main()
 
