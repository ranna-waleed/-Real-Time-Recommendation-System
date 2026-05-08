from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_json, window, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# create spark session
spark = SparkSession.builder \
    .appName("MovieLens-Spark-Streaming") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Streaming Session Started")

# define schema for incoming kafka messages
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# read stream from kafka
print("Connecting to Kafka topic: user-interactions")
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-interactions") \
    .option("startingOffsets", "latest") \
    .load()

# parse the json messages from kafka
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# handle malformed records by dropping nulls
clean_stream = parsed_stream.dropna()

# add event time column for windowing
clean_stream = clean_stream.withColumn(
    "event_time",
    current_timestamp()
)

# apply watermark to handle late data
watermarked_stream = clean_stream.withWatermark("event_time", "10 seconds")

# window analytics - 30 second window with 10 second slide
window_analytics = watermarked_stream \
    .groupBy(
        window(col("event_time"), "30 seconds", "10 seconds"),
        col("item_id")
    ) \
    .agg(
        avg("rating").alias("avg_rating"),
        count("user_id").alias("interaction_count")
    )

# calculate custom trending score
# trending score = interaction count * avg rating / time decay
trending_stream = window_analytics.withColumn(
    "trending_score",
    col("interaction_count") * col("avg_rating")
)

# user activity analytics
user_activity = watermarked_stream \
    .groupBy(
        window(col("event_time"), "30 seconds", "10 seconds"),
        col("user_id")
    ) \
    .agg(
        count("item_id").alias("interactions_count"),
        avg("rating").alias("avg_rating")
    )

# alert system - detect trending items with avg rating above 4.5
alert_stream = trending_stream.filter(col("avg_rating") > 4.5)

# write trending results to console
trending_query = trending_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# write alerts to console
alert_query = alert_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# write user activity to console
user_activity_query = user_activity.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# write trending results to json for dashboard
json_query = trending_stream.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/home/hduser/streaming_output/trending") \
    .option("checkpointLocation", "/home/hduser/streaming_output/checkpoint_trending") \
    .trigger(processingTime="10 seconds") \
    .start()

# write user activity to json for dashboard
user_json_query = user_activity.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/home/hduser/streaming_output/user_activity") \
    .option("checkpointLocation", "/home/hduser/streaming_output/checkpoint_user") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming queries started, waiting for data ...")
spark.streams.awaitAnyTermination()
