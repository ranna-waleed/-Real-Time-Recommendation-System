from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_json, current_timestamp, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from datetime import datetime
import os
import json

# create spark session
spark = SparkSession.builder \
    .appName("MovieLens-Alert-Handler") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Alert Handler Started")

# define schema for incoming kafka messages
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("rating", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

# read stream from kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-interactions") \
    .option("startingOffsets", "latest") \
    .load()

# parse json messages
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# drop malformed records
clean_stream = parsed_stream.dropna()

# add event time
clean_stream = clean_stream.withColumn("event_time", current_timestamp())

# apply watermark for late data
watermarked_stream = clean_stream.withWatermark("event_time", "10 seconds")

# alert thresholds
RATING_THRESHOLD = 4.5
ACTIVITY_THRESHOLD = 10

# create alerts log directory
alerts_log_path = "/home/hduser/alerts_log"
os.makedirs(alerts_log_path, exist_ok=True)

def write_alert(alert_type, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    alert = {
        "timestamp": timestamp,
        "alert_type": alert_type,
        "message": message
    }
    log_file = alerts_log_path + "/alerts.json"
    #append alert to file (JSON lines format)
    with open(log_file, "a") as f:
        f.write(json.dumps(alert) + "\n")
    print("ALERT [" + alert_type + "]: " + message)

def process_alerts(batch_df, batch_id):
    #skip empty batch
    if batch_df.count() == 0:
        return

    print("Checking alerts for batch: " + str(batch_id))

    # check for high rated trending items
    item_stats = batch_df.groupBy("item_id") \
        .agg(
            avg("rating").alias("avg_rating"),
            count("user_id").alias("interaction_count")#no. of interactions
        )

    # alert for items with rating above threshold
    high_rated_items = item_stats.filter(
        col("avg_rating") > RATING_THRESHOLD
    ).collect()

    for row in high_rated_items:
        message = "Item " + str(row["item_id"]) + \
                  " is trending with avg rating " + \
                  str(round(row["avg_rating"], 2)) + \
                  " and " + str(row["interaction_count"]) + " interactions"
        write_alert("TRENDING_ITEM", message)

    # check for user activity spikes
    user_stats = batch_df.groupBy("user_id") \
        .agg(count("item_id").alias("activity_count"))

    active_users = user_stats.filter(
        col("activity_count") > ACTIVITY_THRESHOLD
    ).collect()

    for row in active_users:
        message = "User " + str(row["user_id"]) + \
                  " has unusually high activity with " + \
                  str(row["activity_count"]) + " interactions"
        write_alert("USER_SPIKE", message)

    # check for rating manipulation:same user rating same movie multiple times
    manipulation = batch_df.groupBy("user_id", "item_id") \
        .agg(count("rating").alias("rating_count")) \
        .filter(col("rating_count") > 1) \
        .collect()

    for row in manipulation:
        message = "User " + str(row["user_id"]) + \
                  " rated item " + str(row["item_id"]) + \
                  " multiple times (" + str(row["rating_count"]) + " times)"
        write_alert("RATING_MANIPULATION", message)

# process each micro batch
query = clean_stream.writeStream \
    .foreachBatch(process_alerts) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Alert handler is running, waiting for data ...")
query.awaitTermination()
