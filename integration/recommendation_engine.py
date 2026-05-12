from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, avg, count, from_json, current_timestamp, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
import time

# create spark session
spark = SparkSession.builder \
    .appName("MovieLens-Recommendation-Engine") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Recommendation Engine Started")

# load the saved ALS model
model_path = "/home/hduser/als_model"
print("Loading ALS model from " + model_path)
model = ALSModel.load(model_path)
print("ALS model loaded successfully")

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

# parse json messages
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# drop malformed records
clean_stream = parsed_stream.dropna()

# add event time for windowing
clean_stream = clean_stream.withColumn("event_time", current_timestamp())

# apply watermark for late data handling
watermarked_stream = clean_stream.withWatermark("event_time", "10 seconds")

# function to generate recommendations for each micro batch
def generate_recommendations(batch_df, batch_id):
    #skip empty batch
    if batch_df.count() == 0:
        return

    print("Processing batch id: " + str(batch_id))
    start_time = time.time() $ measure latency

    # get all users who had activity in this batch
    users_in_batch = batch_df.select(
        col("user_id").alias("userId")
    ).distinct()

    # generate top 5 recommendations for each user
    recommendations = model.recommendForUserSubset(users_in_batch, 5)

    # calculate latency
    latency = time.time() - start_time
    print("Recommendations generated in " + str(round(latency, 3)) + " seconds")

    if latency < 5:
        print("Latency is within 5 seconds target")
    else:
        print("Warning: latency exceeded 5 seconds target")

    # show recommendations
    print("Top-5 recommendations for users in this batch:")
    recommendations.show(truncate=False)

    # detect trending items in this batch
    trending = batch_df.groupBy("item_id") \
        .agg(
            count("user_id").alias("interaction_count"),
            avg("rating").alias("avg_rating")
        ) \
        .withColumn(
            "trending_score",
            col("interaction_count") * col("avg_rating")
        ) \
        .orderBy(col("trending_score").desc()) \
        .limit(5)

    print("Top trending items in this batch:")
    trending.show(truncate=False)

    # detect rating manipulation , users rating same item multiple times
    duplicate_ratings = batch_df.groupBy("user_id", "item_id") \
        .agg(count("rating").alias("rating_count")) \
        .filter(col("rating_count") > 1)

    if duplicate_ratings.count() > 0:
        print("ALERT: Possible rating manipulation detected")
        duplicate_ratings.show(truncate=False)

    # trigger alerts for high rated items
    high_rated = trending.filter(col("avg_rating") > 4.5)
    if high_rated.count() > 0:
        print("ALERT: The following items are trending with high ratings:")
        high_rated.show(truncate=False)

# process each micro batch using foreachBatch
query = clean_stream.writeStream \
    .foreachBatch(generate_recommendations) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Recommendation engine is running, waiting for data ...")
query.awaitTermination()
