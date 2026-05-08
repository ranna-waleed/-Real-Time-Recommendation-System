from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import time

# create spark session
spark = SparkSession.builder \
    .appName("MovieLens-ALS-Training") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session Started")

# load the dataset
print("Loading ratings.csv ...")
df = spark.read.csv(
    "/home/hduser/ml-25m/ratings.csv",
    header=True,
    inferSchema=True
)

# select and cast columns
df = df.select(
    col("userId").cast("integer"),
    col("movieId").cast("integer"),
    col("rating").cast("float"),
    col("timestamp").cast("long")
)

# drop null values
df = df.dropna()

# filter invalid ratings
df = df.filter((col("rating") >= 0.5) & (col("rating") <= 5.0))

print("Total valid records: " + str(df.count()))

# split into train and test
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print("Training records: " + str(train_df.count()))
print("Testing records: " + str(test_df.count()))

# train ALS model
print("Training ALS model ...")
start_time = time.time()

als = ALS(
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    rank=10,
    maxIter=10,
    regParam=0.1,
    coldStartStrategy="drop",
    nonnegative=True
)

model = als.fit(train_df)
training_time = time.time() - start_time
print("Model trained in " + str(round(training_time, 2)) + " seconds")

# evaluate the model
print("Evaluating model ...")
predictions = model.transform(test_df)
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)
rmse = evaluator.evaluate(predictions)
print("RMSE: " + str(round(rmse, 4)))

# tune model if RMSE is too high
if rmse > 1.5:
    print("RMSE is above 1.5, tuning the model ...")
    als_tuned = ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        rank=20,
        maxIter=15,
        regParam=0.05,
        coldStartStrategy="drop",
        nonnegative=True
    )
    model = als_tuned.fit(train_df)
    predictions = model.transform(test_df)
    rmse = evaluator.evaluate(predictions)
    print("Tuned RMSE: " + str(round(rmse, 4)))
else:
    print("RMSE is acceptable, no tuning needed")

# save the model
model_path = "/home/hduser/als_model"
model.write().overwrite().save(model_path)
print("Model saved to " + model_path)

# show sample recommendations
print("Generating Top-5 recommendations for sample users ...")
sample_users = train_df.select("userId").distinct().limit(5)
recommendations = model.recommendForUserSubset(sample_users, 5)
recommendations.show(truncate=False)

spark.stop()
print("Done")
