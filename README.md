# Real-Time Movie Recommendation System

A big data system that combines batch machine learning and real-time streaming analytics using Apache Spark and Apache Kafka.

## Team Information

- Domain: Movies (MovieLens 25M Dataset)
- System Focus: Real-Time Intelligence
- Dataset Size: 25 million ratings

## System Overview

This system learns user preferences from historical movie ratings using the ALS algorithm, then processes live user interactions through a Kafka and Spark Streaming pipeline to generate dynamic recommendations and detect trending movies in real time.

## What Makes Our System Different

- Custom trending score formula that combines interaction count and average rating
- Rating manipulation detection that flags users who rate the same item multiple times in the stream
- Three partition Kafka strategy justified by user id distribution to avoid hot partitions
- Live dashboard with 5 panels that auto-refreshes every 3 seconds

## Project Structure

realtime-recommendation-system/
├── ml/
│   └── als_training.py           # Batch ALS model training
├── kafka/
│   └── producer.py               # Kafka event producer
├── streaming/
│   └── spark_streaming.py        # Spark Structured Streaming
├── integration/
│   └── recommendation_engine.py  # ML + Streaming integration
├── alerts/
│   └── alert_handler.py          # Alert system
├── dashboard/
│   └── app.py                    # Real-time dashboard
├── report/
│   └── architecture_diagram.png  # System architecture
├── README.md
└── requirements.txt

## Requirements

- Ubuntu 22.04
- Java 11
- Python 3.12
- Apache Spark 3.5
- Apache Kafka 3.9
- Python packages in requirements.txt

## Installation

Install required Python packages:

```bash
pip3 install kafka-python pandas dash plotly flask pyspark
```

## How to Run

Follow these steps in order. Each step requires a separate terminal window.

### Step 1 - Start Zookeeper

```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Step 2 - Start Kafka Broker

Open a new terminal:

```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

### Step 3 - Create Kafka Topic

Open a new terminal:

```bash
cd ~/kafka
bin/kafka-topics.sh --create --topic user-interactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### Step 4 - Train the ALS Model

```bash
cd ~
spark-submit ml/als_training.py
```

Wait for this to finish before moving to the next step.

### Step 5 - Start Kafka Producer

Open a new terminal:

```bash
cd ~/-Real-Time-Recommendation-System
python3 kafka/producer.py
```

### Step 6 - Start Spark Streaming

Open a new terminal:

```bash
cd ~
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 streaming/spark_streaming.py
```

### Step 7 - Start Recommendation Engine

Open a new terminal:

```bash
cd ~
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 integration/recommendation_engine.py
```

### Step 8 - Start Alert Handler

Open a new terminal:

```bash
cd ~
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 alerts/alert_handler.py
```

### Step 9 - Start Dashboard

Open a new terminal:

```bash
cd ~/-Real-Time-Recommendation-System
python3 dashboard/app.py
```

Then open your browser and go to:http://localhost:8050/

## Kafka Partitioning Strategy

We used 3 partitions for the user-interactions topic. Events are distributed by user_id so that each partition handles a balanced subset of users. This avoids hot partitions where one partition receives significantly more data than others.

## Late Data Handling

We applied a 10 second watermark on the event time column in all streaming queries. Records that arrive more than 10 seconds late are dropped. This is documented in spark_streaming.py and recommendation_engine.py.

## Custom Metric - Trending Score

Trending Score is calculated as: 
trending_score = interaction_count x avg_rating

This gives higher scores to items that are both frequently interacted with and highly rated. Items with many low ratings will score lower than items with fewer but higher ratings.

## Alert System

The system triggers two types of alerts:

- Trending Item Alert: triggered when an item has an average rating above 4.5 in the current window
- User Spike Alert: triggered when a user has more than 10 interactions in the current window
- Rating Manipulation Alert: triggered when a user rates the same item more than once in the stream

Alerts are saved to /home/hduser/alerts_log/alerts.json and shown live in the dashboard.

## Dataset

MovieLens 25M dataset from GroupLens Research.
Download: https://grouplens.org/datasets/movielens/25m/

Place the ratings.csv file at:

/home/hduser/ml-25m/ratings.csv
