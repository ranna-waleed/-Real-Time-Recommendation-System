from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    #kafka sends bytes only , so python dict-> json->bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# sample user and movie ids from movielens dataset
user_ids = list(range(1, 1000))
movie_ids = list(range(1, 5000))

topic_name = 'user-interactions'

print("Kafka producer started, sending events to topic: " + topic_name)

def generate_event():
    user_id = random.choice(user_ids)
    movie_id = random.choice(movie_ids)
    rating = round(random.choice([0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]), 1)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") #current system time

    #build event
    event = {
        "user_id": user_id,
        "item_id": movie_id,
        "rating": rating,
        "timestamp": timestamp
    }
    return event

try:
    count = 0
    while True:
        event = generate_event()
        producer.send(topic_name, value=event) #send events to kafka
        count += 1
        print("Sent event " + str(count) + ": " + str(event))
        time.sleep(0.1)

except KeyboardInterrupt:
    print("Producer stopped by user")

finally:
    producer.flush()
    producer.close()
    print("Producer closed")
