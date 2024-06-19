from kafka import KafkaConsumer
import json
from collections import OrderedDict
import concurrent.futures
import time

def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def read_from_kafka_topic(consumer):

    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data


votes_per_candidate_consumer = create_kafka_consumer('votes_per_candidate')
votes_per_candidate_by_gender_consumer = create_kafka_consumer('votes_per_candidates_by_gender')
votes_by_gender = create_kafka_consumer('votes_by_gender')

async def process_votes_per_candidate_consumer():
    for message in votes_per_candidate_consumer:
        print('m1 >>>', message.value)
        time.sleep(1)

async def process_votes_by_gender():
    for message in votes_by_gender:

        print('m2 >>>', message.value)
        time.sleep(1)

async def process_votes_per_candidate_by_gender_consumer():
    
    for message in votes_per_candidate_by_gender_consumer:
        print('m3 >>>', message.value)
        time.sleep(1)

pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)

pool.submit(process_votes_per_candidate_consumer)
pool.submit(process_votes_by_gender)
pool.submit(process_votes_per_candidate_by_gender_consumer)

pool.shutdown()