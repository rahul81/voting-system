import csv
import json
from kafka import KafkaProducer
import random
import time
from datetime import datetime

random.seed(22)

bootstrap_servers = 'localhost:9092'

def serializer(message):
    return json.dumps(message).encode('utf-8')


producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=serializer)
print("Kafka producer initialised")

CANDIDATES_FILE_PATH = './data/candidates.csv'
candidates = []

candidates_reader = csv.reader(open(CANDIDATES_FILE_PATH, 'r'))
print("Successfully read candidates csv from ", CANDIDATES_FILE_PATH)

header = next(candidates_reader)

for e, row in enumerate(candidates_reader):

    # create key, value pairs of candidates header data and values
    candidate_data = {key:value for key, value in zip(header, row)}
    candidates.append(candidate_data)


VOTERS_FILE_PATH = './data/voters.csv'
voters_reader = csv.reader(open(VOTERS_FILE_PATH, 'r'))

header = next(voters_reader)

print("Voting commences...")

for e, row in enumerate(voters_reader):

    # pick a candidate from available candidates, this mimics the behaviour of selecting a candidate
    voted_to = random.choice(candidates)['candidate_id']

    # with voters data add the chose candidate id and timestamp of the event to create the voting message
    message = {
        key:value for key, value in zip(header, row)} \
            | {"voted_to": voted_to, "vote_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    
    producer.send(
        'votes-topic',
        message
    )

    time.sleep(2)


    # if (e+1)%2 == 0:
    #     break

    














