# Real Time Voting Simulation

This project is about simulating a real time voting scenario, process the data and visualise results on dashboard in near real time. 

## Generating Users

We can generate 2 kinds of entities 
1. Candidates for voting
2. Voters

To generate Candidates data 

```bash
python candidate_generator.py
```

To generate Voters data 

```bash
python voters_generator.py
```

## Start Kafka setup

To start the kafka brokers and confluent kafka webui run

```bash
cd docker-files
docker-compose up -d docker-compose.yaml
```

## Create Kafka Topics

From confluent kafka webui at http://locahost:9021 create the required kafka topics

![Kafka-Topics](/media/kafka-topics.png)

## Generate Votes

To start generating votes data to kafka topic created in previous step, run:

```bash
python votes_producer.py
```

## Start Spark Streaming Pipeline

To start processing the votes data sent to Kafka topic, submit a spark job to your spark cluster.
I have used my local spark cluster.

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 sparkCode/streaming.py
```

## Visualise data

Now the processed voting data is being sent to the output kafka topics by spark pipeline.
Start the dashboard app to visulise the data.

```bash
python dashboard/dashboard.py
```

![](/media/dashboard1.png)

![](/media/dashboard2.png)


