# Distributed Message Queue
This repository contains an implementation of a Distributed Message Queue (Ubuntu/Linux) done as part of the Distributed Systems course at IIT Kharagpur.

The **Distributed Message Queue** can be divided into multiple components:
1. HTTP Server
2. Producer
3. Consumer
4. Manager
5. Broker
6. Manager Replica

## HTTP Server

The HTTP server API is written in python using Flask.\
Server address: http://127.0.0.1:8001 \
Following are the API endpoints made available:
* /topics
  - GET
  - Returns a list of topics available
* /consumer/consume
  - GET
  - Params: (consumer_id, topic) or (consumer_id, topic, partition id)
  - Returns: a message after dequeuing from the Message Queue
* /size
  - GET
  - Params: consumer_id, topic
  - Returns: size of the queue for given topic in Message Queue
* /topics
  - POST
  - Params: topic_name
  - Returns: status of creating a new topic with the given name
* /topics/partition
  - GET
  - Params: topic_name
  - Returns: partitions present in the given topic
* /consumer/register
  - POST
  - Params: topic
  - Return: status of registering as a consumer and the corresponding consumer_id
* /producer/register
  - POST
  - Params: topic
  - Return: status of registering as a producer and the corresponding producer_id
  - Note: creates the topic if it doesn’t already exist
* /producer/produce
  - POST
  - Params: (topic, producer_id, message) or (topic, producer_id, partition id, message)
  - Adds the given message to the Message Queue

## Producer
Producer library written in python. Clients can use this to log messages into the system. It has the following functions:
* **RegisterProducer**:\
Register the producer for the given topic name. On success it returns a Producer ID indicating that producer is successfully registered to that topic. On failure it raises myProducerError with an error message representing the error. 
* **ListTopics**:\
Displays the list of topics present in the distributed queue. On success it returns the list of all the topics available that the producers have created. On failure it raises myProducerError with an error message representing the error.
* **Enqueue**:\
Enqueues a log message to the given topic. On success it returns 0 indicating that the log message is enqueued into the queue. On failure it raises myProducerError with an error message representing the error.
* **CreateTopic**:\
Creates a topic. On success it returns 0 indicating that the Topic has been created by the producer. On failure it raises myProducerError with an error message representing the error.

## Consumer
Consumer library is written in python. Clients can use this to retrieve log messages from the system. It has the following functions:
* **RegisterConsumer:**\
Registers the consumer to the given topic. On success it returns a Consumer ID indicating that consumer is successfully registered to that topic. On failure it raises a myConsumerError with the error message.
* **ListTopics:**\
Get the list of topics available. On success it returns the list of all the topics available that the producers have created. On failure it raises a myConsumerError with the error message.
* **Dequeue:**\
Get a message from the given topic chronologically. On success the function returns the log message that is dequeued from the requested topic log queue. On failure it raises a myConsumerError with the error message.
* **Size:**\
Get number of messages present in the given topic. On success the function Returns the size of the log queue for the requested topic. On failure it raises a myConsumerError with the error message.

## Manager
Manager has the following functionalities:
1. Main database maintenance
2. Message Queue maintenance
4. Broker Maintenance
    - Health Check - Respond to Heart Beats, store in Heath Check DB
    - Gather Updates from Brokers regularly
5. Load Balancing Brokers
    - Route the Producer Requests to Brokers in a Round Robin fashion
6. Replica Maintenance
    - Health Check - Respond to Heart Beats
    - Push Updates to Replica regularly
7. Producer Request tracking
    - Store request times in Health Check DB

## Broker
Brokers deal with all Producer tasks which happen “in-memory” and there is no DB at broker level as broker data doesn’t have to be persistent (Manager regularly grabs updates from Brokers). The idea here is to scale the Producer Requests.
Brokers have the following functionalities:
1. Register to the Manager
2. Handle Producer Requests
    - Receive Requests from the Manager
    - Execute the corresponding tasks with Error Handling
3. Replicate the work done on two other Brokers (RAFT) for Consistency and Durability
3. Send Updates to the Manger on Request
4. Send Heart Beats to the Manager regularly for Health Check

## Manager Replica
Manager Replica handles all the request coming from Consumers. The idea here is to scale the Consumer Requests.
Manager Replica has the following functionalities:
1. Register to the Manager
2. Handle Consumer Requests
     - Receive Requests from the HTTP serer
     - Execurte the corresponding tasks with Error Handling
3. Receive Updates from the Manger on Request
4. Send Heart Beats to the Manager regularly for Health Check
5. Consumer Request tracking
    - Store request times in Health Check DB
  
## Additional Notes
1. This design follows Client-Server architecture but can be done in Microservices architecture to further improve the scalability.
2. All communications between HTTP Server, Manager, Replica and Brokers happen through gRPC.
3. Database:
    - Porgres is used for the main database and the Health Check database.
    - A simple database schema is used consisting of Producer, Consumer, Message and other tables.
4. Topic Partitions:
    - Each topic is further divided into partitions and producers/consumers can perform tasks on individual partitions.
    - However, producer/consumer libraries have not been updated with this functionality. HTTP API can directly be used.
5. Replication (RAFT):
    - The idea is to have durability and a proper functioning system despite broker failures.
    - PySyncObj opensource library is re-engineered to decouple the Transport object from Raft instances.
    - Transport object is responsible for the communication between nodes in the Raft network.
    - Each Broker has a single Transport object and multiple Raft instances using it.
    - Each Raft instance corresponds to a topic partition.
6. All HTTP Server API responses are in JSON format and loosely follow REST principles.
7. PORTS Used:
    - HTTP Server - 8001
    - Manager - 50051
    - Replica - 50053
    - Brokers (as required, one port for gRPC and one for RAFT)
        - Broker 1 - 50052 8003
        - Broker 2 - 50054 8004
        - Broker 3 - 50055 8005

## Prerequisites:

python3

pip

postgres



### python modules:

psycopg2-binary

requests




### Installation Instructions:


1. postgres:

          sudo apt install postgresql postgresql-contrib

          sudo apt install postgresql


2. Configure Database:

          sudo -u postgres psql

          ALTER USER postgres with PASSWORD 'test123';

          create database d_queue;

          \q --> to quit


3. ```pip install psycopg2-binary```



## How to run:

### Servers

```bash start_servers.sh```

Or

See start_servers.sh to run each server individually.


### Tests

#### System Test for Consistency:
Test whether all logs produced by Producers will be consumed by Consumers as per subscriptions.
1. python3 test.py p
    - Runs the code for Registering 5 Producers and produce the corresponding logs
2. python3 test.py c
    - Runs the code for Registering 3 Consumers and consume the corresponding logs
3. Verify the contents of Consumer_#.txt files produced.


## How to use Producer and Consumer libraries:

Copy src/producer_client.py and consumer_client.py as necessary into your working directory and import the classes or see system tests for examples of how to use them.
