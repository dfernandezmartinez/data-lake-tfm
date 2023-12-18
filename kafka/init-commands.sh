# Change directory to Kafka root
cd kafka_2.13-3.6.0/

# Start Zookeeper and Kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# List all topics
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create a new topic with RF=2 (PoC only)
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic poc-vulnerable --replication-factor 2

# Produce into a topic
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic poc-vulnerable

# Consume from a topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic poc-vulnerable

# Describe a topic
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic poc-vulnerable

# Delete a topic 
kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic poc-vulnerable
