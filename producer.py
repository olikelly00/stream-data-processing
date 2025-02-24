from confluent_kafka import Producer
import socket
import random

# Define the Kafka configuration settings to be passed to the producer
conf = {
    # Kafka broker(s) to which the producer connects. In a multi-broker setup, this acts as the entry point to the Kafka cluster    
    'bootstrap.servers': 'localhost:9092',
    # Unique id for the producer
    'client.id': socket.gethostname()
}

# Create producer instance and pass it parameters
producer = Producer(conf)

# Fetch topic metadata - used later to get partition count
topic_data = producer.list_topics()


def produce_event(topic_name, message):
    """
    Produces a message to the specified Kafka topic.

    Args:
        topic_name (str): The name of the Kafka topic.
        message (str): The message payload.
    """


    if topic_name not in topic_data.topics:
        raise Exception(f"Error: Topic {topic_name} does not exist. Must be created first using this CLI command: bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server localhost:9092")
    
    partition_count = len(topic_data.topics[topic_name].partitions)


    # Call the in-built produce function on the producer instance, passing it the topic name, key and value
    
    # Key = optional. If provided, Kafka hashes it to determine the partition, ensuring all messages with the same key go to the same partition.
    # Value =  the actual message (payload)
    # Partition = manually specifying a partition overrides Kafka's default partitioning logic. 
    #   If not provided, Kafka will either use the key to determine a partition or distribute messages evenly.

    producer.produce(topic_name, key=topic_name, value=message, partition=random.randint(0, partition_count-1))

    # Explicitly flush the message so that all buffered messages are immediately sent to the cluster 
    producer.flush()

# Should work as expected
produce_event('orders', 'test_message_1')
produce_event('orders', 'test_message_2')
produce_event('orders', 'test_message_3')
produce_event('orders', 'test_message_4')
produce_event('orders', 'test_message_5')
# Should raise exception
produce_event('transactions', 'test_message_6')