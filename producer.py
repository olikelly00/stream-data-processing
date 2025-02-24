from confluent_kafka import Producer
import socket
import random
import json
import time



# Define the Kafka configuration settings to be passed to the producer
conf = {
    # Kafka broker(s) to which the producer connects. In a multi-broker setup, this acts as the entry point to the Kafka cluster    
    'bootstrap.servers': 'localhost:9092',
    # Unique id for the producer
    'client.id': socket.gethostname()
}
order_id = 0
# Create producer instance and pass it parameters
producer = Producer(conf)

customer_ids = list(range(1,20))

# Fetch topic metadata - used later to get partition count

topic_data = producer.list_topics()
topic_name = "online-orders"


if topic_name not in topic_data.topics:
        raise Exception(f"Error: Topic {topic_name} does not exist. Must be created first using this CLI command: bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server localhost:9092")

def produce_event(topic_name):
    """
    Produces a message to the specified Kafka topic.

    Args:
        topic_name (str): The name of the Kafka topic.
        message (str): The message payload.
    """
    global order_id
    
    amount = random.randint(2, 30)
    
    customer_id = customer_ids[random.randint(0, len(customer_ids) - 1)]
        
    json_message = json.dumps({
        "order_id": order_id,
        "amount": amount,
        "customer_id": customer_id,
        "total_orders": len(topic_data.topics)
    })



    partition_count = len(topic_data.topics[topic_name].partitions)


    # Call the in-built produce function on the producer instance, passing it the topic name, key and value
    
    # Key = optional. If provided, Kafka hashes it to determine the partition, ensuring all messages with the same key go to the same partition.
    # Value =  the actual message (payload)
    # Partition = manually specifying a partition overrides Kafka's default partitioning logic. 
    #   If not provided, Kafka will either use the key to determine a partition or distribute messages evenly.

    producer.produce(
        topic_name, 
        key='order-' + str(order_id), 
        value=json_message.encode("utf-8"),
        partition=random.randint(0, partition_count-1))
    
    # Explicitly flush the message so that all buffered messages are immediately sent to the cluster 
    producer.flush()
    order_id += 1 
    time.sleep(10)

while True:
    # Should work as expected
    produce_event('online-orders')
    
