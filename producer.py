from confluent_kafka import Producer
import socket
import random
import json
import time
import uuid
import logging


# Define the Kafka configuration settings to be passed to the producer
kafka_producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}


topic_name = "online-orders"
producer = Producer(kafka_producer_config)
topic_data = producer.list_topics()
topic_name = "online-orders"
customer_ids = list(range(1,20))

if topic_name not in topic_data.topics:
        raise Exception(f"Error: Topic {topic_name} does not exist. Must be created first using this CLI command: bin/kafka-topics.sh --create --topic {topic_name} --bootstrap-server localhost:9092")

def produce_event(topic_name):    

    order_id = str(uuid.uuid1())
    amount = random.randint(2, 30)
    customer_id = customer_ids[random.randint(0, len(customer_ids) - 1)]
    
    json_message = json.dumps({
        "order_id": order_id,
        "amount": amount,
        "customer_id": customer_id,
        "total_orders": len(topic_data.topics)
    })

    partition_count = len(topic_data.topics[topic_name].partitions)


    producer.produce(
        topic_name, 
        key='order-' + str(order_id), 
        value=json_message.encode("utf-8"),
        partition=random.randint(0, partition_count-1))
    
    producer.flush() 
    logging.info(f"✅ Order Sent: {json_message}")


def main():
    try:
        while True:
            produce_event()
            time.sleep(10)  # Adjust the interval as needed
    except KeyboardInterrupt:
        logging.info("Producer stopped.")

if __name__ == "__main__":
    logging.info("Starting Kafka Producer...")
    main()
    
