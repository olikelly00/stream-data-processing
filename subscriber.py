from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import time
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

running = True

kafka_consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    #  Group ID (mandatory) - specify which consumer group the consumer is a member of.
    'group.id': 'foo',
    # Auto offset reset - specifies what offset the consumer should start reading from in the event.
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True 
}


# Initialise global statistics
order_total = 0 
order_sum = 0
order_amounts = []
customer_order_summary = {}

def msg_process(msg):
    msg_payload = msg.value().decode("utf-8")

    global order_total, order_sum, order_amounts, customer_order_summary

    try:
        payload_dict = json.loads(msg_payload)
        order_id = str(payload_dict["order_id"])
        amount = payload_dict["amount"]
        customer_id = str(payload_dict["customer_id"])
    
        order_total += 1
        order_sum += int(amount)
        order_amounts.append(amount)
        print(customer_order_summary.keys())
        print(customer_id in customer_order_summary.keys())
        customer_key = f"Orders for Customer ID {customer_id}"
        if customer_key in customer_order_summary.keys():
            customer_order_summary[f"Orders for Customer ID {customer_id}"] += 1
        else:
            customer_order_summary[f"Orders for Customer ID {customer_id}"] = 1


        logging.info(f"New Order Received! -- Order ID: {order_id} -- Customer ID: {customer_id} -- Amount: {amount}\n\n")
        logging.info(f"Total Orders: {order_total} -- Total Sales: {order_sum} -- Average Order: {order_sum / order_total}")
        logging.info(f"Smallest order: {min(order_amounts) if min(order_amounts) > 0 else 0} ")
        logging.info(f"Largest order: {max(order_amounts) if max(order_amounts) > 0 else 0}")
        logging.info(f"Customer Order Summary: {customer_order_summary}")


    except json.JSONDecodeError as e:
        logging.error(f"Error deconding JSON message {msg.value()}. Error: {e}")


def start_consume_loop(topic):

    consumer = Consumer(kafka_consumer_config)

    try: 
        consumer.subscribe(topic)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                time.sleep(1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.warning('%% %s [%d] reached end at offset %d\n' %(msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                msg_process(msg)
                consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        logging.info("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.info("Starting Kafka Consumer...")
    
    start_consume_loop(["online-orders"])