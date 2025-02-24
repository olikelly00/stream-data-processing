from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import time
import json

running = True

conf = {
    'bootstrap.servers': 'localhost:9092',
    #  Group ID (mandatory) - specify which consumer group the consumer is a member of.
    'group.id': 'foo',
    # Auto offset reset - specifies what offset the consumer should start reading from in the event.
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True 
}

consumer = Consumer(conf)

order_total = 0 
order_sum = 0
order_amounts = []
customer_order_summary = {}

def msg_process(msg):
    msg_payload = msg.value().decode("utf-8")

    global order_total
    global order_sum
    global order_amounts
    global customer_order_summary

    payload_dict = json.loads(msg_payload)

    order_id = str(payload_dict["order_id"])
    amount = payload_dict["amount"]
    customer_id = str(payload_dict["customer_id"])
    order_total += 1
    order_sum += int(amount)
    order_amounts.append(amount)
    if customer_id in customer_order_summary.keys():
        customer_order_summary[f"Sales for customer ID: {customer_id}"] += 1
    else:
        customer_order_summary[f"Sales for customer ID {customer_id}"] = 1

    """Prints the received Kafka message."""
    print(f"""
        New Event Received: {msg}\n
        Order ID: {order_id}\n
        Customer ID: {customer_id}\n
        Amount: {amount}\n\n
        Summary Stats:\n
        The total orders received: {order_total}\n
        The total amount of all orders received so far: {order_sum}\n
        Average (mean) order value: {order_sum / order_total}\n
        Max amount: {max(order_amounts)}\n
        Min amount: {min(order_amounts)}\n

        Summary stats by customer:
        {[item for item in customer_order_summary]}
        """)
    
    
def start_consume_loop(consumer, topic):
    try: 
        consumer.subscribe(topic)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                time.sleep(1)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write(('%% %s [%d] reached end at offset %d\n' %(msg.topic(), msg.partition(), msg.offset())))
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                msg_process(msg)
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()

start_consume_loop(consumer, ['online-orders'])

