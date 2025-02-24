

from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import time

running = True

conf = {
    'bootstrap.servers': 'localhost:9092',
    #  Group ID (mandatory) - specify which consumer group the consumer is a member of
    'group.id': 'foo',
    # Auto offset reset - specifies what offset the consumer should start reading from in the event
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)


def msg_process(msg):
    """Prints the received Kafka message."""
    print(f"Received: {msg}\nPayload:{msg.value().decode('utf-8')}\nPartition:{msg.partition()}\nOffset:{msg.offset()}")


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

start_consume_loop(consumer, ['orders'])



