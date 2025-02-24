# Kafka Stream Processing Project

This project demonstrates a basic Kafka-based data streaming pipeline using Python. It consists of a **producer** that generates and sends messages to a Kafka topic and a **consumer** that subscribes to the topic and processes incoming messages.

## Project Structure

- **producer.py**: Produces messages to the `online-orders` topic, simulating incoming order data.
- **subscriber.py**: Consumes messages from the `online-orders` topic, processes them, and maintains aggregate statistics.

## Key Learnings

- Setting up a **Kafka producer** and sending messages to a topic.
- Implementing a **Kafka consumer** to read and process messages.
- Using **partitioning and keys** for controlled message distribution.
- Handling **message offsets** to enable replaying data streams.
- Implementing **basic stream analytics** (e.g., total order count, average order value).
- Managing **state persistence** to ensure consumer resilience after crashes.

## Next Steps

- Explore **Kafka Streams** for more advanced real-time processing.
- Implement **error handling** and logging for better observability.
- Integrate **database storage** for long-term analytics.
- Deploy the pipeline using **Docker** and **Kubernetes**.



