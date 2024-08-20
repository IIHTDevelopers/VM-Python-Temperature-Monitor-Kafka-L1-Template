from confluent_kafka import Producer, Consumer, KafkaError
import time

# Configuration for Kafka producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
}

# Configuration for Kafka consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group.id': 'temperature_monitor_group',
    'auto.offset.reset': 'earliest'
}


# Function to deliver message
def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced: {msg.value()}")


# Function to produce temperature data
def produce_temperature_data(producer, topic, temperature):
    try:
        producer.produce(topic, str(temperature), callback=acked)
        producer.poll(1)  # Wait for any events or message delivery callbacks
    except Exception as e:
        print(f"Error producing message: {e}")


# Function to consume temperature data
def consume_temperature_data(consumer, topic, timeout=10):
    consumer.subscribe([topic])
    start_time = time.time()
    try:
        while True:
            if time.time() - start_time > timeout:
                print("Timeout reached. Stopping consumer.")
                break
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break
            print(f"Consumed message: {msg.value().decode('utf-8')}")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()


# Function to start the Kafka producer
def start_producer():
    producer = Producer(producer_conf)
    topic = 'temperature_topic'

    # Example temperature data
    temperatures = [22.5, 23.0, 23.5, 24.0, 24.5]

    for temperature in temperatures:
        produce_temperature_data(producer, topic, temperature)

    producer.flush()


# Function to start the Kafka consumer
def start_consumer():
    consumer = Consumer(consumer_conf)
    topic = 'temperature_topic'
    consume_temperature_data(consumer, topic)


if __name__ == "__main__":
    # Start the producer to send temperature data
    start_producer()

    # Start the consumer to receive temperature data
    start_consumer()
