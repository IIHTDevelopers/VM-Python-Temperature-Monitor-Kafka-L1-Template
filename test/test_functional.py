import unittest
from confluent_kafka import Producer, Consumer, KafkaError
from temperature_monitor import produce_temperature_data, consume_temperature_data, start_producer, start_consumer
import time
from test.TestUtils import TestUtils

class TestKafkaIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up Kafka configuration and create a Kafka topic"""
        cls.producer_conf = {
            'bootstrap.servers': 'localhost:9092',  # Kafka broker(s)
        }
        cls.consumer_conf = {
            'bootstrap.servers': 'localhost:9092',  # Kafka broker(s)
            'group.id': 'temperature_monitor_group',
            'auto.offset.reset': 'earliest'
        }
        cls.topic = 'temperature_topic'
        cls.producer = Producer(cls.producer_conf)
        cls.consumer = Consumer(cls.consumer_conf)

    @classmethod
    def tearDownClass(cls):
        """Tear down Kafka producer and consumer"""
        cls.producer.flush()  # Ensure all messages are sent before closing
        cls.consumer.unassign()  # Unassign the consumer from its group and topic
        cls.consumer.close()  # Close the consumer connection

    def test_produce_and_consume_temperature_data(self):
        """Test producing and consuming a single temperature data point"""
        test_obj = TestUtils()
        try:
            # Produce temperature data
            temperature = 22.5
            produce_temperature_data(self.producer, self.topic, temperature)
            self.producer.flush()

            # Consume temperature data
            self.consumer.subscribe([self.topic])
            msg = self.consumer.poll(timeout=5.0)  # Wait for message for up to 5 seconds

            if msg is None:
                test_obj.yakshaAssert("TestProduceAndConsumeTemperatureData", False, "boundary")
                print("TestProduceAndConsumeTemperatureData = Failed: No message received")
            elif msg.error():
                test_obj.yakshaAssert("TestProduceAndConsumeTemperatureData", False, "boundary")
                print(f"TestProduceAndConsumeTemperatureData = Failed: Message error: {msg.error()}")
            elif msg.value().decode('utf-8') != '22.5':
                test_obj.yakshaAssert("TestProduceAndConsumeTemperatureData", False, "boundary")
                print("TestProduceAndConsumeTemperatureData = Failed: Message content does not match")
            else:
                test_obj.yakshaAssert("TestProduceAndConsumeTemperatureData", True, "boundary")
                print("TestProduceAndConsumeTemperatureData = Passed")

        except Exception as e:
            test_obj.yakshaAssert("TestProduceAndConsumeTemperatureData", False, "boundary")
            print(f"TestProduceAndConsumeTemperatureData = Failed: Exception occurred - {e}")

    def test_start_producer(self):
        """Test starting the producer and verify data is sent"""
        test_obj = TestUtils()
        try:
            start_producer()

            # Consume the data to verify
            self.consumer.subscribe([self.topic])
            messages = []
            start_time = time.time()
            while time.time() - start_time < 5:  # Wait for up to 5 seconds
                msg = self.consumer.poll(timeout=1.0)  # Poll every 1 second
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        test_obj.yakshaAssert("TestStartProducer", False, "boundary")
                        print(f"TestStartProducer = Failed: Message error: {msg.error()}")
                        return
                messages.append(msg.value().decode('utf-8'))

            expected_temperatures = {'22.5', '23.0', '23.5', '24.0', '24.5'}
            if len(messages) == 0:
                test_obj.yakshaAssert("TestStartProducer", False, "boundary")
                print("TestStartProducer = Failed: No messages received")
            elif not any(temp in expected_temperatures for temp in messages):
                test_obj.yakshaAssert("TestStartProducer", False, "boundary")
                print("TestStartProducer = Failed: Expected temperatures not found")
            else:
                test_obj.yakshaAssert("TestStartProducer", True, "boundary")
                print("TestStartProducer = Passed")

        except Exception as e:
            test_obj.yakshaAssert("TestStartProducer", False, "boundary")
            print(f"TestStartProducer = Failed: Exception occurred - {e}")

    def test_start_consumer(self):
        """Test starting the consumer and verify data is consumed"""
        test_obj = TestUtils()
        try:
            # Produce some data to ensure there is something to consume
            temperatures = [22.5, 23.0, 23.5, 24.0, 24.5]
            for temp in temperatures:
                produce_temperature_data(self.producer, self.topic, temp)
            self.producer.flush()

            # Start the consumer and verify data is consumed
            consumed_messages = []
            start_time = time.time()
            while time.time() - start_time < 5:  # Limit to 5 seconds
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        test_obj.yakshaAssert("TestStartConsumer", False, "boundary")
                        print(f"TestStartConsumer = Failed: Consumer error: {msg.error()}")
                        return
                consumed_messages.append(msg.value().decode('utf-8'))

            if len(consumed_messages) == 0:
                test_obj.yakshaAssert("TestStartConsumer", False, "boundary")
                print("TestStartConsumer = Failed: No messages consumed")
            else:
                expected_temperatures = {'22.5', '23.0', '23.5', '24.0', '24.5'}
                if not all(temp in expected_temperatures for temp in consumed_messages):
                    test_obj.yakshaAssert("TestStartConsumer", False, "boundary")
                    print("TestStartConsumer = Failed: Not all expected temperatures consumed")
                else:
                    test_obj.yakshaAssert("TestStartConsumer", True, "boundary")
                    print("TestStartConsumer = Passed")

        except Exception as e:
            test_obj.yakshaAssert("TestStartConsumer", False, "boundary")
            print(f"TestStartConsumer = Failed: Exception occurred - {e}")

if __name__ == "__main__":
    unittest.main()
