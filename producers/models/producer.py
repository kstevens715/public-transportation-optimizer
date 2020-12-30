"""Producer base-class providing common utilites and functionality"""
import logging
import time
import socket


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)
SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            {
                "bootstrap.servers": BROKER_URL,
                "schema.registry.url": SCHEMA_REGISTRY_URL,
                "client.id": socket.gethostname()
            },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # Had to make a client that would get garbage collected, or ran into connection problems
        client = AdminClient({
            "bootstrap.servers": BROKER_URL,
            "client.id": socket.gethostname(),
        })

        if not self.topic_exists(client, self.topic_name):
            new_topic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=self.num_replicas
            )
            futures = client.create_topics([new_topic])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"topic created {self.topic_name}")
                except Exception as e:
                    logger.error(f"FAILED to create topic {self.topic_name}: {e}")

    def topic_exists(self, client, topic):
        topic_response = client.list_topics()
        return topic in set(t.topic for t in iter(topic_response.topics.values()))

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
