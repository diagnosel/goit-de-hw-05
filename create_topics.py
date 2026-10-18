"""1. Create and list the three homework topics."""
from kafka.admin import KafkaAdminClient, NewTopic
from config import BOOTSTRAP_SERVERS, SUFFIX, SENSORS, TEMPERATURE, HUMIDITY


def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        existing = set(admin_client.list_topics())
        new_topics = [NewTopic(name, num_partitions=1, replication_factor=1)
                      for name in (SENSORS, TEMPERATURE, HUMIDITY)
                      if name not in existing]
        if new_topics:
            admin_client.create_topics(new_topics=new_topics)
        print("Homework 05 topics", flush=True)
        [print(topic) for topic in sorted(admin_client.list_topics()) if SUFFIX in topic]
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
