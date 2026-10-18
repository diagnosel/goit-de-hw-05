"""5. Read both alert topics and print the stored warnings."""
import argparse
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from config import BOOTSTRAP_SERVERS, SUFFIX, TEMPERATURE, HUMIDITY, decode


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default=f"alerts_{SUFFIX}")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--idle-ms", type=int, default=0)
    args = parser.parse_args()
    options = {"consumer_timeout_ms": args.idle_ms} if args.idle_ms else {}
    consumer = KafkaConsumer(TEMPERATURE, HUMIDITY, bootstrap_servers=BOOTSTRAP_SERVERS,
                             group_id=args.group_id, auto_offset_reset="earliest",
                             enable_auto_commit=False, value_deserializer=decode, **options)
    received = 0
    print(f"ALERT READER group={args.group_id}\nTopics: {TEMPERATURE}, {HUMIDITY}", flush=True)
    try:
        for record in consumer:
            alert = record.value
            print(f"ALERT RECEIVED topic={record.topic} partition={record.partition} offset={record.offset}\n"
                  f"  sensor={alert['sensor_id']} event={alert['event_id']} time={alert['timestamp']}\n"
                  f"  temperature={alert['temperature']} C humidity={alert['humidity']} %\n"
                  f"  message={alert['message']}\n  source={alert['source']}", flush=True)
            consumer.commit({TopicPartition(record.topic, record.partition):
                             OffsetAndMetadata(record.offset + 1, "", -1)})
            received += 1
            if args.max_messages and received >= args.max_messages:
                break
        if args.max_messages and received < args.max_messages:
            raise RuntimeError(f"Expected {args.max_messages} alerts, received {received}")
    except KeyboardInterrupt:
        print("Stopped by user", flush=True)
    finally:
        consumer.close()
    print(f"DONE alerts_received={received}", flush=True)


if __name__ == "__main__":
    main()
