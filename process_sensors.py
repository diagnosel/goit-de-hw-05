"""3 and 4. Read sensor data and send matching alerts."""
import argparse
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from config import BOOTSTRAP_SERVERS, SUFFIX, SENSORS, TEMPERATURE, HUMIDITY, decode, make_producer


def build_alerts(reading, source):
    """One reading can match both independent checks."""
    alerts = []
    if reading["temperature"] > 40:
        alerts.append((TEMPERATURE, "temperature", "Temperature above 40 C"))
    if reading["humidity"] > 80 or reading["humidity"] < 20:
        alerts.append((HUMIDITY, "humidity", "Humidity outside 20 to 80 percent"))
    return [(topic, {**reading, "alert_type": kind, "message": message,
                     "source": source, "alert_id": f"{source}:{kind}"})
            for topic, kind, message in alerts]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group-id", default=f"processor_{SUFFIX}")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--idle-ms", type=int, default=0, help="0 means wait without a timeout")
    args = parser.parse_args()
    options = {"consumer_timeout_ms": args.idle_ms} if args.idle_ms else {}
    consumer = KafkaConsumer(SENSORS, bootstrap_servers=BOOTSTRAP_SERVERS,
                             group_id=args.group_id, auto_offset_reset="earliest",
                             enable_auto_commit=False, value_deserializer=decode,
                             max_poll_records=50, **options)
    producer = make_producer()
    received = sent = 0
    print(f"PROCESSOR group={args.group_id} topic={SENSORS}", flush=True)
    try:
        for record in consumer:
            reading = record.value
            source = f"{record.topic}:{record.partition}:{record.offset}"
            print(f"RECEIVED source={source} sensor={reading['sensor_id']} "
                  f"event={reading['event_id']} time={reading['timestamp']}\n"
                  f"  temperature={reading['temperature']} C humidity={reading['humidity']} %", flush=True)
            alerts = build_alerts(reading, source)
            print(f"FILTER temperature={reading['temperature'] > 40} "
                  f"humidity={reading['humidity'] > 80 or reading['humidity'] < 20} "
                  f"alerts={len(alerts)}", flush=True)
            for topic, alert in alerts:
                metadata = producer.send(topic, value=alert,
                                         key=reading["sensor_id"].encode()).get(timeout=30)
                sent += 1
                print(f"ALERT SENT topic={metadata.topic} partition={metadata.partition} "
                      f"offset={metadata.offset}\n"
                      f"  sensor={reading['sensor_id']} event={reading['event_id']} "
                      f"message={alert['message']}", flush=True)
            # Commit only this record, after every matching alert is acknowledged.
            consumer.commit({TopicPartition(record.topic, record.partition):
                             OffsetAndMetadata(record.offset + 1, "", -1)})
            received += 1
            if args.max_messages and received >= args.max_messages:
                break
        if args.max_messages and received < args.max_messages:
            raise RuntimeError(f"Expected {args.max_messages} readings, received {received}")
    except KeyboardInterrupt:
        print("Stopped by user", flush=True)
    finally:
        producer.close(timeout=30)
        consumer.close()
    print(f"DONE readings={received} alerts_sent={sent}", flush=True)


if __name__ == "__main__":
    main()
