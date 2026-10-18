# GoIT homework 05 — Kafka sensor alerts

Two Python processes send readings to Kafka. A processor checks each reading and writes warnings to two alert topics. A separate consumer reads both alert topics.

## Requirements

Tested locally with Python 3.12.4, Java 17.0.17, Apache Kafka 4.0.2 and kafka-python 2.2.15. Docker and Spark are not needed. The supplied broker config binds to loopback only, with ports 9092 and 9093.

From this folder, create a Python environment and install the pinned client:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Download the [Kafka 4.0.2 binary archive](https://archive.apache.org/dist/kafka/4.0.2/kafka_2.13-4.0.2.tgz) and its [SHA-512 file](https://archive.apache.org/dist/kafka/4.0.2/kafka_2.13-4.0.2.tgz.sha512). Verify the checksum and extract the archive. Set `KAFKA_HOME` to the extracted folder and `JAVA_HOME` to Java 17. For example, on the Mac used for this work:

```bash
export KAFKA_HOME=/private/tmp/kafka_2.13-4.0.2
export JAVA_HOME=/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```

## Start the local broker

Run these commands from this homework folder. Format storage only on the first run. On later runs, keep `.runtime/kafka-logs` and start the broker without formatting it again.

```bash
mkdir -p .runtime
export HW05_CLUSTER_ID="$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)"
"$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$HW05_CLUSTER_ID" -c local_broker.properties
KAFKA_HEAP_OPTS='-Xms256m -Xmx512m' "$KAFKA_HOME/bin/kafka-server-start.sh" local_broker.properties
```

Keep that terminal open. Wait for the broker to start. Do not use an existing work or production Kafka cluster for this demo.

## Run the five stages

Open other terminals in this folder and activate the same Python environment in each terminal.

1. Create and list the three topics:

```bash
python create_topics.py
```

2. Start the processor and alert reader in separate terminals before starting the sensors:

```bash
python process_sensors.py
```

```bash
python consume_alerts.py
```

3. Run each sensor command in its own terminal at the same time:

```bash
python sensor.py --sensor-id sensor-101 --messages 12 --interval 2 --seed 11
```

```bash
python sensor.py --sensor-id sensor-202 --messages 12 --interval 2 --seed 22
```

Each command sends 12 random readings and then exits. The optional seed makes the random values repeatable; timestamps, event IDs and message order can change. Without `--messages`, a sensor keeps running until Ctrl+C. Without `--sensor-id`, it chooses one ID at startup and keeps it for that process.

4. Check `RECEIVED` and `FILTER` lines in the processor. Stage 3 receives and checks values. Stage 4 prints `ALERT SENT` only after Kafka acknowledges each matching alert.
5. Check `ALERT RECEIVED` in the reader. It shows the alert topic, partition, offset, sensor ID, time, values, source and message.

Stop the reader, processor and broker with Ctrl+C in their own terminals after inspection. Do not stop unrelated processes.

## Rules and message fields

- Temperature is a random value from 25 to 45 C. Create an alert only when it is greater than 40.
- Humidity is a random value from 15 to 85 percent. Create an alert only when it is less than 20 or greater than 80.
- Values of exactly 40 C, 20 percent and 80 percent do not trigger their respective alerts.
- Two independent checks allow one reading to create two alerts.
- A reading has `sensor_id`, `timestamp` in UTC, `temperature`, `humidity` and `event_id`.
- An alert preserves those fields and adds `alert_type`, `message`, `source` and `alert_id`. The source is the input topic, partition and offset.

The topics are `building_sensors_aurika_hryhorova_hw05`, `temperature_alerts_aurika_hryhorova_hw05` and `humidity_alerts_aurika_hryhorova_hw05`. Each has one partition and one replica for the single local broker. `SENT` and `ALERT SENT` follow successful `send(...).get(...)` calls with `acks="all"`. One replica does not protect against losing this machine.

## Verified results

Two complete runs used fresh, separate topic sets on the local broker. Each run sent 24 readings from two concurrent processes and produced 14 alerts: 7 temperature and 7 humidity alerts. The reader received all 14. Eleven readings produced no alert; one reading produced both alerts. Independent reads of the three topics checked every alert against the input values, not only the totals.

For example, the first run stored sensor-101 reading `5acae9f8:8` at input offset 17: temperature 44.64 C and humidity 82.53 percent. It produced one alert in each alert topic, both at offset 4. The alert time is the original reading time, `2026-10-18T19:25:53.323+00:00`.

Run the boundary checks without Kafka:

```bash
python -m unittest -v test_thresholds.py
```

This test uses eight cases, including exact limits, values just outside them and a reading that needs both alerts. The test values are separate from the generated demo readings.

## Repeat runs and delivery limits

Consumer groups remember their committed positions. The default group continues from its last committed offset. A new `--group-id` replays existing records from the beginning. Replaying the processor can write the same alerts again.

For a clean demo, set a new `KAFKA_TOPIC_SUFFIX` in every terminal, run `create_topics.py`, and use that same suffix for the processor, reader and both sensors. `KAFKA_BOOTSTRAP_SERVERS` can select a different local broker. No credentials are stored in code.

The processor commits each input offset only after all matching alert sends succeed. This is at-least-once processing: a crash after sending an alert and before committing the input can repeat that alert. `alert_id` identifies its source; the code does not claim transactional or exactly-once delivery. Crash recovery and broker failover were not tested in this homework.

## Submission files

- `config.py`, `create_topics.py`, `sensor.py`, `process_sensors.py`, `consume_alerts.py`: the pipeline.
- `local_broker.properties` and `requirements.txt`: local setup.
- `test_thresholds.py`: boundary checks.
- `screenshots/p1_...p5_...jpg`: five actual screenshots of console output displayed in a local log viewer. The two-sensor screenshot was taken while both processes ran. The viewer preserves the log text and labels excerpts.
- `ДЗ5_Григорова_Ауріка_Вадимівна.docx`: the screenshots and short descriptions.

The LMS archive is `ДЗ5_Григорова_Ауріка_Вадимівна.zip`. It contains the code, setup files, screenshots and Word document. Local broker data, environment files and audit logs are excluded from the archive and Git. Publication and LMS submission need separate approval.

## Sources

- [Apache Kafka 4.0 quick start and Java requirement](https://kafka.apache.org/40/getting-started/quickstart/)
- [Kafka consumer concepts](https://kafka.apache.org/40/getting-started/introduction/)
- [kafka-python 2.2.15](https://pypi.org/project/kafka-python/2.2.15/)
- [kafka-python API](https://kafka-python.readthedocs.io/en/master/apidoc/modules.html)
