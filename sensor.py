"""2. One process sends random readings from one sensor."""
import argparse
from datetime import datetime, timezone
import os
import random
import time
import uuid
from config import SENSORS, make_producer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sensor-id", default=None)
    parser.add_argument("--messages", type=int, default=0, help="0 means keep running")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=None, help="Repeat the same random values")
    args = parser.parse_args()
    if args.messages < 0 or args.interval < 0:
        parser.error("messages and interval must be zero or positive")
    sensor_id = args.sensor_id or uuid.uuid4().hex[:8]
    run_id = uuid.uuid4().hex[:8]
    rng = random.Random(args.seed)
    producer = make_producer()
    print(f"START sensor={sensor_id} pid={os.getpid()} run={run_id}", flush=True)
    try:
        sequence = 0
        while args.messages == 0 or sequence < args.messages:
            reading = {"sensor_id": sensor_id,
                       "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                       "temperature": round(rng.uniform(25, 45), 2),
                       "humidity": round(rng.uniform(15, 85), 2),
                       "event_id": f"{run_id}:{sequence}"}
            metadata = producer.send(SENSORS, value=reading,
                                     key=sensor_id.encode()).get(timeout=30)
            print(f"SENT sensor={sensor_id} event={reading['event_id']} "
                  f"time={reading['timestamp']}\n"
                  f"  temperature={reading['temperature']} C humidity={reading['humidity']} % "
                  f"topic={metadata.topic} partition={metadata.partition} offset={metadata.offset}", flush=True)
            sequence += 1
            if args.messages == 0 or sequence < args.messages:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopped by user", flush=True)
    finally:
        producer.close(timeout=30)
        print(f"END sensor={sensor_id}", flush=True)


if __name__ == "__main__":
    main()
