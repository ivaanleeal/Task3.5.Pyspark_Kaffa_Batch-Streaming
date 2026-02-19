import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "humidity-sensors"
MESSAGES_PER_SECOND = 1

SENSOR_ID = "hum-1"
LOCATION = "warehouse-1"
BASE_HUMIDITY = 45.0
NIGHT_BONUS = 10.0


def humidity_for_now(now_utc):
    hour = now_utc.hour
    night = hour >= 18 or hour < 6
    noise = random.uniform(-3.0, 3.0)
    humidity = BASE_HUMIDITY + (NIGHT_BONUS if night else 0.0) + noise
    return round(humidity, 2)


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=3,
    )

    interval = 1.0 / MESSAGES_PER_SECOND

    try:
        while True:
            now = datetime.now(timezone.utc)
            payload = [
                SENSOR_ID,
                f"{humidity_for_now(now):.2f}",
                now.isoformat().replace("+00:00", "Z"),
                LOCATION,
                str(int(now.timestamp() * 1000)),
            ]
            line = ",".join(payload)
            producer.send(KAFKA_TOPIC, value=line)
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()