import json, time, random, math
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

while True:
    now = datetime.now(timezone.utc)
    # Ciclo de 24h: el pico ocurre a las 14:00
    hour = now.hour + now.minute/60
    cycle = math.sin((hour - 8) * math.pi / 12) # Mínimo a las 02:00, Máximo a las 14:00
    temp = 22.0 + (5.0 * cycle) + random.uniform(-0.3, 0.3)

    data = {
        "sensor_id": "TEMP_02",
        "temperature": round(temp, 2),
        "timestamp": now.isoformat(),
        "location": "Warehouse_B"
    }
    producer.send("temperature-sensors", data)
    time.sleep(5)
