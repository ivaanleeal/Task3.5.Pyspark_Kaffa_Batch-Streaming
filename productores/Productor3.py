from kafka import KafkaProducer
import json
import time
import random
import math
from datetime import datetime

# Configuración del sensor
SENSOR_ID = "TEMP_002"
LOCATION = "Laboratorio B"
base_temp = 22.0  # temperatura promedio
TOPIC = "temperaturas"

# Crear el productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # convierte dict a JSON
)

def generate_temperature():
    now = datetime.now()
    hour = now.hour + now.minute / 60

    # Ciclo de 24 horas usando función seno
    cycle = 3 * math.sin((2 * math.pi / 24) * hour)

    noise = random.uniform(-0.3, 0.3)
    temperature = base_temp + cycle + noise

    data = {
        "sensor_id": SENSOR_ID,
        "temperature": round(temperature, 2),
        "timestamp": now.isoformat(),
        "location": LOCATION
    }
    return data

# Loop principal
while True:
    message = generate_temperature()
    producer.send(TOPIC, message)  # enviar al top
