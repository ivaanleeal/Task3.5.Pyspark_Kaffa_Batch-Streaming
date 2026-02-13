from kafka import KafkaProducer
import time
import random
from datetime import datetime

TOPIC = "humedades"

# Serializador simple de texto
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

def generate_csv_data():
    now = datetime.now().isoformat()
    hum = round(random.uniform(40, 70), 2)
    # Formato: ID, Humedad, Timestamp, Location
    return f"HUM_001,{hum},{now},Almacen"

print("📡 Empezando envío de datos CSV (Ctrl+C para parar)...")

# Opcional: Enviar cabecera primero
producer.send(TOPIC, "sensor_id,humedad,timestamp,location")

while True:
    line = generate_csv_data()
    producer.send(TOPIC, line)
    print(f"📤 Enviado: {line}")
    time.sleep(5)
