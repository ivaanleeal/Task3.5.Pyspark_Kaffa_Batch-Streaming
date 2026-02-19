import json
import random
import math
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Falsificación de horas
current_fake_time = datetime(2025, 2, 13, 0, 0, 0, tzinfo=timezone.utc)
start_reference = current_fake_time 

print("Iniciando generación infinita de temperatura (Tiempo Acelerado)...")

try:
    while True:
        # 1. Calculamos las horas "falsas" transcurridas para la tendencia del Sensor 1
        hours_passed = (current_fake_time - start_reference).total_seconds() / 3600
        
        # --- SENSOR 1: Calentamiento gradual (+0.5°C/h) ---
        temp1 = 20.0 + (0.5 * hours_passed) + random.uniform(-0.2, 0.2)
        data1 = {
            "sensor_id": "TEMP_01",
            "temperature": round(temp1, 2),
            "timestamp": current_fake_time.isoformat(),
            "location": "Warehouse_A"
        }
        
        # --- SENSOR 2: Ciclo estable de 24h ---
        hour_of_day = current_fake_time.hour
        # Oscila entre 17°C y 27°C dependiendo de la hora "falsa"
        temp2 = 22.0 + 5 * math.sin((hour_of_day - 8) * (2 * math.pi / 24)) + random.uniform(-0.3, 0.3)
        data2 = {
            "sensor_id": "TEMP_02",
            "temperature": round(temp2, 2),
            "timestamp": current_fake_time.isoformat(),
            "location": "Warehouse_B"
        }

        # Enviar a Kafka
        producer.send("temperature-sensors", data1)
        producer.send("temperature-sensors", data2)
        
        # LOG para ver qué está pasando en la consola
        print(f"Enviando datos: {current_fake_time.strftime('%Y-%m-%d %H:%M')} | S1: {data1['temperature']}°C | S2: {data2['temperature']}°C")

        # Avanzamos 10 minutos en el "mundo falso"
        current_fake_time += timedelta(minutes=10)
        
        # Pero solo esperamos 0.1 segundos en el "mundo real"
        # Esto significa que 1 día de datos se genera en unos 14 segundos reales
        time.sleep(0.1) 

except KeyboardInterrupt:
    print("\nDeteniendo productor...")
finally:
    producer.flush()
    producer.close()