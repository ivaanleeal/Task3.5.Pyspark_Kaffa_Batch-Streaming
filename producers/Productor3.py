import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "humidity-sensors"

SENSOR_ID = "HUM_01"
LOCATION = "Warehouse_A"
BASE_HUMIDITY = 45.0
NIGHT_BONUS = 20.0  # Incremento notable para la tendencia nocturna

def humidity_for_time(fake_now):
    hour = fake_now.hour
    # Tendencia oculta: Aumenta durante la noche (18:00-06:00)
    is_night = hour >= 18 or hour < 6
    noise = random.uniform(-3.0, 3.0)
    humidity = BASE_HUMIDITY + (NIGHT_BONUS if is_night else 0.0) + noise
    return round(humidity, 2)

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all"
    )

    # --- INICIO DEL TIEMPO FALSO ---
    current_fake_time = datetime(2025, 2, 13, 0, 0, 0, tzinfo=timezone.utc)
    # Cada paso avanza 15 minutos en el mundo virtual
    time_delta = timedelta(minutes=15)

    print(f"Iniciando generación infinita de Humedad en {KAFKA_TOPIC}...")

    try:
        while True:
            hum = humidity_for_time(current_fake_time)
            
            # Formato CSV: ID, humedad, ISO8601, ubicación, timestamp_ms
            payload = [
                SENSOR_ID,
                f"{hum:.2f}",
                current_fake_time.isoformat().replace("+00:00", "Z"),
                LOCATION,
                str(int(current_fake_time.timestamp() * 1000)),
            ]
            
            line = ",".join(payload)
            producer.send(KAFKA_TOPIC, value=line)
            
            # Log para monitorizar el progreso en tu consola
            print(f"Enviando: {current_fake_time.strftime('%Y-%m-%d %H:%M')} | Humedad: {hum}%")

            # Avanzamos el reloj falso
            current_fake_time += time_delta
            
            # Pausa real pequeña para que el flujo sea constante y visible
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nProductor de humedad detenido por el usuario.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()