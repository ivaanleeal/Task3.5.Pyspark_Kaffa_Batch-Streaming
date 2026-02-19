import requests
import json
import time

CONNECT_URL = "http://localhost:8083/connectors"

# 1. Temperatura: JSON + Particionado por hora (Hora de España)
temp_config = {
    "name": "temperature-sink",
    "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": "temperature-sensors",
        "s3.bucket.name": "temperature-sensors", 
        "s3.region": "us-east-1",
        "store.url": "http://minio:9000",
        "aws.access.key.id": "minioadmin",
        "aws.secret.access.key": "minioadmin",
        "s3.path.style.access": "true",
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "flush.size": "1",
        "rotate.interval.ms": "1000",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
        "partition.duration.ms": "3600000",
        "timezone": "Europe/Madrid",  # <--- CAMBIADO A HORA DE ESPAÑA
        "timestamp.extractor": "Wallclock",
        "locale": "es-ES"             # <--- Ajustado a local de España
    }
}

# 2. Humedad: JSON + Sin particiones (Mismo Bucket de tu imagen)
hum_config = {
        "name": "humidity-sink",
        "config": {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": "humidity-sensors",
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "s3.bucket.name": "humidity-sensors",
            "s3.region": "us-east-1",
            "store.url": "http://minio:9000",
            "format.class": "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
            "format.bytearray.extension": ".csv",
            "flush.size": "10",
            "rotate.interval.ms": "60000",
            "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
            "schema.compatibility": "NONE",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter",
            "value.converter.schemas.enable": "false",
        },
    }

def setup():
    # Limpieza total de conectores previos
    to_delete = ["temperature-sink", "humidity-sink", "humidity-sink-json"]
    for name in to_delete:
        requests.delete(f"{CONNECT_URL}/{name}")
    
    time.sleep(1)
    
    for config in [temp_config, hum_config]:
        name = config["name"]
        print(f"🚀 Creando {name}...")
        res = requests.post(CONNECT_URL, json=config, headers={"Content-Type": "application/json"})
        if res.status_code in [200, 201]:
            print(f"✅ {name} listo.")
        else:
            print(f"❌ Error en {name}: {res.text}")

if __name__ == "__main__":
    setup()