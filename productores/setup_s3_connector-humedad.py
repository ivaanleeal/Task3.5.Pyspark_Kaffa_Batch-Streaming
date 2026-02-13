import requests
import json
import time

CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "humidity-minio-sink"

connector_config = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": "humedades",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "s3.bucket.name": "iot-data",
        "s3.region": "us-east-1",
        "store.url": "http://minio:9000",
        "aws.access.key.id": "minioadmin",
        "aws.secret.access.key": "minioadmin",

        # USAMOS BYTEARRAY PARA FORZAR LA EXTENSIÓN .CSV
        "format.class": "io.confluent.connect.s3.format.bytearray.ByteArrayFormat",
        "format.bytearray.extension": ".csv",
        
        # FORZAMOS SALIDA INMEDIATA (1 MENSAJE = 1 ARCHIVO)
        "flush.size": "1",
        "rotate.interval.ms": "5000",
        
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        "schema.compatibility": "NONE",
        
        # CONVERTIDORES DE TEXTO PLANO
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter.schemas.enable": "false"
    }
}

def setup():
    # 1. Borrar si existe
    requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
    time.sleep(2)
    
    # 2. Crear conector
    print(f"🚀 Configurando conector {CONNECTOR_NAME} para CSV...")
    res = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        json=connector_config
    )
    
    if res.status_code in [200, 201]:
        print("✅ Conector creado. Esperando datos...")
    else:
        print(f"❌ Error: {res.text}")

if __name__ == "__main__":
    setup()
