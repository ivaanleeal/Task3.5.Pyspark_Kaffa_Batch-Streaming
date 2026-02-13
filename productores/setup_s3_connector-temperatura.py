import requests
import json
import time

CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "temperature-minio-sink"

# ==============================
# S3 Sink Connector Configuration
# ==============================
connector_config = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": "temperaturas",

        # MinIO / S3 Config
        "s3.bucket.name": "iot-data",
        "s3.region": "us-east-1",
        "s3.part.size": "5242880",
        "store.url": "http://minio:9000",
        "aws.access.key.id": "minioadmin",
        "aws.secret.access.key": "minioadmin",
        "s3.path.style.access": "true",

        # Format
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "flush.size": "10",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",

        # Time-based partitioning (hourly)
        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "path.format": "'año'=YYYY/'mes'=MM/'dia'=dd/'hora'=HH",
        "partition.duration.ms": "3600000",
        "locale": "es-ES",
        "timezone": "UTC",
        "timestamp.extractor": "Record",
        "timestamp.field": "timestamp",

        "schema.compatibility": "NONE"
    }
}

# ==============================
# Functions
# ==============================

def wait_for_connect():
    """Wait for Kafka Connect to be ready"""
    print("⏳ Waiting for Kafka Connect to be ready...")
    max_retries = 30

    for i in range(max_retries):
        try:
            response = requests.get(f"{CONNECT_URL}/connectors")
            if response.status_code == 200:
                print("✅ Kafka Connect is ready!")
                return True
        except requests.exceptions.RequestException:
            pass

        print(f"Attempt {i+1}/{max_retries}... retrying in 5 seconds")
        time.sleep(5)

    print("❌ ERROR: Kafka Connect did not become ready")
    return False


def delete_connector_if_exists():
    """Delete connector if it already exists"""
    try:
        response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        if response.status_code == 200:
            print(f"⚠ Connector '{CONNECTOR_NAME}' already exists. Deleting...")
            delete_response = requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
            if delete_response.status_code == 204:
                print("🗑 Connector deleted successfully")
                time.sleep(5)
    except requests.exceptions.RequestException as e:
        print(f"Error checking/deleting connector: {e}")


def create_connector():
    """Create the S3 sink connector"""
    print(f"🚀 Creating connector: {CONNECTOR_NAME}")

    response = requests.post(
        f"{CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        json=connector_config  # <-- Correct way
    )

    if response.status_code == 201:
        print("✅ Connector created successfully!")
        print(json.dumps(response.json(), indent=2))
        return True
    else:
        print(f"❌ Failed to create connector. Status: {response.status_code}")
        print(response.text)
        return False


def check_connector_status():
    """Check connector status"""
    print("\n🔎 Checking connector status...")
    response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")

    if response.status_code == 200:
        print(json.dumps(response.json(), indent=2))
    else:
        print(f"❌ Failed to get connector status: {response.status_code}")
        print(response.text)


# ==============================
# Main
# ==============================

def main():
    print("=" * 60)
    print("Kafka Connect → MinIO (Temperature Sink Setup)")
    print("=" * 60)

    if not wait_for_connect():
        return

    delete_connector_if_exists()

    if create_connector():
        time.sleep(3)
        check_connector_status()

        print("\n" + "=" * 60)
        print("🎉 Setup complete!")
        print("Topic: sensores-temperatura")
        print("Bucket: iot-data")
        print("Partitioning: Year/Month/Day/Hour")
        print("MinIO Console: http://localhost:9001")
        print("User: minioadmin | Password: minioadmin")
        print("=" * 60)


if __name__ == "__main__":
    main()
