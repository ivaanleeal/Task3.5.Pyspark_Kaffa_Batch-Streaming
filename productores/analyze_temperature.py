from minio import Minio, S3Error
import pandas as pd
import json
import io
import matplotlib.pyplot as plt

# ==============================
# CONFIGURACIÓN
# ==============================

MINIO_ENDPOINT = "localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "iot-data"

# Especificar la hora que queremos consultar
YEAR = "2026"
MONTH = "02"
DAY = "13"
HOUR = "17"

PREFIX = f"sensores-temperatura/año={YEAR}/mes={MONTH}/dia={DAY}/hora={HOUR}/"

# ==============================
# Conexión a MinIO
# ==============================

client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

# ==============================
# Crear bucket si no existe
# ==============================

try:
    if not client.bucket_exists(BUCKET_NAME):
        print(f"⚠ Bucket '{BUCKET_NAME}' no existe. Creando...")
        client.make_bucket(BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' creado correctamente")
    else:
        print(f"✅ Bucket '{BUCKET_NAME}' ya existe")
except S3Error as err:
    print(f"❌ Error al verificar/crear bucket: {err}")
    exit(1)

# ==============================
# Listar y leer objetos JSON
# ==============================

try:
    objects = client.list_objects(BUCKET_NAME, prefix=PREFIX, recursive=True)
    data_list = []

    count_files = 0
    for obj in objects:
        count_files += 1
        response = client.get_object(BUCKET_NAME, obj.object_name)
        content = response.read().decode("utf-8")
        # Cada línea del archivo es un JSON
        for line in content.strip().split("\n"):
            data_list.append(json.loads(line))

    if count_files == 0:
        print(f"⚠ No se encontraron archivos en la ruta: {PREFIX}")
        exit(0)

    print(f"✅ {len(data_list)} registros cargados desde {count_files} archivo(s)")

except S3Error as err:
    print(f"❌ Error leyendo objetos: {err}")
    exit(1)

# ==============================
# Convertir a DataFrame
# ==============================

df = pd.DataFrame(data_list)
if df.empty:
    print("⚠ No hay datos para procesar")
    exit(0)

df["timestamp"] = pd.to_datetime(df["timestamp"])

# ==============================
# Estadísticas por sensor
# ==============================

stats = df.groupby("sensor_id")["temperature"].agg(["mean", "min", "max"])
print("\n📊 Estadísticas por sensor:")
print(stats)

# ==============================
# Graficar temperatura a lo largo del tiempo
# ==============================

plt.figure(figsize=(10,5))
for sensor in df["sensor_id"].unique():
    sensor_data = df[df["sensor_id"] == sensor]
    plt.plot(sensor_data["timestamp"], sensor_data["temperature"], marker='o', label=sensor)

plt.xlabel("Tiempo")
plt.ylabel("Temperatura (°C)")
plt.title("Temperatura a lo largo del tiempo por sensor")
plt.legend()
plt.xticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

# ==============================
# Calcular pendiente (tendencia
